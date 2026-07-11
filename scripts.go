package redlease

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// acquireScript takes the lock with NX and, on success, increments the fence
// counter atomically and returns the new token. Doing both in one script
// guarantees every distinct leadership term gets a strictly greater token than
// any term before it, even under concurrent acquire attempts. Returns 0 when the
// lock is already held by another instance.
//
// Winning also stamps the new token as the fence high-water mark, so a deposed
// term's tokens go stale at election, not at the successor's first fenced
// write. When the mark is ahead of the counter (counter lost to eviction or
// partial restore, or an out-of-band NewFencer token), win() first heals the
// counter from the mark, so every election mints strictly above everything
// ever applied and the mark never moves down. Both copies go through GET, not
// a Lua number, keeping the stored form verbatim (see fenceApply).
//
// A lock whose value is this instance's own id — left over from a previous term
// whose release did not reach Redis, or from a restart with a fixed InstanceID —
// is taken over rather than waited out: the TTL is refreshed and a fresh token
// is minted, because it is a new leadership term regardless of who held the lock
// before, and fenced writes from the dead term must stay rejectable.
//
// KEYS[1] lock key, KEYS[2] fence counter, KEYS[3] applied high-water key.
// ARGV[1] instance id, ARGV[2] TTL ms.
var acquireScript = goredis.NewScript(`
local function win()
    local applied = tonumber(redis.call('get', KEYS[3]) or '0')
    local counter = tonumber(redis.call('get', KEYS[2]) or '0')
    if applied > counter then
        redis.call('set', KEYS[2], redis.call('get', KEYS[3]))
    end
    local token = redis.call('incr', KEYS[2])
    redis.call('set', KEYS[3], redis.call('get', KEYS[2]))
    return token
end
if redis.call('set', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return win()
end
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return win()
end
return 0
`)

// renewScript extends the lock TTL only when the stored value matches the
// instance id, so a recovered instance cannot extend a lock another now holds.
//
// KEYS[1] lock key. ARGV[1] instance id, ARGV[2] TTL ms.
var renewScript = goredis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end
`)

// releaseScript deletes the lock only when the stored value matches the instance
// id, so an instance never deletes a lock another instance now holds.
//
// KEYS[1] lock key. ARGV[1] instance id.
var releaseScript = goredis.NewScript(`
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
`)

// fenceGuard is the Lua prologue shared by every fenced write. It loads the
// fence high-water mark — advanced by every elected term (see acquireScript)
// and every applied fenced write — and rejects the call when the caller's
// token (ARGV[1]) is lower. The caller's own write follows, then fenceApply
// advances the mark; all of it executes atomically in one round trip, so there
// is no window in which the token could go stale between check and write.
//
// Tokens below 1 are always rejected: real tokens start at 1, and 0 is the
// "not leader" sentinel — exactly what Token and Fencer hand out when this
// instance is not the leader. Rejecting it here means a caller that ignored the
// ok result cannot slip an unfenced write through before any leader has written.
//
// Tokens pass through Lua's number type (a float64), which is exact for
// integers up to 2^53 — far beyond any realistic election count, but the reason
// the comparison must never be fed arbitrary caller-supplied magnitudes.
//
// KEYS[1] is always the applied-high-water key; ARGV[1] is always the token.
// Each script inserts its own write between fenceGuard and fenceApply using the
// remaining KEYS/ARGV.
const fenceGuard = `
local applied = tonumber(redis.call('get', KEYS[1]) or '0')
local token = tonumber(ARGV[1])
if token < 1 or token < applied then
    return 0
end
`

// fenceApply is the Lua epilogue shared by every fenced write: it advances the
// high-water mark and reports the write as applied. It runs after the write,
// never before — a Redis script that hits a runtime error mid-way keeps its
// earlier effects (there is no rollback), so advancing the mark first would let
// a write that then failed fence out older tokens without having written
// anything.
//
// The mark is stored as ARGV[1] — the token exactly as the client sent it —
// rather than the Lua number from the guard, so the stored form never depends
// on the engine's number-to-string conversion (which varies across Redis
// versions and reimplementations, e.g. emitting "1e+15" for round values).
const fenceApply = `
redis.call('set', KEYS[1], ARGV[1])
return 1
`

// fenceHSetScript fences an HSET. KEYS[2] hash, ARGV[2] field, ARGV[3] value.
var fenceHSetScript = goredis.NewScript(fenceGuard + `
redis.call('hset', KEYS[2], ARGV[2], ARGV[3])
` + fenceApply)

// fenceSetScript fences a SET. KEYS[2] key, ARGV[2] value.
var fenceSetScript = goredis.NewScript(fenceGuard + `
redis.call('set', KEYS[2], ARGV[2])
` + fenceApply)

// acquireLock attempts to take the leader lock. On success it returns the
// fencing token for this term (a strictly increasing value); won is false when
// another instance holds the lock.
//
// The call carries a deadline of one acquire interval — by then the next
// attempt is due anyway, so a slower round trip has no value — capped at
// TTL - RenewInterval: the lock's TTL starts at the server-side SET, which can
// precede the response by the whole timeout, and a winning term must still fit
// one renew-bounded round trip inside what remains of the TTL budget (see
// hold).
//
// Like renewOnce, the wait is select-bounded so a client without I/O or
// context timeouts cannot hang the retry loop or shutdown. An abandoned
// acquire that wins at the server is harmless: the lock then holds this
// instance's own id, so the next attempt takes it over (or it lapses at the
// TTL), and no Fencer for the unknown term exists, so no write can carry its
// token.
func (e *Elector) acquireLock(ctx context.Context) (token int64, won bool, err error) {
	callCtx, cancel := context.WithTimeout(ctx, min(e.acquire, e.ttl-e.renew))
	defer cancel()

	type result struct {
		n   int64
		err error
	}

	res := make(chan result, 1)

	go func() {
		n, err := acquireScript.Run(callCtx, e.client, []string{e.lockKey, e.fenceKey, e.appliedKey}, e.id, e.ttlMillis()).Int64()
		res <- result{n: n, err: err}
	}()

	var r result

	select {
	case r = <-res:
	case <-callCtx.Done():
		// As in renewOnce: prefer a result delivered in a photo-finish with
		// the deadline over the context error.
		select {
		case r = <-res:
		default:
			return 0, false, callCtx.Err()
		}
	}

	if r.err != nil {
		return 0, false, r.err
	}

	if r.n == 0 {
		return 0, false, nil
	}

	return r.n, true, nil
}

// hold renews the lock until it is lost or ctx is cancelled. A renewal that
// returns 0 means another instance owns the lock (immediate step-down). A
// transient Redis error is tolerated until the lock TTL would have lapsed, after
// which we step down to avoid two leaders.
//
// lastRenew is when the request that last extended the lock was *sent* — for the
// first iteration, the time captured before the acquire call. The lock's true
// expiry is anchored at the server-side moment the SET/PEXPIRE applied, which
// lies between sending the request and seeing the response; anchoring the
// deadline at send time keeps time.Since(lastRenew) >= ttl a conservative bound,
// so the leader steps down at or before the lock actually lapses at Redis —
// except across a system suspend, where Go's monotonic clock can stop and the
// resumed leader underestimates elapsed time; that residual window is what
// fencing covers.
//
// The first renewal fires immediately (the acquire response may have consumed
// TTL budget before hold started), the deadline is enforced by a timer in the
// wait rather than polled per iteration, and each attempt's timeout is capped
// at the remaining budget — so neither a parked wait nor an in-flight round
// trip outlives the deadline.
func (e *Elector) hold(ctx context.Context, lastRenew time.Time) {
	t := time.NewTicker(e.renew)
	defer t.Stop()

	deadline := time.NewTimer(e.ttl - time.Since(lastRenew))
	defer deadline.Stop()

	for {
		budget := e.ttl - time.Since(lastRenew)
		if budget <= 0 {
			return // deadline passed; the lock may already have lapsed
		}

		attemptAt := time.Now()

		n, err := e.renewOnce(ctx, min(e.renew, budget))
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}

			e.reportError(err)

			if time.Since(lastRenew) >= e.ttl {
				return // renewal failing past TTL; step down
			}
		case n == 0:
			return // lock lost
		default:
			lastRenew = attemptAt

			// Stop-drain-Reset: the Go 1.23 semantics that make a bare Reset
			// safe are keyed to the consuming main module's go directive, not
			// this module's; under the old semantics a stale fire could linger
			// and force an early step-down.
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}

			deadline.Reset(e.ttl - time.Since(lastRenew))
		}

		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return // deadline reached with no renewal in between; step down
		case <-t.C:
		}
	}
}

// renewOnce sends one ownership-checked renewal and waits for its result for at
// most timeout — hold passes the renew interval (by then the next attempt is
// due, so a slower round trip has no value), shortened to the remaining TTL
// budget when that is smaller. The bound is enforced with a select rather than
// trusted to the Redis client: a client configured without I/O or context
// timeouts would otherwise block the hold loop past its step-down deadline, and
// stepping down on time must not depend on client configuration.
//
// A call that outlives its slot counts as a failed attempt and is abandoned;
// the deadline on its context reaps it as soon as the client honors contexts.
// An abandoned renewal that later reaches Redis can only extend this instance's
// own lock (the script is ownership-checked). The subsequent release usually
// deletes it; at worst a successor waits out the TTL — an availability cost,
// never a second leader.
func (e *Elector) renewOnce(ctx context.Context, timeout time.Duration) (int, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		n   int
		err error
	}

	res := make(chan result, 1)

	go func() {
		n, err := renewScript.Run(callCtx, e.client, []string{e.lockKey}, e.id, e.ttlMillis()).Int()
		res <- result{n: n, err: err}
	}()

	select {
	case r := <-res:
		return r.n, r.err
	case <-callCtx.Done():
		// A round trip that finished in a photo-finish with the deadline may
		// already have delivered its result; prefer it over the context error,
		// so a renewal that extended the lock at the server is never reported
		// as failed.
		select {
		case r := <-res:
			return r.n, r.err
		default:
			return 0, callCtx.Err()
		}
	}
}

// release deletes the lock if this instance still owns it, so a successor need
// not wait for the TTL to lapse. Best-effort: on failure the lock simply expires
// at its TTL, which only delays failover; the failure is reported through
// OnError. Like renewOnce, the wait is bounded with a select so a client blocked
// on an unresponsive server cannot stall shutdown; an abandoned delete is
// ownership-checked and thus always safe.
//
// The bound is one renew interval: release's only value is beating the lock's
// natural expiry, which a longer wait can never help — it would only slow
// shutdown.
func (e *Elector) release() {
	ctx, cancel := context.WithTimeout(context.Background(), e.renew)
	defer cancel()

	res := make(chan error, 1)

	go func() {
		res <- releaseScript.Run(ctx, e.client, []string{e.lockKey}, e.id).Err()
	}()

	var err error

	select {
	case err = <-res:
	case <-ctx.Done():
		// As in renewOnce: a delete that finished in a photo-finish with the
		// deadline may already have delivered its result; prefer it.
		select {
		case err = <-res:
		default:
			err = ctx.Err()
		}
	}

	if err != nil {
		e.reportError(err)
	}
}

// FenceHSet performs a fenced HSET of field=value into hashKey: the write is
// applied only if token is at least the fence high-water mark — the highest
// token minted by an election or carried by an applied fenced write. It returns
// true when applied and false when token is stale — a newer term has been
// elected or has written — so the caller can stop emitting derived events.
// token must be the value passed to the LeaderFunc for the current term. A
// non-nil error means the write could not be attempted.
//
// On Redis Cluster, hashKey must hash to the same slot as the elector's keys:
// the check and the write run in one script against the applied key and
// hashKey together, so give hashKey the same hash tag as [Config.Name] or the
// call fails with CROSSSLOT. The same holds for every Fence* helper's keys.
func (e *Elector) FenceHSet(ctx context.Context, token int64, hashKey, field, value string) (applied bool, err error) {
	n, err := fenceHSetScript.Run(ctx, e.client, []string{e.appliedKey, hashKey}, token, field, value).Int()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// FenceSet performs a fenced SET of key=value, applied only if token is current.
// Semantics match [Elector.FenceHSet].
func (e *Elector) FenceSet(ctx context.Context, token int64, key, value string) (applied bool, err error) {
	n, err := fenceSetScript.Run(ctx, e.client, []string{e.appliedKey, key}, token, value).Int()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// FenceEval fences an arbitrary Redis write supplied as a Lua body, for writes
// the typed helpers do not cover (ZADD, XADD, multi-key updates, and so on). The
// body runs atomically only when token is current; the fence returns 1 when
// applied and 0 when fenced out.
//
// The body must not contain its own return statement: FenceEval appends the
// fence's epilogue, which advances the token high-water mark and returns 1. A
// return inside body skips that epilogue, so the result reports
// applied == false and the mark does not advance even on a write that ran. Just
// perform the write and let FenceEval supply the return.
//
// Within body, the protected token check is already done. Address your own keys
// and arguments starting at index 2 — KEYS[1] and ARGV[1] are reserved for the
// fence (the high-water key and the token). Pass your keys in writeKeys and your
// arguments in args; they appear as KEYS[2..] and ARGV[2..] in that order. On
// Redis Cluster, every key in writeKeys must share the elector's hash slot;
// see [Elector.FenceHSet].
//
// Each distinct body compiles once and is cached on the Elector for EVALSHA
// reuse, capped at 256 bodies; past the cap FenceEval returns an error
// wrapping [ErrTooManyEvalBodies]. Keep bodies constant and pass variable data
// through writeKeys and args — interpolating values into the body creates a
// new cache entry (and a new script for Redis) per call.
//
// Example — fence a ZADD:
//
//	applied, err := e.FenceEval(ctx, token,
//	    "redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])",
//	    []string{"myset"}, "1.0", "member")
func (e *Elector) FenceEval(ctx context.Context, token int64, body string, writeKeys []string, args ...any) (applied bool, err error) {
	keys := make([]string, 0, len(writeKeys)+1)
	keys = append(keys, e.appliedKey)
	keys = append(keys, writeKeys...)

	evalArgs := make([]any, 0, len(args)+1)
	evalArgs = append(evalArgs, token)
	evalArgs = append(evalArgs, args...)

	script, err := e.evalScript(body)
	if err != nil {
		return false, err
	}

	n, err := script.Run(ctx, e.client, keys, evalArgs...).Int()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// maxEvalScripts bounds the FenceEval body cache; see ErrTooManyEvalBodies.
const maxEvalScripts = 256

// ErrTooManyEvalBodies is returned (wrapped) by [Elector.FenceEval] once more
// than maxEvalScripts distinct Lua bodies have been used. It signals a caller
// bug — variable data interpolated into the body — not a transient Redis
// error: check for it with errors.Is, fix the body to be constant (pass
// variable data through writeKeys and args), and do not retry.
var ErrTooManyEvalBodies = errors.New("redlease: too many distinct FenceEval bodies; keep bodies constant and pass variable data through writeKeys and args")

// evalScript returns the compiled fenced script for body, caching it so repeated
// FenceEval calls with the same body keep EVALSHA caching instead of recompiling
// and re-sending the source each time. It fails once maxEvalScripts distinct
// bodies have been cached and body is yet another one.
func (e *Elector) evalScript(body string) (*goredis.Script, error) {
	e.evalMu.RLock()
	s := e.evalScripts[body]
	e.evalMu.RUnlock()

	if s != nil {
		return s, nil
	}

	e.evalMu.Lock()
	defer e.evalMu.Unlock()

	if s := e.evalScripts[body]; s != nil {
		return s, nil
	}

	if len(e.evalScripts) >= maxEvalScripts {
		return nil, fmt.Errorf("%w (cap %d)", ErrTooManyEvalBodies, maxEvalScripts)
	}

	s = goredis.NewScript(fenceGuard + "\n" + body + "\n" + fenceApply)
	e.evalScripts[body] = s

	return s, nil
}

// ttlMillis renders the lock TTL in whole milliseconds for the Lua scripts,
// preserving sub-second precision that a seconds-granularity TTL would truncate.
func (e *Elector) ttlMillis() string {
	ms := max(e.ttl.Milliseconds(), 1)

	return strconv.FormatInt(ms, 10)
}
