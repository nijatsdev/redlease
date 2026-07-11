package redlease

import (
	"context"
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
// KEYS[1] lock key, KEYS[2] fence counter. ARGV[1] instance id, ARGV[2] TTL ms.
var acquireScript = goredis.NewScript(`
if redis.call('set', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return redis.call('incr', KEYS[2])
else
    return 0
end
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
// highest applied token, rejects the call when the caller's token (ARGV[1]) is
// lower, and otherwise advances the high-water mark. The caller's own write
// follows, so the check and the write execute atomically in one round trip —
// there is no window in which the token could go stale between them.
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
// Each script appends its own write using the remaining KEYS/ARGV.
const fenceGuard = `
local applied = tonumber(redis.call('get', KEYS[1]) or '0')
local token = tonumber(ARGV[1])
if token < 1 or token < applied then
    return 0
end
redis.call('set', KEYS[1], token)
`

// fenceHSetScript fences an HSET. KEYS[2] hash, ARGV[2] field, ARGV[3] value.
var fenceHSetScript = goredis.NewScript(fenceGuard + `
redis.call('hset', KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// fenceSetScript fences a SET. KEYS[2] key, ARGV[2] value.
var fenceSetScript = goredis.NewScript(fenceGuard + `
redis.call('set', KEYS[2], ARGV[2])
return 1
`)

// acquireLock attempts to take the leader lock. On success it returns the
// fencing token for this term (a strictly increasing value); won is false when
// another instance holds the lock.
//
// The call carries a deadline of one acquire interval: by then the next attempt
// is due anyway, so a slower round trip has no value. Unlike renewals, the call
// is always awaited — an abandoned acquire could win the lock without the
// caller learning its token.
func (e *Elector) acquireLock(ctx context.Context) (token int64, won bool, err error) {
	callCtx, cancel := context.WithTimeout(ctx, e.acquire)
	defer cancel()

	n, err := acquireScript.Run(callCtx, e.client, []string{e.lockKey, e.fenceKey}, e.id, e.ttlMillis()).Int64()
	if err != nil {
		return 0, false, err
	}

	if n == 0 {
		return 0, false, nil
	}

	return n, true, nil
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
// so the leader steps down at or before the lock actually lapses at Redis.
func (e *Elector) hold(ctx context.Context, lastRenew time.Time) {
	t := time.NewTicker(e.renew)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			attemptAt := time.Now()

			n, err := e.renewOnce(ctx)
			switch {
			case err != nil:
				if ctx.Err() != nil {
					return
				}

				if time.Since(lastRenew) >= e.ttl {
					return // renewal failing past TTL; step down
				}
			case n == 0:
				return // lock lost
			default:
				lastRenew = attemptAt
			}
		}
	}
}

// renewOnce sends one ownership-checked renewal and waits for its result for at
// most one renew interval — by then the next attempt is due, so a slower round
// trip has no value. The bound is enforced with a select rather than trusted to
// the Redis client: a client configured without I/O or context timeouts would
// otherwise block the hold loop past its step-down deadline, and stepping down
// on time must not depend on client configuration.
//
// A call that outlives its slot counts as a failed attempt and is abandoned;
// the deadline on its context reaps it as soon as the client honors contexts.
// An abandoned renewal that later reaches Redis can only extend this instance's
// own lock (the script is ownership-checked). The subsequent release usually
// deletes it; at worst a successor waits out the TTL — an availability cost,
// never a second leader.
func (e *Elector) renewOnce(ctx context.Context) (int, error) {
	callCtx, cancel := context.WithTimeout(ctx, e.renew)

	type result struct {
		n   int
		err error
	}

	res := make(chan result, 1)

	go func() {
		defer cancel()

		n, err := renewScript.Run(callCtx, e.client, []string{e.lockKey}, e.id, e.ttlMillis()).Int()
		res <- result{n: n, err: err}
	}()

	select {
	case r := <-res:
		return r.n, r.err
	case <-callCtx.Done():
		return 0, callCtx.Err()
	}
}

// release deletes the lock if this instance still owns it, so a successor need
// not wait for the TTL to lapse. Best-effort: on failure the lock simply expires
// at its TTL, which only delays failover. Like renewOnce, the wait is bounded
// with a select so a client blocked on an unresponsive server cannot stall
// shutdown; an abandoned delete is ownership-checked and thus always safe.
func (e *Elector) release() {
	ctx, cancel := context.WithTimeout(context.Background(), e.ttl)

	done := make(chan struct{})

	go func() {
		defer cancel()
		defer close(done)

		_, _ = releaseScript.Run(ctx, e.client, []string{e.lockKey}, e.id).Result()
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// FenceHSet performs a fenced HSET of field=value into hashKey: the write is
// applied only if token is at least the highest token already applied through
// this elector. It returns true when applied and false when token is stale — a
// newer leadership term has since written — so the caller can stop emitting
// derived events. token must be the value passed to the LeaderFunc for the
// current term. A non-nil error means the write could not be attempted.
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
// fence's "return 1", so a return inside body shadows it and makes the result
// report applied == false even on a write that ran. Just perform the write and
// let FenceEval supply the return.
//
// Within body, the protected token check is already done. Address your own keys
// and arguments starting at index 2 — KEYS[1] and ARGV[1] are reserved for the
// fence (the high-water key and the token). Pass your keys in writeKeys and your
// arguments in args; they appear as KEYS[2..] and ARGV[2..] in that order.
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

	n, err := e.evalScript(body).Run(ctx, e.client, keys, evalArgs...).Int()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

// evalScript returns the compiled fenced script for body, caching it so repeated
// FenceEval calls with the same body keep EVALSHA caching instead of recompiling
// and re-sending the source each time.
func (e *Elector) evalScript(body string) *goredis.Script {
	if s, ok := e.evalScripts.Load(body); ok {
		if script, ok := s.(*goredis.Script); ok {
			return script
		}
	}

	s := goredis.NewScript(fenceGuard + "\n" + body + "\nreturn 1\n")
	actual, _ := e.evalScripts.LoadOrStore(body, s)

	if script, ok := actual.(*goredis.Script); ok {
		return script
	}

	return s
}

// ttlMillis renders the lock TTL in whole milliseconds for the Lua scripts,
// preserving sub-second precision that a seconds-granularity TTL would truncate.
func (e *Elector) ttlMillis() string {
	ms := max(e.ttl.Milliseconds(), 1)

	return strconv.FormatInt(ms, 10)
}
