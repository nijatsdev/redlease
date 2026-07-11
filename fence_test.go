package redlease

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireLock_AssignsMonotonicTokens(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	// First term wins and gets token 1.
	token1, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)
	assert.Equal(t, int64(1), token1)

	// While the lock is held, a contender cannot acquire and gets no token.
	eB := newElector(t, rc, "host-b")

	token, won, err := eB.acquireLock(t.Context())
	require.NoError(t, err)
	assert.False(t, won)
	assert.Zero(t, token)

	// After the lock is released, the next term gets a strictly greater token.
	mr.Del("test:leader")

	token2, won, err := eB.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)
	assert.Greater(t, token2, token1, "each term must get a strictly greater fencing token")
}

// A lock left over from a previous term of this same instance — a release that
// never reached Redis, or a restart with a fixed InstanceID — is taken over
// immediately rather than waited out to its TTL, and the takeover is a new term:
// fresh token, refreshed TTL.
func TestAcquireLock_ReacquiresOwnLeftoverLock(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	require.NoError(t, mr.Set("test:leader", "host-a"))
	mr.SetTTL("test:leader", time.Hour)

	token, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	assert.True(t, won, "an instance must reacquire a lock it still holds instead of waiting out the TTL")
	assert.Equal(t, int64(1), token, "reacquisition mints a fresh token for the new term")
	assert.LessOrEqual(t, mr.TTL("test:leader"), testTTL, "reacquisition must refresh the TTL to the configured value")

	// Another instance's lock is still untouchable.
	eB := newElector(t, rc, "host-b")

	_, won, err = eB.acquireLock(t.Context())
	require.NoError(t, err)
	assert.False(t, won, "a lock held by a different instance must not be taken over")
}

// Electing a new term advances the fence high-water mark by itself: a deposed
// term's token goes stale at the moment of the successor's election, not only
// once the successor performs its first fenced write. Without this, a paused
// ex-leader could keep writing arbitrarily long into a new term whose leader
// writes lazily.
func TestAcquireLock_ElectionFencesOutPreviousTerm(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	eA := newElector(t, rc, "host-a")

	tokenA, won, err := eA.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	applied, err := eA.FenceHSet(t.Context(), tokenA, "state", "key", "from-A")
	require.NoError(t, err)
	require.True(t, applied)

	// A's lease lapses; B is elected and performs no fenced write.
	mr.Del("test:leader")

	eB := newElector(t, rc, "host-b")

	tokenB, won, err := eB.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)
	require.Greater(t, tokenB, tokenA)

	// The mark advanced at election, stored verbatim (tokens ran 1 then 2).
	mark, err := mr.Get("test:fence:applied")
	require.NoError(t, err)
	assert.Equal(t, "2", mark, "election must advance the fence high-water mark")

	// A's token is stale immediately — no write from B needed.
	applied, err = eA.FenceHSet(t.Context(), tokenA, "state", "key", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "a deposed term's token must be stale from the moment of election")
	assert.Equal(t, "from-A", mr.HGet("state", "key"))

	// The new term's own token writes as usual.
	applied, err = eB.FenceHSet(t.Context(), tokenB, "state", "key", "from-B")
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, "from-B", mr.HGet("state", "key"))
}

// An election must never regress the fence high-water mark. Two ways the mark
// can be ahead of the counter: the counter was lost (eviction, partial
// restore) while the mark survived, or a caller stamped an out-of-band token
// through NewFencer. Winning heals the counter from the mark first, so the new
// term's token lands strictly above everything ever applied — a regrown
// counter must not re-admit tokens the fence had already rejected.
func TestAcquireLock_NeverRegressesHighWaterMark(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// An out-of-band token puts the mark far ahead of the counter, which no
	// election has ever touched.
	applied, err := e.FenceHSet(ctx, 40, "state", "key", "v40")
	require.NoError(t, err)
	require.True(t, applied)

	token, won, err := e.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won)
	assert.Equal(t, int64(41), token, "an election must mint strictly above the applied mark")

	mark, err := mr.Get("test:fence:applied")
	require.NoError(t, err)
	assert.Equal(t, "41", mark, "the mark must only ever move up")

	applied, err = e.FenceHSet(ctx, 40, "state", "key", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "the out-of-band token must stay fenced out after the election")

	// The counter is lost while the mark survives; the next election must heal
	// the counter from the mark instead of restarting tokens at 1.
	mr.Del("test:leader")
	mr.Del("test:fence")

	token, won, err = e.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won)
	assert.Equal(t, int64(42), token, "a lost counter must be healed from the surviving mark")
}

func TestFenceHSet_RejectsStaleToken(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// A newer leader (token 5) writes first.
	applied, err := e.FenceHSet(ctx, 5, "state", "key", "new")
	require.NoError(t, err)
	require.True(t, applied)
	assert.Equal(t, "new", mr.HGet("state", "key"))

	// A stale leader (token 3) must be fenced out and must not overwrite.
	applied, err = e.FenceHSet(ctx, 3, "state", "key", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "a lower token must be fenced out")
	assert.Equal(t, "new", mr.HGet("state", "key"), "stale write must not overwrite newer state")

	// The same token may write again (idempotent re-publish within a term).
	applied, err = e.FenceHSet(ctx, 5, "state", "key", "same-term")
	require.NoError(t, err)
	assert.True(t, applied, "the current token must keep being accepted")
	assert.Equal(t, "same-term", mr.HGet("state", "key"))

	// A strictly greater token (new leader) takes over.
	applied, err = e.FenceHSet(ctx, 6, "state", "key", "newer")
	require.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, "newer", mr.HGet("state", "key"))
}

// Token 0 is the "not leader" sentinel that Token and Fencer return when this
// instance holds no leadership; the fence must reject it (and anything below 1)
// even before any leader has ever written, so a caller that ignored the ok
// result cannot slip an unfenced write through.
func TestFence_RejectsNonPositiveTokens(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// No fenced write has ever happened: the high-water mark is unset.
	applied, err := e.FenceHSet(ctx, 0, "state", "key", "sentinel")
	require.NoError(t, err)
	assert.False(t, applied, "token 0 must be rejected even with no prior writes")
	assert.Empty(t, mr.HGet("state", "key"))

	applied, err = e.FenceSet(ctx, -3, "note", "negative")
	require.NoError(t, err)
	assert.False(t, applied, "negative tokens must be rejected")

	// The zero Fencer an Elector returns while not leader is equally inert.
	f, ok := e.Fencer()
	require.False(t, ok)

	applied, err = f.Set(ctx, "note", "not-leader")
	require.NoError(t, err)
	assert.False(t, applied, "a not-leader Fencer's writes must be fenced out")

	// A rejected sentinel must not have advanced the high-water mark: the first
	// real term's token 1 still applies.
	applied, err = e.FenceHSet(ctx, 1, "state", "key", "first-term")
	require.NoError(t, err)
	assert.True(t, applied, "the first real token must still be accepted")
}

func TestFenceSet_RejectsStaleToken(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	applied, err := e.FenceSet(ctx, 5, "leader-note", "new")
	require.NoError(t, err)
	require.True(t, applied)

	val, err := mr.Get("leader-note")
	require.NoError(t, err)
	assert.Equal(t, "new", val)

	// Stale token is fenced out; value must stand.
	applied, err = e.FenceSet(ctx, 2, "leader-note", "stale")
	require.NoError(t, err)
	assert.False(t, applied)

	val, err = mr.Get("leader-note")
	require.NoError(t, err)
	assert.Equal(t, "new", val, "stale SET must not overwrite newer state")
}

func TestFenceEval_FencesArbitraryWrite(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// Fence a ZADD via the escape hatch. Caller addresses KEYS[2]/ARGV[2..].
	const zadd = "redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])"

	applied, err := e.FenceEval(ctx, 5, zadd, []string{"board"}, "100", "alice")
	require.NoError(t, err)
	require.True(t, applied)

	score, err := rc.ZScore(ctx, "board", "alice").Result()
	require.NoError(t, err)
	assert.InDelta(t, 100.0, score, 0.0001)

	// A stale token must be fenced out: the ZADD must not run.
	applied, err = e.FenceEval(ctx, 3, zadd, []string{"board"}, "999", "alice")
	require.NoError(t, err)
	assert.False(t, applied, "stale token must be fenced out")

	score, err = rc.ZScore(ctx, "board", "alice").Result()
	require.NoError(t, err)
	assert.InDelta(t, 100.0, score, 0.0001, "fenced-out ZADD must not change state")
}

// The FenceEval body cache is capped: past maxEvalScripts distinct bodies,
// FenceEval fails loudly — that many bodies almost certainly means variable
// data interpolated into the body, which would otherwise leak client memory
// and Redis's script cache silently and forever. Already-cached bodies keep
// working at the cap.
func TestFenceEval_RejectsUnboundedDistinctBodies(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// Fill the cache to the cap; compilation alone caches, no Redis needed.
	for i := range maxEvalScripts {
		_, err := e.evalScript(fmt.Sprintf("redis.call('set', KEYS[2], %d)", i))
		require.NoError(t, err)
	}

	_, err := e.FenceEval(ctx, 1, "redis.call('set', KEYS[2], 'one-too-many')", []string{"k"})
	require.Error(t, err, "a body past the cap must fail loudly, not leak")
	require.ErrorIs(t, err, ErrTooManyEvalBodies,
		"the cap error must be detectable with errors.Is — it is a caller bug, not a transient Redis error")

	applied, err := e.FenceEval(ctx, 1, "redis.call('set', KEYS[2], 0)", []string{"k"})
	require.NoError(t, err, "an already-cached body must keep working at the cap")
	assert.True(t, applied)

	val, err := mr.Get("k")
	require.NoError(t, err)
	assert.Equal(t, "0", val)
}

// A fenced write whose body fails at runtime must not advance the high-water
// mark: Redis scripts keep their earlier effects on a mid-script error (no
// rollback), so the mark moves only in the epilogue, after the write. A token
// that only ever failed to write must not fence out older, still-valid writers.
func TestFenceEval_FailedBodyDoesNotAdvanceHighWaterMark(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// Term 5 writes; the mark is now 5.
	applied, err := e.FenceHSet(ctx, 5, "state", "key", "v5")
	require.NoError(t, err)
	require.True(t, applied)

	// Term 7 attempts a write that fails at runtime: ZADD against a string key.
	require.NoError(t, mr.Set("plain", "not-a-zset"))

	_, err = e.FenceEval(ctx, 7,
		"redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])",
		[]string{"plain"}, "1", "member")
	require.Error(t, err, "a WRONGTYPE write must surface as an error")

	// Term 6 must still be able to write: the failed term-7 attempt never
	// applied anything, so it must not have raised the mark past 6.
	applied, err = e.FenceHSet(ctx, 6, "state", "key", "v6")
	require.NoError(t, err)
	assert.True(t, applied, "a failed write must not advance the high-water mark")
	assert.Equal(t, "v6", mr.HGet("state", "key"))
}

// The high-water mark is stored verbatim — the token string the client sent —
// never through the Lua engine's number-to-string conversion, whose output
// varies across Redis versions and reimplementations (scientific notation for
// round values, "%.14g" truncation under Lua's own tostring). Verbatim storage
// makes the comparison's 2^53 float bound the only limit, on every engine.
func TestFence_StoresLargeTokensExactly(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	const big = int64(1_000_000_000_000_001) // 16 digits: "%.14g" would store "1e+15"

	applied, err := e.FenceHSet(ctx, big, "state", "key", "big")
	require.NoError(t, err)
	require.True(t, applied)

	mark, err := mr.Get("test:fence:applied")
	require.NoError(t, err)
	assert.Equal(t, "1000000000000001", mark, "the mark must be stored verbatim")

	// A token one below — exactly what the rounded mark would have been — must
	// be fenced out.
	applied, err = e.FenceHSet(ctx, big-1, "state", "key", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "a token below an exactly-stored mark must be fenced out")
	assert.Equal(t, "big", mr.HGet("state", "key"))
}

// A Fencer bound to a term applies its write under that term's token, and is
// fenced out once a newer term has advanced the high-water mark. Because a
// newer-token HSet here fences out a later Set and Eval, this also covers the
// high-water mark being shared across all three write types.
func TestFencer_BindsTokenToWrites(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	f := NewFencer(e, 5)
	assert.Equal(t, int64(5), f.Token())

	applied, err := f.HSet(ctx, "state", "key", "via-fencer")
	require.NoError(t, err)
	require.True(t, applied)
	assert.Equal(t, "via-fencer", mr.HGet("state", "key"))

	applied, err = f.Set(ctx, "note", "via-fencer")
	require.NoError(t, err)
	require.True(t, applied)

	const zadd = "redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])"

	applied, err = f.Eval(ctx, zadd, []string{"board"}, "100", "alice")
	require.NoError(t, err)
	require.True(t, applied)

	// A newer term advances the high-water mark past this Fencer's token.
	newer := NewFencer(e, 6)

	applied, err = newer.HSet(ctx, "state", "key", "newer")
	require.NoError(t, err)
	require.True(t, applied)

	// The stale Fencer is now fenced out across all of its write methods.
	applied, err = f.HSet(ctx, "state", "key", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "stale Fencer.HSet must be fenced out")

	applied, err = f.Set(ctx, "note", "stale")
	require.NoError(t, err)
	assert.False(t, applied, "stale Fencer.Set must be fenced out")

	applied, err = f.Eval(ctx, zadd, []string{"board"}, "999", "alice")
	require.NoError(t, err)
	assert.False(t, applied, "stale Fencer.Eval must be fenced out")

	assert.Equal(t, "newer", mr.HGet("state", "key"))
}
