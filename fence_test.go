package redlease

import (
	"testing"

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
