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

// The fence high-water mark is shared across write types: a token advanced by
// one helper fences a later, lower-token write through any other helper.
func TestFence_HighWaterSharedAcrossWriteTypes(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")
	ctx := t.Context()

	// Advance the high-water mark to 5 via an HSET.
	applied, err := e.FenceHSet(ctx, 5, "h", "f", "v")
	require.NoError(t, err)
	require.True(t, applied)

	// A SET with a lower token must now be fenced out too.
	applied, err = e.FenceSet(ctx, 4, "k", "v")
	require.NoError(t, err)
	assert.False(t, applied, "high-water mark must be shared across write helpers")

	exists, err := rc.Exists(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "fenced-out SET must not have written the key")
}
