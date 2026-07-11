package redlease

// Integration tests against a real Redis server. The rest of the suite runs on
// miniredis, whose Lua engine (gopher-lua) re-implements the one Redis embeds;
// the fencing guarantees rest entirely on script semantics, so these tests
// exercise the same scripts on the real engine. They are skipped unless
// REDIS_ADDR points at a reachable server:
//
//	REDIS_ADDR=localhost:6379 go test -race ./...
//
// Every test derives its keys from a unique lock name, so tests run in
// parallel and repeated runs never collide; the keys are deleted on cleanup.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integrationSetup returns a client for the server behind REDIS_ADDR (skipping
// the test when unset) and a unique lock name whose derived keys are removed on
// cleanup.
func integrationSetup(t *testing.T) (*goredis.Client, string) {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping real-Redis integration test")
	}

	rc := goredis.NewClient(&goredis.Options{Addr: addr})
	name := fmt.Sprintf("redlease-it:%s:%d", t.Name(), time.Now().UnixNano())

	t.Cleanup(func() {
		ctx := context.Background()
		_ = rc.Del(ctx, name+":leader", name+":fence", name+":fence:applied", name+":state").Err()
		_ = rc.Close()
	})

	return rc, name
}

func integrationElector(t *testing.T, rc *goredis.Client, name, id string) *Elector {
	t.Helper()

	e, err := New(rc, Config{
		Name:            name,
		TTL:             time.Second,
		RenewInterval:   100 * time.Millisecond,
		AcquireInterval: 100 * time.Millisecond,
		InstanceID:      id,
	})
	require.NoError(t, err)

	return e
}

func TestIntegration_AcquireContendRelease(t *testing.T) {
	t.Parallel()

	rc, name := integrationSetup(t)
	ctx := t.Context()

	a := integrationElector(t, rc, name, "host-a")
	b := integrationElector(t, rc, name, "host-b")

	// A wins the first term.
	tokenA, won, err := a.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won)
	assert.Equal(t, int64(1), tokenA)

	// B cannot acquire while A holds the lock.
	_, won, err = b.acquireLock(ctx)
	require.NoError(t, err)
	assert.False(t, won)

	// A renews its own lock; B cannot renew a lock it does not hold.
	n, err := a.renewOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = b.renewOnce(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "renewal must be ownership-checked")

	// A reacquires its own leftover lock as a new term with a greater token.
	tokenA2, won, err := a.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won, "an instance must take over its own leftover lock")
	assert.Greater(t, tokenA2, tokenA)

	// After A releases, B wins a strictly greater token.
	a.release()

	tokenB, won, err := b.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won, "the lock must be free after release")
	assert.Greater(t, tokenB, tokenA2, "each term must get a strictly greater fencing token")
}

func TestIntegration_FenceSemantics(t *testing.T) {
	t.Parallel()

	rc, name := integrationSetup(t)
	ctx := t.Context()

	e := integrationElector(t, rc, name, "host-a")
	state := name + ":state"

	// Tokens below 1 are rejected even before any leader has written.
	applied, err := e.FenceSet(ctx, 0, state, "sentinel")
	require.NoError(t, err)
	assert.False(t, applied, "token 0 must be rejected")

	// A current token applies; a stale one is fenced out.
	applied, err = e.FenceSet(ctx, 5, state, "v5")
	require.NoError(t, err)
	require.True(t, applied)

	applied, err = e.FenceSet(ctx, 3, state, "stale")
	require.NoError(t, err)
	assert.False(t, applied, "a lower token must be fenced out")

	val, err := rc.Get(ctx, state).Result()
	require.NoError(t, err)
	assert.Equal(t, "v5", val, "a fenced-out write must not change state")

	// A body that fails at runtime (ZADD on a string key) must surface the
	// error and must not advance the high-water mark.
	_, err = e.FenceEval(ctx, 7,
		"redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])",
		[]string{state}, "1", "member")
	require.Error(t, err, "a WRONGTYPE write must surface as an error")

	applied, err = e.FenceSet(ctx, 6, state, "v6")
	require.NoError(t, err)
	assert.True(t, applied, "a failed write must not advance the high-water mark")
}
