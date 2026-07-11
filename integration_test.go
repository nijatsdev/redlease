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
	"sync"
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

	// Election stamps the new token as the fence high-water mark, verbatim.
	mark, err := rc.Get(ctx, name+":fence:applied").Result()
	require.NoError(t, err)
	assert.Equal(t, "1", mark, "election must advance the fence high-water mark")

	// B cannot acquire while A holds the lock.
	_, won, err = b.acquireLock(ctx)
	require.NoError(t, err)
	assert.False(t, won)

	// A renews its own lock; B cannot renew a lock it does not hold.
	n, err := a.renewOnce(ctx, a.renew)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = b.renewOnce(ctx, b.renew)
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

// The core mint-path invariant under real concurrency: with N instances
// hammering acquire and release against a real Redis, every won token is
// globally unique and each instance's successive wins carry strictly
// increasing tokens. This is exactly what running SET NX and INCR in one
// atomic script exists to provide; the rest of the suite only ever exercises
// the mint path sequentially. ("Never two leaders at once" is deliberately
// not asserted — it is not an invariant lease-based election can promise;
// the token ordering is.)
func TestIntegration_ConcurrentContention_TokensUniqueAndIncreasing(t *testing.T) {
	t.Parallel()

	rc, name := integrationSetup(t)

	const (
		contenders = 8
		attempts   = 50
	)

	// Electors are built on the test goroutine: the helper uses require,
	// which must not run on spawned goroutines.
	electors := make([]*Elector, contenders)
	for i := range electors {
		electors[i] = integrationElector(t, rc, name, fmt.Sprintf("host-%d", i))
	}

	var (
		mu     sync.Mutex
		tokens []int64
	)

	var wg sync.WaitGroup

	for i := range contenders {
		wg.Add(1)

		go func() {
			defer wg.Done()

			e := electors[i]

			var last int64

			for range attempts {
				token, won, err := e.acquireLock(t.Context())
				if err != nil || !won {
					continue
				}

				assert.Greater(t, token, last, "an instance's successive wins must carry strictly increasing tokens")
				last = token

				mu.Lock()

				tokens = append(tokens, token)
				mu.Unlock()

				e.release()
			}
		}()
	}

	wg.Wait()

	require.NotEmpty(t, tokens, "at least some acquire attempts must have won")

	seen := make(map[int64]struct{}, len(tokens))
	for _, tok := range tokens {
		_, dup := seen[tok]
		assert.False(t, dup, "token %d was minted for two different wins", tok)
		seen[tok] = struct{}{}
	}
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

	// The counter was never touched by an election, so the applied mark (6) is
	// ahead of it. Winning must heal the counter from the mark and mint
	// strictly above everything ever applied, never regress the mark.
	token, won, err := e.acquireLock(ctx)
	require.NoError(t, err)
	require.True(t, won)
	assert.Equal(t, int64(7), token, "an election must mint above the applied mark")

	mark, err := rc.Get(ctx, name+":fence:applied").Result()
	require.NoError(t, err)
	assert.Equal(t, "7", mark, "the mark must only ever move up")
}
