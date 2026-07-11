package redlease

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fast keeps the election tests quick.
const (
	testTTL     = 2 * time.Second
	testRenew   = 50 * time.Millisecond
	testAcquire = 50 * time.Millisecond
	waitFor     = 2 * time.Second
)

func newRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	return mr, rc
}

func newElector(t *testing.T, rc *goredis.Client, id string) *Elector {
	t.Helper()

	e, err := New(rc, Config{
		Name:            "test",
		TTL:             testTTL,
		RenewInterval:   testRenew,
		AcquireInterval: testAcquire,
		InstanceID:      id,
	})
	require.NoError(t, err)

	return e
}

// signal is a one-shot, race-safe close used to observe election transitions.
type signal struct {
	once sync.Once
	ch   chan struct{}
}

func newSignal() *signal { return &signal{ch: make(chan struct{})} }
func (s *signal) fire()  { s.once.Do(func() { close(s.ch) }) }

func fired(s *signal, d time.Duration) bool {
	select {
	case <-s.ch:
		return true
	case <-time.After(d):
		return false
	}
}

// runElector starts an elector and returns elected/steppedDown signals plus a
// channel closed when Run exits. It captures the token handed to the leader.
// The token is atomic, not merely ordered by the elected signal: the signal's
// close gives happens-before for the first term only, and a mid-test
// re-election would make a plain write race with the reader.
func runElector(ctx context.Context, e *Elector) (elected, steppedDown *signal, token *atomic.Int64, exited chan struct{}) {
	elected, steppedDown = newSignal(), newSignal()
	token = new(atomic.Int64)
	exited = make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
			token.Store(f.Token())

			elected.fire()
			<-leaderCtx.Done()
			steppedDown.fire()
		})
	}()

	return elected, steppedDown, token, exited
}

// TestRun_DeliversFencerThatWrites exercises the primary path: a real leadership
// term receives a Fencer through its LeaderFunc, and that Fencer's write applies
// under the term's token. Other tests synthesize a Fencer with NewFencer; this
// one proves the one a LeaderFunc actually receives is wired and writable.
func TestRun_DeliversFencerThatWrites(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	ctx, cancel := context.WithCancel(t.Context())

	type result struct {
		token   int64
		applied bool
		err     error
	}

	got := make(chan result, 1)
	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
			applied, err := f.HSet(leaderCtx, "state", "key", "via-callback")
			got <- result{token: f.Token(), applied: applied, err: err}

			<-leaderCtx.Done()
		})
	}()

	t.Cleanup(func() {
		cancel()
		<-exited
	})

	select {
	case r := <-got:
		require.NoError(t, r.err)
		assert.Equal(t, int64(1), r.token, "callback Fencer must carry the term's token")
		assert.True(t, r.applied, "callback Fencer's write must apply")
		assert.Equal(t, "via-callback", mr.HGet("state", "key"))
	case <-time.After(waitFor):
		t.Fatal("LeaderFunc was not invoked")
	}
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	_, err := New(nil, Config{Name: "x"})
	require.Error(t, err)

	_, err = New(rc, Config{Name: ""})
	require.Error(t, err)

	// RenewInterval equal to TTL leaves no renewal headroom.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: time.Second})
	require.Error(t, err)

	// RenewInterval above TTL/2 is rejected: fewer than two attempts fit before
	// expiry, so a single dropped renewal costs leadership.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: 600 * time.Millisecond})
	require.Error(t, err)

	// RenewInterval at exactly TTL/2 is accepted.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: 500 * time.Millisecond})
	require.NoError(t, err)

	// AcquireInterval must be < TTL: a follower that polls slower than one lock
	// lifetime would fail over far slower than necessary.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: 200 * time.Millisecond, AcquireInterval: 2 * time.Second})
	require.Error(t, err)

	// TTL below the floor is rejected: renewal cannot reliably beat expiry.
	_, err = New(rc, Config{Name: "x", TTL: 50 * time.Millisecond, RenewInterval: 10 * time.Millisecond})
	require.Error(t, err)

	// A TTL at the floor with sane intervals is accepted.
	_, err = New(rc, Config{Name: "x", TTL: minTTL, RenewInterval: 20 * time.Millisecond, AcquireInterval: 20 * time.Millisecond})
	require.NoError(t, err)

	// Intervals below 1ms guarantee every round trip times out before Redis
	// can answer: the elector would flap forever. Rejected with a clear error.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: time.Nanosecond})
	require.Error(t, err)

	_, err = New(rc, Config{Name: "x", TTL: time.Second, AcquireInterval: 500 * time.Microsecond})
	require.Error(t, err)

	// Negative durations are configuration mistakes, not "unset": rejected
	// rather than silently replaced with defaults.
	_, err = New(rc, Config{Name: "x", TTL: -time.Second})
	require.Error(t, err)

	_, err = New(rc, Config{Name: "x", RenewInterval: -time.Millisecond})
	require.Error(t, err)

	_, err = New(rc, Config{Name: "x", AcquireInterval: -time.Millisecond})
	require.Error(t, err)

	// Defaults fill in for zero timing fields.
	e, err := New(rc, Config{Name: "x"})
	require.NoError(t, err)
	assert.NotEmpty(t, e.InstanceID())
}

// jitter perturbs the acquire interval only downward, by at most 10%: followers
// spread their polls without ever polling slower than the configured interval,
// so the AcquireInterval < TTL bound survives.
func TestJitter_ShortensByAtMostTenPercent(t *testing.T) {
	t.Parallel()

	const d = 100 * time.Millisecond

	for range 1000 {
		got := jitter(d)
		assert.LessOrEqual(t, got, d, "jitter must never lengthen the interval")
		assert.GreaterOrEqual(t, got, 90*time.Millisecond, "jitter must shorten by at most 10%")
	}
}

func TestTTLMillis_PreservesSubSecondPrecision(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	// A sub-second TTL must not collapse to 0 or round to a whole second.
	e, err := New(rc, Config{Name: "x", TTL: 1500 * time.Millisecond, RenewInterval: 200 * time.Millisecond, AcquireInterval: 200 * time.Millisecond})
	require.NoError(t, err)
	assert.Equal(t, "1500", e.ttlMillis(), "TTL must be expressed in whole milliseconds")

	// A pathologically small TTL still yields at least 1ms, never 0.
	tiny := &Elector{ttl: 1 * time.Nanosecond}
	assert.Equal(t, "1", tiny.ttlMillis())
}

// A fractional-millisecond TTL is truncated at construction: Redis grants the
// lease in whole milliseconds, and the client-side step-down deadline must
// never budget against precision the server was not told about — otherwise a
// sub-millisecond window opens where the lock has lapsed at Redis while this
// instance still believes it is leader.
func TestNew_TruncatesFractionalTTL(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	e, err := New(rc, Config{
		Name:            "x",
		TTL:             150*time.Millisecond + 900*time.Microsecond,
		RenewInterval:   20 * time.Millisecond,
		AcquireInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Equal(t, 150*time.Millisecond, e.ttl, "the client-side TTL must match the whole milliseconds Redis is told")
}

func TestAcquire_SetsMillisecondLockTTL(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	e, err := New(rc, Config{Name: "test", TTL: 1500 * time.Millisecond, RenewInterval: 200 * time.Millisecond, AcquireInterval: 200 * time.Millisecond, InstanceID: "host-a"})
	require.NoError(t, err)

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// The lock's TTL must reflect the sub-second value, not a truncated 1s.
	ttl := mr.TTL("test:leader")
	assert.Greater(t, ttl, time.Second, "lock TTL must preserve the 1500ms setting, not truncate to 1s")
	assert.LessOrEqual(t, ttl, 1500*time.Millisecond)
}

func TestRun_ElectsAndReleasesLock(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	ctx, cancel := context.WithCancel(t.Context())
	elected, _, token, exited := runElector(ctx, newElector(t, rc, "host-a"))

	require.True(t, fired(elected, waitFor), "instance was not elected")
	assert.Equal(t, int64(1), token.Load(), "first term must get token 1")

	val, err := mr.Get("test:leader")
	require.NoError(t, err)
	assert.Equal(t, "host-a", val, "lock must be held by the elected instance")

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	// Graceful step-down releases the lock so a successor need not wait for TTL.
	require.ErrorIs(t, rc.Get(context.Background(), "test:leader").Err(), goredis.Nil,
		"lock must be released on shutdown")
}

func TestRun_NilCallback_TokenDrivenStyle(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	// No LeaderFunc: Run just keeps us elected; work happens out here.
	go func() {
		defer close(exited)

		e.Run(ctx, nil)
	}()

	// Once elected, the token is reachable via Token and a fenced write works.
	require.Eventually(t, e.IsLeader, waitFor, 10*time.Millisecond, "did not become leader")

	token, ok := e.Token()
	require.True(t, ok)

	applied, err := e.FenceHSet(ctx, token, "state", "key", "value")
	require.NoError(t, err)
	assert.True(t, applied, "fenced write under the held token must apply")
	assert.Equal(t, "value", mr.HGet("state", "key"))

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	// Lock released on shutdown even with no callback.
	require.ErrorIs(t, rc.Get(context.Background(), "test:leader").Err(), goredis.Nil,
		"lock must be released on shutdown")
	assert.False(t, e.IsLeader(), "must not be leader after shutdown")
}

// A second Run on the same Elector while one is already running would corrupt
// shared leadership state; it must panic rather than fail silently. A new Run
// after the previous one returned is legitimate.
func TestRun_PanicsWhenCalledConcurrently(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, nil)
	}()

	// Once leader, the first Run is certainly inside its loop.
	require.Eventually(t, e.IsLeader, waitFor, 10*time.Millisecond, "did not become leader")

	assert.Panics(t, func() { e.Run(ctx, nil) }, "a concurrent Run must panic")

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	// Sequential reuse stays allowed: a fresh Run after the previous returned.
	assert.NotPanics(t, func() { e.Run(ctx, nil) }, "a sequential Run must not panic")
}

// Returning from the LeaderFunc ends the term: the lock is released and Run
// re-contends, rather than holding leadership idle with no work running. The
// next win is a new term with a strictly greater token.
func TestRun_LeaderFuncReturnEndsTerm(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	ctx, cancel := context.WithCancel(t.Context())

	var calls atomic.Int64

	tokens := make(chan int64, 2)
	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
			tokens <- f.Token()

			if calls.Add(1) == 1 {
				return // first term ends by returning
			}

			<-leaderCtx.Done()
		})
	}()

	t.Cleanup(func() {
		cancel()
		<-exited
	})

	readToken := func() int64 {
		select {
		case tok := <-tokens:
			return tok
		case <-time.After(waitFor):
			t.Fatal("LeaderFunc was not invoked")
			return 0
		}
	}

	first := readToken()
	second := readToken()
	assert.Greater(t, second, first, "the term after an early return must be a fresh term with a greater token")
}

// Resign ends the current term from any goroutine — the essential step-down
// lever for the token-driven style — and Run then re-contends, so a later term
// can begin with a fresh token.
func TestResign_EndsTermAndRecontends(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	// Resign while not leading is a no-op.
	e.Resign()

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, nil)
	}()

	require.Eventually(t, e.IsLeader, waitFor, 10*time.Millisecond, "did not become leader")

	first, ok := e.Token()
	require.True(t, ok)

	e.Resign()

	// The first term ends (token drops to 0) and, the lock being free again,
	// Run re-contends and wins a fresh term. Waiting for the token to move past
	// the first term covers both without racing the quick re-election.
	require.Eventually(t, func() bool { return e.token.Load() != first }, waitFor, 10*time.Millisecond,
		"Resign must end the current term")
	require.Eventually(t, e.IsLeader, waitFor, 10*time.Millisecond, "Run must re-contend after Resign")

	second, ok := e.Token()
	require.True(t, ok)
	assert.Greater(t, second, first, "the term after Resign must carry a fresh token")

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}
}

func TestTokenAndIsLeader_TrackLeadership(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	// Before election: not the leader, no token.
	assert.False(t, e.IsLeader(), "must not be leader before Run")

	tok, ok := e.Token()
	assert.False(t, ok)
	assert.Zero(t, tok)

	ctx, cancel := context.WithCancel(t.Context())

	elected := newSignal()
	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, _ Fencer) {
			elected.fire()
			<-leaderCtx.Done()
		})
	}()

	require.True(t, fired(elected, waitFor), "instance was not elected")

	// While leader: IsLeader true, Token returns the current term's token.
	assert.True(t, e.IsLeader(), "must report leader while elected")

	tok, ok = e.Token()
	assert.True(t, ok)
	assert.Equal(t, int64(1), tok, "Token must return the term's fencing token")

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	// After step-down: leadership cleared.
	assert.False(t, e.IsLeader(), "must not report leader after step-down")

	tok, ok = e.Token()
	assert.False(t, ok)
	assert.Zero(t, tok)
}

func TestRun_FollowerTakesOverWhenLeaderExits(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	rcB := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rcB.Close() })

	ctxA, cancelA := context.WithCancel(t.Context())
	electedA, _, tokenA, exitedA := runElector(ctxA, newElector(t, rc, "host-a"))
	require.True(t, fired(electedA, waitFor), "A was not elected")

	ctxB, cancelB := context.WithCancel(t.Context())

	electedB, _, tokenB, exitedB := runElector(ctxB, newElector(t, rcB, "host-b"))

	t.Cleanup(func() {
		cancelB()
		<-exitedB
	})

	assert.False(t, fired(electedB, 300*time.Millisecond), "B led while A still held the lock")

	cancelA()

	select {
	case <-exitedA:
	case <-time.After(waitFor):
		t.Fatal("A did not exit")
	}

	require.True(t, fired(electedB, waitFor), "B did not take over after A exited")
	assert.Greater(t, tokenB.Load(), tokenA.Load(), "successor must get a strictly greater fencing token")
}

// OnFollower fires once per transition into the follower role, not once ever: an
// instance that follows, wins a term, then loses it must see OnFollower again.
// Phase 1 covers the first fire (the lock starts held by another instance);
// phase 3 covers the re-entry the Observer doc promises ("again only after an
// intervening leadership term").
func TestObserver_FiresOnFollowerAgainAfterLosingLeadership(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	// Start as a follower: another instance holds the lock.
	require.NoError(t, mr.Set("test:leader", "someone-else"))
	mr.SetTTL("test:leader", time.Hour)

	var followerCount atomic.Int64

	elected := newSignal()

	e, err := New(rc, Config{
		Name:            "test",
		TTL:             testTTL,
		RenewInterval:   testRenew,
		AcquireInterval: testAcquire,
		InstanceID:      "host-b",
		Observer: Observer{
			OnFollower: func() { followerCount.Add(1) },
			OnElected:  func(int64) { elected.fire() },
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, nil)
	}()

	t.Cleanup(func() {
		cancel()
		<-exited
	})

	// Phase 1 — follower: OnFollower fires once for entering the role.
	require.Eventually(t, func() bool { return followerCount.Load() == 1 },
		waitFor, 10*time.Millisecond, "OnFollower must fire on first becoming a follower")

	// Phase 2 — leader: free the lock so this instance wins the next acquire.
	mr.Del("test:leader")
	require.True(t, fired(elected, waitFor), "instance must win once the lock is free")

	// Phase 3 — follower again: steal the lock so the leader's renew sees it lost
	// and steps down, then re-enters the follower role on the next failed acquire.
	require.NoError(t, mr.Set("test:leader", "thief"))
	mr.SetTTL("test:leader", time.Hour)

	require.Eventually(t, func() bool { return followerCount.Load() == 2 },
		waitFor, 10*time.Millisecond, "OnFollower must fire again after an intervening leadership term")
}

// OnElected fires once per leadership term, not once ever, and each term carries
// a strictly greater fencing token. An instance that wins, loses, then wins again
// must see OnElected twice with an increasing token — the symmetric counterpart
// to the OnFollower re-entry above, and the contract that gives every term a
// fresh fence.
func TestObserver_FiresOnElectedEachTermWithGreaterToken(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	var (
		mu     sync.Mutex
		tokens []int64
	)

	e, err := New(rc, Config{
		Name:            "test",
		TTL:             testTTL,
		RenewInterval:   testRenew,
		AcquireInterval: testAcquire,
		InstanceID:      "host-a",
		Observer: Observer{
			OnElected: func(token int64) {
				mu.Lock()
				defer mu.Unlock()

				tokens = append(tokens, token)
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, nil)
	}()

	t.Cleanup(func() {
		cancel()
		<-exited
	})

	count := func() int {
		mu.Lock()
		defer mu.Unlock()

		return len(tokens)
	}

	// Term 1: nothing holds the lock, so this instance wins.
	require.Eventually(t, func() bool { return count() == 1 },
		waitFor, 10*time.Millisecond, "OnElected must fire on the first term")

	// Steal the lock so the leader's renew sees it lost and steps down.
	require.NoError(t, mr.Set("test:leader", "thief"))
	mr.SetTTL("test:leader", testTTL)

	// Free it again so this instance wins a second term.
	require.Eventually(t, func() bool { return !e.IsLeader() },
		waitFor, 10*time.Millisecond, "instance must step down once the lock is stolen")
	mr.Del("test:leader")

	require.Eventually(t, func() bool { return count() == 2 },
		waitFor, 10*time.Millisecond, "OnElected must fire again on a second term")

	mu.Lock()
	defer mu.Unlock()

	assert.Greater(t, tokens[1], tokens[0], "each term's fencing token must be strictly greater")
}

// blackholeServer accepts TCP connections and never responds — a stand-in for a
// network partition or a hung server. Accepted connections are closed on test
// cleanup so blocked client reads unwind.
func blackholeServer(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		conns []net.Conn
	)

	t.Cleanup(func() {
		_ = ln.Close()

		mu.Lock()
		defer mu.Unlock()

		for _, c := range conns {
			_ = c.Close()
		}
	})

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}

			mu.Lock()

			conns = append(conns, c)
			mu.Unlock()
		}
	}()

	return ln
}

// errScripter satisfies the Redis interface and fails every call, counting the
// attempts — a stand-in for a Redis that is down but fails fast.
type errScripter struct {
	calls atomic.Int64
}

func (s *errScripter) fail(cmd interface{ SetErr(error) }) {
	s.calls.Add(1)
	cmd.SetErr(assert.AnError)
}

func (s *errScripter) Eval(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx)
	s.fail(cmd)

	return cmd
}

func (s *errScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx)
	s.fail(cmd)

	return cmd
}

func (s *errScripter) EvalRO(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx)
	s.fail(cmd)

	return cmd
}

func (s *errScripter) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx)
	s.fail(cmd)

	return cmd
}

func (s *errScripter) ScriptExists(ctx context.Context, _ ...string) *goredis.BoolSliceCmd {
	cmd := goredis.NewBoolSliceCmd(ctx)
	s.fail(cmd)

	return cmd
}

func (s *errScripter) ScriptLoad(ctx context.Context, _ string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx)
	s.fail(cmd)

	return cmd
}

// okScripter satisfies the Redis interface and replies success instantly — the
// timing that maximizes the chance a round trip completes before the caller
// parks on its select, which is exactly when a self-inflicted context
// cancellation could race the delivered result.
type okScripter struct{}

func (okScripter) ok(ctx context.Context) *goredis.Cmd {
	cmd := goredis.NewCmd(ctx)
	cmd.SetVal(int64(1))

	return cmd
}

func (s okScripter) Eval(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	return s.ok(ctx)
}

func (s okScripter) EvalSha(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	return s.ok(ctx)
}

func (s okScripter) EvalRO(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	return s.ok(ctx)
}

func (s okScripter) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...any) *goredis.Cmd {
	return s.ok(ctx)
}

func (okScripter) ScriptExists(ctx context.Context, _ ...string) *goredis.BoolSliceCmd {
	cmd := goredis.NewBoolSliceCmd(ctx)
	cmd.SetVal([]bool{true})

	return cmd
}

func (okScripter) ScriptLoad(ctx context.Context, _ string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx)
	cmd.SetVal("sha")

	return cmd
}

// A renewal that succeeded at the server must never be reported as an error:
// the result and the goroutine's own deferred cancel used to race in
// renewOnce's select, occasionally returning context.Canceled for a completed
// round trip (~0.07% of calls with an instant server). The loop makes the
// old race a near-certain failure while staying quick.
func TestRenewOnce_NeverReportsASuccessfulRenewalAsError(t *testing.T) {
	t.Parallel()

	e, err := New(okScripter{}, Config{
		Name:            "test",
		TTL:             time.Second,
		RenewInterval:   100 * time.Millisecond,
		AcquireInterval: 100 * time.Millisecond,
		InstanceID:      "host-a",
	})
	require.NoError(t, err)

	for range 20000 {
		n, err := e.renewOnce(t.Context(), e.renew)
		require.NoError(t, err, "a successful renewal must never surface as an error")
		require.Equal(t, 1, n)
	}
}

// The same race lived in release: a delete that succeeded instantly could be
// reported to OnError as context.Canceled.
func TestRelease_NeverReportsASuccessfulDeleteAsError(t *testing.T) {
	t.Parallel()

	var errCount atomic.Int64

	e, err := New(okScripter{}, Config{
		Name:            "test",
		TTL:             time.Second,
		RenewInterval:   100 * time.Millisecond,
		AcquireInterval: 100 * time.Millisecond,
		InstanceID:      "host-a",
		Observer:        Observer{OnError: func(error) { errCount.Add(1) }},
	})
	require.NoError(t, err)

	for range 5000 {
		e.release()
	}

	assert.Zero(t, errCount.Load(), "a successful release must never surface through OnError")
}

// Consecutive acquire errors back off exponentially (capped at TTL) instead of
// retrying at the full acquire cadence: with acquire = 50ms and TTL = 200ms the
// retry delays run 50, 100, 200, 200, ... ms, so a 600ms window sees about 5
// attempts where fixed-cadence polling would fire roughly 12. The generous
// upper bound keeps the assertion meaningful without being timing-sensitive.
func TestRun_BacksOffOnConsecutiveAcquireErrors(t *testing.T) {
	t.Parallel()

	s := &errScripter{}

	e, err := New(s, Config{
		Name:            "test",
		TTL:             200 * time.Millisecond,
		RenewInterval:   50 * time.Millisecond,
		AcquireInterval: 50 * time.Millisecond,
		InstanceID:      "host-a",
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()

	e.Run(ctx, nil)

	calls := s.calls.Load()
	assert.GreaterOrEqual(t, calls, int64(2), "Run must keep retrying through errors")
	assert.LessOrEqual(t, calls, int64(8), "consecutive errors must back off, not poll at full cadence")
}

// OnError surfaces transient Redis errors the elector handles internally, so
// callers can log or alert on trouble before it costs leadership.
func TestObserver_OnErrorSurfacesAcquireErrors(t *testing.T) {
	t.Parallel()

	var errCount atomic.Int64

	s := &errScripter{}

	e, err := New(s, Config{
		Name:            "test",
		TTL:             200 * time.Millisecond,
		RenewInterval:   50 * time.Millisecond,
		AcquireInterval: 50 * time.Millisecond,
		InstanceID:      "host-a",
		Observer:        Observer{OnError: func(error) { errCount.Add(1) }},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	e.Run(ctx, nil)

	assert.Positive(t, errCount.Load(), "acquire errors must surface through OnError")
}

func TestObserver_OnErrorSurfacesRenewalErrors(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	// Retries disabled so each failed renewal errors immediately.
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1})

	t.Cleanup(func() { _ = rc.Close() })

	var errCount atomic.Int64

	e, err := New(rc, Config{
		Name:            "test",
		TTL:             200 * time.Millisecond,
		RenewInterval:   50 * time.Millisecond,
		AcquireInterval: 50 * time.Millisecond,
		InstanceID:      "host-a",
		Observer:        Observer{OnError: func(error) { errCount.Add(1) }},
	})
	require.NoError(t, err)

	acquiredAt := time.Now()

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// Kill Redis so every renewal errors; hold reports each through OnError
	// while tolerating them until the TTL deadline.
	mr.Close()

	e.hold(t.Context(), acquiredAt)

	assert.Positive(t, errCount.Load(), "renewal errors must surface through OnError")
}

// An acquire against an unresponsive server must fail within its interval even
// when the Redis client has no I/O timeouts and ignores context deadlines —
// otherwise a follower's retry loop (and shutdown) would hang forever. An
// abandoned acquire that wins server-side is recoverable: the lock holds this
// instance's own id, so the next attempt takes it over.
func TestAcquireLock_BoundedAgainstUnresponsiveServer(t *testing.T) {
	t.Parallel()

	ln := blackholeServer(t)

	// No I/O timeouts and no retries: only redlease's own bound can unblock.
	rc := goredis.NewClient(&goredis.Options{
		Addr:         ln.Addr().String(),
		DialTimeout:  time.Second,
		ReadTimeout:  -1,
		WriteTimeout: -1,
		MaxRetries:   -1,
	})

	t.Cleanup(func() { _ = rc.Close() })

	const interval = 100 * time.Millisecond

	e, err := New(rc, Config{Name: "test", TTL: time.Second, RenewInterval: interval, AcquireInterval: interval, InstanceID: "host-a"})
	require.NoError(t, err)

	start := time.Now()

	_, _, err = e.acquireLock(t.Context())
	require.Error(t, err, "acquire against an unresponsive server must fail, not hang")
	assert.Less(t, time.Since(start), waitFor, "acquire must be bounded by its interval, not block indefinitely")
}

// A renewal against an unresponsive server must fail within one renew interval
// even when the Redis client is configured with no I/O timeouts and does not
// honor context deadlines: stepping down on time must not depend on client
// configuration.
func TestRenewOnce_BoundedAgainstUnresponsiveServer(t *testing.T) {
	t.Parallel()

	ln := blackholeServer(t)

	// No I/O timeouts and no retries: only redlease's own bound can unblock.
	rc := goredis.NewClient(&goredis.Options{
		Addr:         ln.Addr().String(),
		DialTimeout:  time.Second,
		ReadTimeout:  -1,
		WriteTimeout: -1,
		MaxRetries:   -1,
	})

	t.Cleanup(func() { _ = rc.Close() })

	const renew = 100 * time.Millisecond

	e, err := New(rc, Config{Name: "test", TTL: time.Second, RenewInterval: renew, AcquireInterval: renew, InstanceID: "host-a"})
	require.NoError(t, err)

	start := time.Now()

	_, err = e.renewOnce(t.Context(), renew)
	require.Error(t, err, "renewal against an unresponsive server must fail, not hang")
	assert.Less(t, time.Since(start), waitFor, "renewal must be bounded by the renew interval, not block indefinitely")
}

// A leader whose renewals persistently error must step down by the time the
// lock would have lapsed at Redis. The deadline is anchored at the moment the
// winning acquire request was sent — the conservative bound for the server-side
// expiry — not at some later client-side timestamp.
func TestHold_StepsDownWithinTTLWhenRenewalsFail(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	// Retries disabled so each failed renewal errors immediately and the timing
	// assertion below reflects the deadline logic, not client retry backoff.
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1})

	t.Cleanup(func() { _ = rc.Close() })

	const (
		ttl   = 200 * time.Millisecond
		renew = 50 * time.Millisecond
	)

	e, err := New(rc, Config{Name: "test", TTL: ttl, RenewInterval: renew, AcquireInterval: renew, InstanceID: "host-a"})
	require.NoError(t, err)

	acquiredAt := time.Now()

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// Kill Redis so every renewal errors.
	mr.Close()

	e.hold(t.Context(), acquiredAt)

	elapsed := time.Since(acquiredAt)
	assert.GreaterOrEqual(t, elapsed, ttl, "leader must tolerate transient errors until the TTL deadline")
	assert.Less(t, elapsed, ttl+300*time.Millisecond, "leader must step down at the TTL deadline, not a tick later")
}

// The step-down deadline is enforced by a timer inside hold's wait, not polled
// once per loop iteration. With renewals failing fast, attempts land at ~0,
// 250 and 500ms and the deadline (600ms) passes while hold is parked waiting
// for the 750ms tick; hold must return at the deadline, not at the tick after
// it. Before the deadline became timer-driven this returned at ~750ms.
func TestHold_StepsDownAtDeadlineNotAtNextTick(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	// Retries disabled so each failed renewal errors immediately.
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1})

	t.Cleanup(func() { _ = rc.Close() })

	const (
		ttl   = 600 * time.Millisecond
		renew = 250 * time.Millisecond
	)

	e, err := New(rc, Config{Name: "test", TTL: ttl, RenewInterval: renew, AcquireInterval: renew, InstanceID: "host-a"})
	require.NoError(t, err)

	acquiredAt := time.Now()

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// Kill Redis so every renewal errors.
	mr.Close()

	e.hold(t.Context(), acquiredAt)

	elapsed := time.Since(acquiredAt)
	assert.GreaterOrEqual(t, elapsed, ttl, "leader must tolerate transient errors until the TTL deadline")
	assert.Less(t, elapsed, ttl+renew/2, "step-down must fire at the deadline itself, not wait out the next tick")
}

// The first renewal fires immediately on entering hold, not one interval in:
// the winning acquire's response may have consumed a share of the TTL budget
// before hold started, so the deadline must be re-anchored by a completed
// round trip as soon as possible.
func TestHold_RenewsImmediatelyOnEntry(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	const (
		ttl   = 2 * time.Second
		renew = time.Second
	)

	e, err := New(rc, Config{Name: "test", TTL: ttl, RenewInterval: renew, AcquireInterval: renew, InstanceID: "host-a"})
	require.NoError(t, err)

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// Simulate an acquire whose response consumed most of the TTL budget: the
	// server-side clock has already run the lock down to a sliver.
	mr.SetTTL("test:leader", 100*time.Millisecond)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)

		e.hold(ctx, time.Now())
	}()

	// Well before the first renew tick would fire, the immediate renewal must
	// have refreshed the TTL back to the configured value.
	require.Eventually(t, func() bool { return mr.TTL("test:leader") > renew },
		renew/2, 10*time.Millisecond, "the first renewal must fire immediately, not one interval in")

	cancel()
	<-done
}

// hold must not start a renewal whose deadline has already passed: an attempt
// takes up to one renew interval, which a lapsed deadline has no room for.
// Entering hold past the deadline steps down at once, without extending a lock
// that may no longer be safely ours.
func TestHold_StepsDownImmediatelyWhenDeadlineAlreadyPassed(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	_, won, err := e.acquireLock(t.Context())
	require.NoError(t, err)
	require.True(t, won)

	// Any renewal would be observable as this marker TTL being overwritten.
	mr.SetTTL("test:leader", time.Hour)

	start := time.Now()

	e.hold(t.Context(), time.Now().Add(-testTTL))

	assert.Less(t, time.Since(start), testRenew, "hold must step down without waiting for a tick")
	assert.Equal(t, time.Hour, mr.TTL("test:leader"), "hold must not renew past the deadline")
}

func TestRun_StepsDownWhenLockStolen(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	ctx, cancel := context.WithCancel(t.Context())

	elected, steppedDown, _, exited := runElector(ctx, newElector(t, rc, "host-a"))

	t.Cleanup(func() {
		cancel()
		<-exited
	})

	require.True(t, fired(elected, waitFor), "instance was not elected")

	// Simulate a partition: another instance steals the lock. Step-down is driven
	// by the leader's next renew (testRenew, 50ms) seeing the value no longer its
	// own — not by the lock TTL — so the thief's TTL only needs to outlast that.
	require.NoError(t, mr.Set("test:leader", "thief"))
	mr.SetTTL("test:leader", testTTL)

	assert.True(t, fired(steppedDown, waitFor), "leader did not step down after losing the lock")

	// Ownership-checked release must not delete the thief's lock.
	val, err := mr.Get("test:leader")
	require.NoError(t, err)
	assert.Equal(t, "thief", val, "step-down must not delete a lock held by another instance")
}

// OnSteppedDown fires after a leadership term ends, never during it: while the
// leader still holds the lock it must not have fired, and it must fire once the
// term ends (here via shutdown). The token OnElected carries is covered by
// TestObserver_FiresOnElectedEachTermWithGreaterToken.
func TestObserver_SteppedDownFiresAfterLeadership(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	var (
		mu          sync.Mutex
		steppedDown bool
	)

	elected := newSignal()

	e, err := New(rc, Config{
		Name:            "test",
		TTL:             testTTL,
		RenewInterval:   testRenew,
		AcquireInterval: testAcquire,
		InstanceID:      "host-a",
		Observer: Observer{
			OnElected: func(int64) { elected.fire() },
			OnSteppedDown: func() {
				mu.Lock()
				defer mu.Unlock()

				steppedDown = true
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, _ Fencer) { <-leaderCtx.Done() })
	}()

	require.True(t, fired(elected, waitFor), "OnElected did not fire")

	mu.Lock()
	assert.False(t, steppedDown, "OnSteppedDown fired before leadership ended")
	mu.Unlock()

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	mu.Lock()
	defer mu.Unlock()

	assert.True(t, steppedDown, "OnSteppedDown must fire after leadership ends")
}
