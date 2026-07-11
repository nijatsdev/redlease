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
func runElector(ctx context.Context, e *Elector) (elected, steppedDown *signal, token *int64, exited chan struct{}) {
	elected, steppedDown = newSignal(), newSignal()
	token = new(int64)
	exited = make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
			*token = f.Token()

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
	defer cancel()

	type result struct {
		token   int64
		applied bool
		err     error
	}

	got := make(chan result, 1)

	go e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
		applied, err := f.HSet(leaderCtx, "state", "key", "via-callback")
		got <- result{token: f.Token(), applied: applied, err: err}

		<-leaderCtx.Done()
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

	// Defaults fill in for zero timing fields.
	e, err := New(rc, Config{Name: "x"})
	require.NoError(t, err)
	assert.NotEmpty(t, e.InstanceID())
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
	assert.Equal(t, int64(1), *token, "first term must get token 1")

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

// Returning from the LeaderFunc ends the term: the lock is released and Run
// re-contends, rather than holding leadership idle with no work running. The
// next win is a new term with a strictly greater token.
func TestRun_LeaderFuncReturnEndsTerm(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)
	e := newElector(t, rc, "host-a")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var calls atomic.Int64

	tokens := make(chan int64, 2)

	go e.Run(ctx, func(leaderCtx context.Context, f Fencer) {
		tokens <- f.Token()

		if calls.Add(1) == 1 {
			return // first term ends by returning
		}

		<-leaderCtx.Done()
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
	defer cancelB()

	electedB, _, tokenB, _ := runElector(ctxB, newElector(t, rcB, "host-b"))

	assert.False(t, fired(electedB, 300*time.Millisecond), "B led while A still held the lock")

	cancelA()

	select {
	case <-exitedA:
	case <-time.After(waitFor):
		t.Fatal("A did not exit")
	}

	require.True(t, fired(electedB, waitFor), "B did not take over after A exited")
	assert.Greater(t, *tokenB, *tokenA, "successor must get a strictly greater fencing token")
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
	defer cancel()

	go e.Run(ctx, nil)

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
	defer cancel()

	go e.Run(ctx, nil)

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

	_, err = e.renewOnce(t.Context())
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
	assert.Less(t, elapsed, ttl+renew+300*time.Millisecond, "leader must step down promptly once the TTL deadline passes")
}

func TestRun_StepsDownWhenLockStolen(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	elected, steppedDown, _, _ := runElector(ctx, newElector(t, rc, "host-a"))
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
