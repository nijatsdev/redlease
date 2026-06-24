package redlease

import (
	"context"
	"sync"
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

		e.Run(ctx, func(leaderCtx context.Context, tok int64) {
			*token = tok

			elected.fire()
			<-leaderCtx.Done()
			steppedDown.fire()
		})
	}()

	return elected, steppedDown, token, exited
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	_, err := New(nil, Config{Name: "x"})
	require.Error(t, err)

	_, err = New(rc, Config{Name: ""})
	require.Error(t, err)

	// RenewInterval must be < TTL.
	_, err = New(rc, Config{Name: "x", TTL: time.Second, RenewInterval: time.Second})
	require.Error(t, err)

	// Defaults fill in for zero timing fields.
	e, err := New(rc, Config{Name: "x"})
	require.NoError(t, err)
	assert.NotEmpty(t, e.InstanceID())
}

func TestTTLMillis_PreservesSubSecondPrecision(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	// A sub-second TTL must not collapse to 0 or round to a whole second.
	e, err := New(rc, Config{Name: "x", TTL: 1500 * time.Millisecond, RenewInterval: 200 * time.Millisecond})
	require.NoError(t, err)
	assert.Equal(t, "1500", e.ttlMillis(), "TTL must be expressed in whole milliseconds")

	// A pathologically small TTL still yields at least 1ms, never 0.
	tiny := &Elector{ttl: 1 * time.Nanosecond}
	assert.Equal(t, "1", tiny.ttlMillis())
}

func TestAcquire_SetsMillisecondLockTTL(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	e, err := New(rc, Config{Name: "test", TTL: 1500 * time.Millisecond, RenewInterval: 200 * time.Millisecond, InstanceID: "host-a"})
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

		e.Run(ctx, func(leaderCtx context.Context, _ int64) {
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

func TestRun_StepsDownWhenLockStolen(t *testing.T) {
	t.Parallel()

	mr, rc := newRedis(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	elected, steppedDown, _, _ := runElector(ctx, newElector(t, rc, "host-a"))
	require.True(t, fired(elected, waitFor), "instance was not elected")

	// Simulate a partition: another instance steals the lock.
	require.NoError(t, mr.Set("test:leader", "thief"))
	mr.SetTTL("test:leader", testTTL)

	assert.True(t, fired(steppedDown, waitFor), "leader did not step down after losing the lock")

	// Ownership-checked release must not delete the thief's lock.
	val, err := mr.Get("test:leader")
	require.NoError(t, err)
	assert.Equal(t, "thief", val, "step-down must not delete a lock held by another instance")
}

func TestObserver_FiresElectedAndSteppedDown(t *testing.T) {
	t.Parallel()

	_, rc := newRedis(t)

	var (
		mu          sync.Mutex
		electedTok  int64
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
			OnElected: func(token int64) {
				mu.Lock()
				electedTok = token
				mu.Unlock()
				elected.fire()
			},
			OnSteppedDown: func() {
				mu.Lock()
				steppedDown = true
				mu.Unlock()
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	exited := make(chan struct{})

	go func() {
		defer close(exited)

		e.Run(ctx, func(leaderCtx context.Context, _ int64) { <-leaderCtx.Done() })
	}()

	require.True(t, fired(elected, waitFor), "OnElected did not fire")

	mu.Lock()
	assert.Equal(t, int64(1), electedTok, "OnElected must receive the term's fencing token")
	assert.False(t, steppedDown, "OnSteppedDown fired before leadership ended")
	mu.Unlock()

	cancel()

	select {
	case <-exited:
	case <-time.After(waitFor):
		t.Fatal("Run did not exit after context cancel")
	}

	mu.Lock()
	assert.True(t, steppedDown, "OnSteppedDown must fire after leadership ends")
	mu.Unlock()
}
