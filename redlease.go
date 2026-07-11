// Package redlease implements lease-based leader election on Redis with fencing
// tokens.
//
// One instance holds a Redis lock with a TTL and runs the caller's work while it
// is leader; if it cannot renew, it steps down so another instance takes over.
// Unlike a plain Redis lock, every leadership term is assigned a strictly
// increasing fencing token. Writes routed through the Elector's Fence* helpers
// are rejected when they carry a token below the newest elected term or applied
// write — electing a term fences out its predecessors immediately — so a paused
// or clock-skewed leader that still believes it holds the lock cannot overwrite
// newer state.
//
// # When you need this
//
// Fencing matters only when a stale leader's write to shared state would be
// harmful — for example a value that must not regress, a sequence number, or a
// counter. If the leader does no writes, or writes self-healing last-writer-wins
// state, a plain lock is enough and you do not need this package.
//
// # Correctness boundary
//
// The fence token is generated and stored in Redis. On a single Redis instance
// this gives a strict, monotonic guarantee — provided the fence keys survive:
// crash-restart durability requires AOF appendfsync always (RDB and AOF
// everysec lose recent writes), and eviction must not touch the keys (run
// noeviction or a volatile-* policy; the fence keys carry no TTL). On a
// replicated deployment (Sentinel
// or Cluster), Redis replication is asynchronous: a primary can acknowledge the
// acquire — and the token increment — before it reaches a replica, and a failover
// to that replica can lose them. In that window the monotonicity the fence relies
// on can be violated. For strict correctness across failover, source the fencing
// token from a linearizable store (etcd, ZooKeeper) instead of Redis. This package
// is the right tool when you run Redis single-instance, or when a brief, rare token
// regression on failover is acceptable.
//
// On Redis Cluster, also wrap [Config.Name] in a hash tag (e.g. "{name}") so the
// lock, fence, and applied keys hash to the same slot; otherwise the acquire
// script fails with CROSSSLOT. The same constraint extends to fenced writes:
// each Fence* script touches the applied key and your target keys in one
// script, so every key written through the fence must carry the same hash tag
// as Name — on Cluster, fenced application state must live in the elector's
// slot. If that does not fit your data layout, enforce the fence at the
// resource yourself using the raw token (see [Elector.Token]).
package redlease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// DefaultTTL is the lock TTL used when [Config.TTL] is unset. Override via
// [Config].
const DefaultTTL = 5 * time.Second

// Default renew and acquire cadences derive from the resolved TTL rather than
// fixed durations, so they track whatever TTL the caller picks: a shorter TTL
// renews and polls proportionally faster. RenewInterval defaults to TTL/3 —
// roughly two renewal attempts before expiry, the standard safety margin — and
// AcquireInterval to TTL/2, so a follower polls at least twice per lock
// lifetime. Override either via [Config].
const (
	defaultRenewDivisor   = 3
	defaultAcquireDivisor = 2
)

// minTTL is the smallest lock TTL [New] accepts. Below this, a renewal round
// trip to Redis cannot reliably complete before the lock expires, so the leader
// would flap. Callers needing a shorter failover window should reduce
// RenewInterval against a TTL at or above this floor instead.
const minTTL = 100 * time.Millisecond

// minInterval is the smallest RenewInterval and AcquireInterval [New] accepts.
// An interval is also its round trip's timeout; below this floor every call
// times out before Redis can answer and the elector flaps.
const minInterval = time.Millisecond

// Redis is the subset of the go-redis client this package needs: the script
// runner used by all lock and fence operations. *goredis.Client and
// *goredis.ClusterClient both satisfy it, as does any compatible wrapper.
//
// Configure the client with sane I/O timeouts: the elector bounds every round
// trip itself, but a call abandoned past its bound stays blocked inside a
// timeout-less client, pinning a pooled connection — during a long server hang
// those accumulate and can starve a shared client.
type Redis interface {
	goredis.Scripter
}

// Config configures an [Elector]. Only Name is required (the client is passed
// separately to [New]); zero-valued timing fields fall back to the Default*
// constants.
type Config struct {
	// Name identifies the lock. All instances contending for the same leadership
	// must use the same Name; different Names are independent locks.
	//
	// The lock, fence, and applied keys all derive from Name, and a single Lua
	// script touches more than one of them. On Redis Cluster they must therefore
	// hash to the same slot: wrap Name in a hash tag, e.g. "{report-builder}",
	// so all derived keys share it. Without one they scatter across slots and the
	// acquire script fails with CROSSSLOT. Keys written through the Fence*
	// helpers must carry the same hash tag too; see the package documentation.
	Name string

	// TTL is how long the lock lives without renewal. A leader that cannot renew
	// within TTL loses leadership. Shorter TTL means faster failover but less
	// tolerance for pauses. Must be at least 100ms so a renewal can complete
	// before expiry. Defaults to DefaultTTL.
	//
	// Redis applies the TTL at millisecond granularity (PX), so a fractional
	// remainder is truncated.
	TTL time.Duration

	// RenewInterval is how often the leader renews the lock. Must be at most
	// TTL/2, so at least two renewal attempts fit before expiry, and at least
	// 1ms (it doubles as each renewal's timeout). Note that at exactly TTL/2
	// the retry after a dropped renewal lands right at expiry; keep it strictly
	// below TTL/2 (the TTL/3 default) for real retry headroom.
	RenewInterval time.Duration

	// AcquireInterval is how often a follower retries acquiring the lock. Must be
	// less than TTL, so a follower polls at least once per lock lifetime, and at
	// least 1ms, like RenewInterval. Defaults to TTL/2.
	//
	// The interval governs the ordinary case of losing the race to another
	// instance. When the acquire round trip errors instead (Redis unreachable),
	// the retry delay doubles per consecutive error, capped at TTL, and resets
	// once Redis answers again.
	AcquireInterval time.Duration

	// InstanceID uniquely identifies this instance; it is the lock's value and
	// guards renewal and release so an instance never affects another's lock. If
	// empty, a hostname + random suffix is generated.
	//
	// The ID must be unique among live processes, not merely stable: a lock
	// found holding this instance's own ID is treated as its leftover and taken
	// over, so two live replicas sharing an ID seize the lock from each other
	// and both run as leader — permanently and silently. Derive a fixed ID from
	// something unique per replica (pod or host name), never a value shared
	// across replicas like the service name.
	InstanceID string

	// Observer receives lifecycle events. All fields are optional; the library
	// emits no output of its own, so callers wire these to their own logger,
	// metrics, or tracing as they see fit.
	Observer Observer
}

// Observer is a set of optional callbacks invoked on leadership transitions. A
// nil field is simply not called. Callbacks run on the Run goroutine and must
// not block — a slow OnElected eats TTL budget before the first renewal and
// can cost the term. A panicking callback is not recovered (see [LeaderFunc]);
// OnElected in particular fires while the lock is held, so its panic leaves
// the lock to expire at the TTL.
//
// Role transitions are the primary events, mirroring the design of
// k8s.io/client-go/tools/leaderelection: transient Redis errors during
// acquire, renewal, and release are handled internally — retried, backed off,
// or stepped down from — and the consequence the caller cares about, losing
// leadership, surfaces through OnSteppedDown. OnError additionally exposes
// those handled errors for logging and metrics; it never requires action.
type Observer struct {
	// OnElected fires when this instance wins a leadership term, with that
	// term's fencing token. It fires before the LeaderFunc is invoked.
	OnElected func(token int64)

	// OnSteppedDown fires when this instance loses or relinquishes leadership,
	// after the LeaderFunc has returned.
	OnSteppedDown func()

	// OnFollower fires when this instance is, for now, a follower: an acquire
	// attempt found the lock held by another instance. It fires once per
	// transition into the follower role — on the first lost attempt, and again
	// only after an intervening leadership term — not on every retry. A consumer
	// can use it to learn its initial role at startup without waiting to win.
	OnFollower func()

	// OnError fires when a Redis round trip fails during acquire, renewal, or
	// release. The elector has already handled the failure — retried, backed
	// off, or stepped down — so the callback exists purely for visibility: wire
	// it to your logger or metrics to see trouble before it costs leadership.
	// It is not called for the context cancellation that ends Run.
	//
	// Deterministic failures (a missing Cluster hash tag surfacing as
	// CROSSSLOT, auth errors) are retried forever at the backoff cadence, and
	// OnError is their only signal — wire it up before first deploying.
	OnError func(err error)
}

// Elector runs leader election for a single lock and mints fencing tokens.
type Elector struct {
	client Redis

	lockKey    string
	fenceKey   string
	appliedKey string
	id         string

	ttl     time.Duration
	renew   time.Duration
	acquire time.Duration

	obs Observer

	// token holds the current leadership term's fencing token, or 0 when this
	// instance is not the leader. Real tokens start at 1, so 0 is a safe
	// "not leader" sentinel. Read via Token / IsLeader from any goroutine.
	token atomic.Int64

	// resign points at the current term's cancel function while a term is
	// running, nil otherwise. Resign loads it to end the term from any
	// goroutine; a stale load can only cancel an already-finished term's
	// context, which is harmless.
	resign atomic.Pointer[context.CancelFunc]

	// running guards against concurrent Run calls on the same Elector, which
	// would corrupt shared leadership state (token, resign, the lock itself).
	running atomic.Bool

	// evalScripts caches compiled FenceEval scripts keyed by Lua body, guarded
	// by evalMu. Initialized in New; entries are never evicted, and the cache
	// is capped at maxEvalScripts so interpolated bodies fail loudly instead
	// of leaking.
	evalMu      sync.RWMutex
	evalScripts map[string]*goredis.Script
}

// New returns an Elector from cfg. It returns an error if required fields are
// missing or timing parameters are inconsistent: TTL below 100ms, RenewInterval
// greater than TTL/2, AcquireInterval not less than TTL, or either interval
// below 1ms.
func New(client Redis, cfg Config) (*Elector, error) {
	if client == nil {
		return nil, errors.New("redlease: nil client")
	}

	if cfg.Name == "" {
		return nil, errors.New("redlease: empty Name")
	}

	// A negative duration is a configuration mistake, not "unset"; only zero
	// means "use the default".
	if cfg.TTL < 0 || cfg.RenewInterval < 0 || cfg.AcquireInterval < 0 {
		return nil, errors.New("redlease: negative durations are not valid")
	}

	// Redis grants the lease in whole milliseconds (PX); truncate so the
	// client-side step-down deadline never budgets against precision the
	// server was not told about.
	ttl := orDuration(cfg.TTL, DefaultTTL).Truncate(time.Millisecond)

	if ttl < minTTL {
		return nil, fmt.Errorf("redlease: TTL must be at least %s", minTTL)
	}

	renew := orDuration(cfg.RenewInterval, ttl/defaultRenewDivisor)
	acquire := orDuration(cfg.AcquireInterval, ttl/defaultAcquireDivisor)

	// Renew must leave room for a retry: at least two attempts should fit before
	// the lock expires. TTL/2 is the permissive bound; the TTL/3 default leaves
	// real headroom for the retry to complete before expiry.
	if renew > ttl/2 {
		return nil, errors.New("redlease: RenewInterval must be at most TTL/2")
	}

	if acquire >= ttl {
		return nil, errors.New("redlease: AcquireInterval must be less than TTL")
	}

	if renew < minInterval {
		return nil, fmt.Errorf("redlease: RenewInterval must be at least %s", minInterval)
	}

	if acquire < minInterval {
		return nil, fmt.Errorf("redlease: AcquireInterval must be at least %s", minInterval)
	}

	id := cfg.InstanceID
	if id == "" {
		id = instanceID()
	}

	return &Elector{
		client:      client,
		lockKey:     cfg.Name + ":leader",
		fenceKey:    cfg.Name + ":fence",
		appliedKey:  cfg.Name + ":fence:applied",
		id:          id,
		ttl:         ttl,
		renew:       renew,
		acquire:     acquire,
		obs:         cfg.Observer,
		evalScripts: make(map[string]*goredis.Script),
	}, nil
}

// InstanceID returns this elector's instance identity.
func (e *Elector) InstanceID() string { return e.id }

// Token returns the current leadership term's fencing token and true while this
// instance is the leader, or 0 and false otherwise. It is safe to call from any
// goroutine, so code outside the LeaderFunc can perform fenced writes:
//
//	if token, ok := e.Token(); ok {
//	    e.FenceHSet(ctx, token, "state", "key", "value")
//	}
//
// The returned token is a snapshot; leadership can change immediately after.
// That is safe because the fence is enforced at write time — a stale token is
// rejected by the Fence* helpers — so unlike [Elector.IsLeader] there is no
// time-of-check-to-time-of-use hazard in acting on it.
func (e *Elector) Token() (token int64, ok bool) {
	t := e.token.Load()
	return t, t != 0
}

// Fencer returns a [Fencer] bound to the current leadership term and true while
// this instance is the leader, or an inert Fencer and false otherwise — inert
// meaning it carries token 0, the "not leader" sentinel every fenced write
// rejects, so even a caller that ignores ok cannot slip a write through. It is
// the token-driven-style counterpart to the [Fencer] a [LeaderFunc] receives:
// code outside the callback can take a Fencer and pass it down to its writers.
//
// Like [Elector.Token], the returned Fencer is a snapshot; leadership can change
// immediately after. That is safe because the fence is enforced at write time —
// a stale token is rejected by the Fencer's methods.
func (e *Elector) Fencer() (f Fencer, ok bool) {
	t := e.token.Load()
	return Fencer{e: e, token: t}, t != 0
}

// Resign voluntarily ends the current leadership term, if any: the LeaderFunc's
// context is cancelled, the lock is released, and Run re-contends after
// AcquireInterval — so this instance may well win again unless another takes
// the lock first. It is a no-op when this instance is not leading, and safe to
// call from any goroutine. It is the step-down lever for the token-driven style
// (Run with a nil LeaderFunc), which otherwise could stop leading only by
// cancelling Run's context entirely.
func (e *Elector) Resign() {
	if c := e.resign.Load(); c != nil {
		(*c)()
	}
}

// IsLeader reports whether this instance currently holds leadership. It is
// advisory only: leadership can be lost the instant after it returns, so never
// gate a correctness-sensitive write on it. For writes, carry the token from
// [Elector.Token] through a Fence* helper, which rejects a stale token at write
// time.
func (e *Elector) IsLeader() bool {
	return e.token.Load() != 0
}

// LeaderFunc is the work run while an instance is leader. It receives a context
// cancelled when leadership is lost or Run's context is cancelled, and a [Fencer]
// bound to this leadership term — use it for every fenced write, or pass it down
// to the code that writes shared state. LeaderFunc must return promptly once its
// context is cancelled; Run does not release the lock or re-contend until it does.
//
// Returning also ends the term: holding the lock with no work running would only
// block other instances, so Run releases it and re-contends. A LeaderFunc that
// wants to stay leader must block until its context is cancelled.
//
// A panic in a LeaderFunc is not recovered: it crashes the process like any
// other goroutine panic, and the lock is left to expire at its TTL rather than
// being released, so failover waits out the TTL. Recover inside your LeaderFunc
// if you want a panicking term to step down gracefully instead.
type LeaderFunc func(ctx context.Context, f Fencer)

// Run contends for leadership until ctx is cancelled. Each time this instance
// wins, it invokes fn (a [LeaderFunc]) with a [Fencer] for the term, then steps
// down and re-contends when the term ends — leadership lost, fn returned,
// [Elector.Resign] called, or ctx cancelled. Run blocks until ctx is cancelled
// and fn (if running) has returned; on the final shutdown step-down the
// OnSteppedDown observer still fires.
//
// fn may be nil. Then Run just keeps this instance elected — acquiring, renewing,
// and releasing the lock — while the caller does its leader work from another
// goroutine, gated on [Elector.Token]. This is the token-driven style; the
// callback style and this one are interchangeable, pick whichever fits.
//
// Run must not be called more than once concurrently on the same Elector; the
// two calls would corrupt shared leadership state. A concurrent call panics.
// Use one Elector per Run. Sequential calls (a new Run after a previous one
// returned) are fine.
func (e *Elector) Run(ctx context.Context, fn LeaderFunc) {
	if !e.running.CompareAndSwap(false, true) {
		panic("redlease: Run called concurrently on the same Elector; use one Elector per Run")
	}
	defer e.running.Store(false)

	// wasFollower tracks whether OnFollower has already fired for the current
	// follower stretch, so it fires once per transition into the role rather than
	// on every losing retry. A leadership term resets it.
	wasFollower := false

	// backoff is the delay applied to the next attempt after a failed acquire
	// round trip (a Redis error, not a lost race). It starts at the acquire
	// interval and doubles per consecutive error, capped at the TTL, so an
	// unreachable Redis is not hammered at full cadence while recovery is still
	// noticed within about one lock lifetime. Any round trip that reaches Redis
	// — won or follower — resets it, and losing the race never backs off:
	// polling slower than the acquire interval would slow failover.
	backoff := e.acquire

	for ctx.Err() == nil {
		// Captured before the acquire request goes out: the lock's TTL starts
		// counting at the server-side SET, so this is the conservative anchor for
		// the renewal deadline in hold.
		acquiredAt := time.Now()

		token, won, err := e.acquireLock(ctx)

		delay := e.acquire

		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}

			e.reportError(err)

			delay = backoff
			backoff = min(backoff*2, e.ttl)
		case won:
			wasFollower = false
			backoff = e.acquire

			e.lead(ctx, token, acquiredAt, fn)
		default:
			backoff = e.acquire

			// Lock held by another instance: we are a follower.
			if !wasFollower {
				wasFollower = true

				if e.obs.OnFollower != nil {
					e.obs.OnFollower()
				}
			}
		}

		if !sleepOrDone(ctx, jitter(delay)) {
			return
		}
	}
}

// lead holds the lock for one leadership term, then releases it. The term is
// scoped by a derived context that ends when leadership is lost, ctx is
// cancelled, fn returns, or Resign is called. When fn is non-nil it runs in its
// own goroutine on that context; lead waits for it to return before releasing,
// so a successor never starts before the previous holder has fully stopped.
// When fn is nil, lead simply holds leadership until the term ends — the caller
// drives its work elsewhere via Token / FenceHSet. acquiredAt is when the
// winning acquire request was sent; it seeds the renewal deadline in hold.
func (e *Elector) lead(ctx context.Context, token int64, acquiredAt time.Time, fn LeaderFunc) {
	termCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	e.resign.Store(&cancel)
	e.token.Store(token)

	if e.obs.OnElected != nil {
		e.obs.OnElected(token)
	}

	if fn == nil {
		e.hold(termCtx, acquiredAt)
	} else {
		done := make(chan struct{})

		go func() {
			defer close(done)
			// fn returning ends the term: holding the lock with no work running
			// would only block other instances.
			defer cancel()

			fn(termCtx, Fencer{e: e, token: token})
		}()

		e.hold(termCtx, acquiredAt)
		cancel()
		<-done
	}

	e.resign.Store(nil)

	// Clear the token before releasing so a concurrent Token/IsLeader caller
	// stops seeing this instance as leader as soon as the work has stopped.
	e.token.Store(0)
	e.release()

	if e.obs.OnSteppedDown != nil {
		e.obs.OnSteppedDown()
	}
}

// reportError forwards a Redis error the elector has already handled to the
// OnError observer, if set. Like every Observer callback it runs on the Run
// goroutine.
func (e *Elector) reportError(err error) {
	if e.obs.OnError != nil {
		e.obs.OnError(err)
	}
}

// orDuration resolves an optional duration: zero means "unset, use the
// fallback". Negative values never reach here; New rejects them.
func orDuration(v, fallback time.Duration) time.Duration {
	if v == 0 {
		return fallback
	}

	return v
}

// instanceID returns a process-unique identity (hostname + random suffix) so two
// instances never share a value and cannot extend or release each other's lock.
func instanceID() string {
	host, _ := os.Hostname()

	// crypto/rand.Read never returns an error as of Go 1.24 (this module's
	// floor); it fills b entirely or the program does not run.
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	suffix := hex.EncodeToString(b)
	if host != "" {
		return host + "-" + suffix
	}

	return suffix
}

// jitter returns d shortened by a random amount of up to 10%, so followers that
// started in lockstep spread their acquire attempts over a window instead of
// hitting Redis in the same instant. The perturbation is only ever downward,
// preserving the validated AcquireInterval < TTL bound (a follower still polls
// at least once per lock lifetime).
func jitter(d time.Duration) time.Duration {
	return d - time.Duration(mathrand.Int64N(int64(d)/10+1))
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
