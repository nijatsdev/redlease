# redlease

[![CI](https://github.com/nijatsdev/redlease/actions/workflows/ci.yml/badge.svg)](https://github.com/nijatsdev/redlease/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/nijatsdev/redlease.svg)](https://pkg.go.dev/github.com/nijatsdev/redlease)
[![Go Report Card](https://goreportcard.com/badge/github.com/nijatsdev/redlease)](https://goreportcard.com/report/github.com/nijatsdev/redlease)

Lease-based leader election on Redis, **with fencing tokens**.

```bash
go get github.com/nijatsdev/redlease
```

> Pre-1.0: the API may change between `v0.x` releases.

redlease elects **one long-lived leader** among instances and keeps it elected — for a singleton background job, a scheduler, a cron-like task, or the single writer in a one-writer-many-readers system. It manages the whole leadership lifecycle: acquire, renew, step down, release, and fail over.

It is **not** a general-purpose mutex. The litmus test:

> Is the lock held for the duration of a **role** or the duration of an **operation**?
> Role (be the leader, own the schedule, be the one writer) → **redlease**.
> Operation (guard this critical section, update this counter safely) → a distributed mutex.

One instance holds a Redis lock with a TTL and runs your work while it is leader. If it cannot renew the lock, it steps down so another instance takes over. What redlease adds on top:

**Every leadership term is assigned a strictly increasing fencing token, and writes routed through the `Fence*` helpers reject any token older than the latest applied.** A paused, GC-stalled, or clock-skewed leader that still believes it holds the lock cannot overwrite newer state — its writes are refused at Redis.

---

## Why fencing

Lease-based leader election has an unavoidable window. The lock has a TTL; if the leader pauses (GC, CPU starvation) or is partitioned, the lock can expire and a second instance can be elected **while the first still thinks it is leader**. For a few seconds, two leaders exist. This is inherent to *any* lease-based lock — shortening the TTL only shrinks the window, it never closes it.

Fencing makes that window safe. Each term gets a token; the protected resource only accepts writes whose token is at least the highest it has already seen. The stale leader's writes carry an old token and are rejected. This is the mitigation Martin Kleppmann describes in [*How to do distributed locking*](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html).

### Do you actually need it?

Fencing matters **only** when a stale leader's write to shared state would be harmful:

| Your leader… | Need fencing? |
| --- | --- |
| writes a value that must not regress (sequence number, counter, monotonic state) | **Yes** |
| does no writes (runs a cron, sends notifications) | No — at most you want idempotency/dedup |
| writes self-healing last-writer-wins state that the next correct write repairs | No — a plain lock is enough |

If you are in the "No" rows, a simpler lock will do. redlease is for the first row.

---

## Usage

```go
rc := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

e, err := redlease.New(rc, redlease.Config{Name: "report-builder", TTL: 5 * time.Second})
if err != nil {
    log.Fatal(err)
}

// Run blocks until ctx is cancelled. The callback runs only while leader; its
// context is cancelled the instant leadership is lost. The Fencer carries this
// term's fencing token — use it for every fenced write, or pass it down to the
// code that writes shared state.
e.Run(ctx, func(leaderCtx context.Context, f redlease.Fencer) {
    applied, err := f.HSet(leaderCtx, "jobs:report", "status", "running")
    if err != nil {
        // Redis error.
    }
    if !applied {
        // A newer leader has taken over; this term is stale. Stop working.
        return
    }
})
```

All instances that should contend for the same leadership must share the same `Config.Name`.

### Fenced writes

A `Fencer` binds the term's token to the elector that minted it, so you carry one value instead of threading a token and a client separately. Each fenced write checks the token and performs the write **atomically in a single Lua script**, so there is no window in which the token could go stale between the check and the write. All methods share one high-water mark, so a token advanced by any of them fences every later, lower-token write:

```go
f.HSet(ctx, hashKey, field, value) // fenced HSET
f.Set(ctx, key, value)             // fenced SET

// Escape hatch for any other Redis write (ZADD, XADD, multi-key, ...).
// KEYS[1]/ARGV[1] are reserved for the fence; address yours from index 2.
f.Eval(ctx,
    "redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])",
    []string{"board"}, "100", "alice")
```

All three return `(applied bool, err error)`: `applied == false` means your token is stale — a newer leader has taken over — and you should stop writing. `f.Token()` returns the raw token for fencing a resource that isn't Redis (see below).

The same writes are available on the elector itself — `e.FenceHSet(ctx, token, ...)`, `e.FenceSet`, `e.FenceEval` — taking the token explicitly. Use those in the token-driven style, where you hold a token from `e.Token()` rather than a `Fencer` from the callback.

### Observability

redlease emits **no logs of its own** — a library should not impose a log format, level, or destination on its caller. Instead it exposes leadership transitions through an optional `Observer`; wire them to your own logger, metrics, or tracing. Every field is optional and a nil one is simply not called.

```go
e, _ := redlease.New(rc, redlease.Config{
    Name: "report-builder",
    Observer: redlease.Observer{
        OnElected:     func(token int64) { slog.Info("leader elected", "fence", token) },
        OnSteppedDown: func()            { slog.Info("leader stepped down") },
        OnFollower:    func()            { slog.Info("running as follower") },
    },
})
```

`OnFollower` fires when an acquire attempt finds the lock held by another instance — once per transition into the follower role, not on every retry. It lets a follower learn its initial role at startup without waiting to win.

Only role transitions are reported. Transient Redis errors during acquire or renewal are handled internally; the consequence the caller cares about — losing leadership — surfaces through `OnSteppedDown`. Monitor Redis health through your Redis client, not through the `Observer`. Callbacks run on the `Run` goroutine and must not block.

### Checking leadership outside the callback

The `Fencer` reaches your `LeaderFunc` directly, but sometimes another goroutine — an HTTP handler, say — needs to act as the leader too. `Fencer()`, `Token()`, and `IsLeader()` expose the current state from any goroutine:

```go
if f, ok := e.Fencer(); ok {
    // We are the leader. The fenced write is safe even if leadership changes
    // right now: a stale token is rejected at write time.
    f.HSet(ctx, "state", "key", "value")
}
```

Prefer `Token()` for anything that writes. `IsLeader()` exists for display/metrics, but it is **advisory** — leadership can be lost the instant after it returns, so never gate a correctness-sensitive write on `if e.IsLeader() { write() }`. That is the split-brain race fencing exists to prevent; carry the token through a `Fence*` helper instead.

If all your leader work happens this way — outside the callback — pass `nil` for the `LeaderFunc`. `Run` then just keeps the instance elected (acquire, renew, release) while your other goroutines act via `Token()`:

```go
go e.Run(ctx, nil) // hold leadership; do the work elsewhere via e.Token()
```

The callback style and this token-driven style are interchangeable — pick whichever fits your app.

---

## How it works

- **Acquire** — a single Lua script does `SET name:leader <id> NX PX <ttl-ms>` and, on success, `INCR name:fence` to mint the token. Doing both atomically guarantees every term's token is strictly greater than any prior term's. The TTL is set in milliseconds so sub-second values are honored exactly.
- **Hold** — the leader renews the lock on `RenewInterval` via an ownership-checked script (it only extends a lock whose value is still its own id). A renewal that returns 0 means the lock was lost; a transient Redis error is tolerated until `TTL` would have lapsed, then the leader steps down.
- **Release** — on graceful step-down the leader deletes the lock (ownership-checked, so it never deletes a successor's lock), letting the next instance take over without waiting for the TTL.
- **Fence** — each fenced write runs a Lua script that stores the highest applied token in `name:fence:applied` and rejects any write carrying a lower token, performing the write only when the token is current.

## Fencing writes that don't go to Redis

Fencing is **not a Redis concept** — it applies to any shared resource a stale leader could corrupt (a Postgres row, an S3 object). The catch is that the fence must be enforced **at the resource itself**, atomically with the write, because that is the only place the check and the write can happen together.

This package enforces the fence for **Redis** writes, because Redis is the resource it can reach into (via Lua). If your leader writes elsewhere, redlease still gives you the universal half — the monotonic token — but you must enforce it at *your* resource. For example, in Postgres:

```sql
UPDATE state SET value = $1, fence = $2 WHERE key = $3 AND fence <= $2;
-- rows affected == 0  ->  your token was stale; you were fenced out
```

So: use the `Fence*` helpers when you write to Redis; use the token with a conditional write (a `WHERE fence <= token`, a compare-and-swap, an `If-Match` precondition) when you write anywhere else.

---

## Correctness boundary

**Read this before using it for anything that matters.**

The fencing token is generated and stored **in Redis**. On a **single Redis instance**, this gives a strict, monotonic guarantee: tokens never go backward, and the fence is sound.

On a **replicated Redis** deployment (Sentinel or Cluster), Redis replication is **asynchronous**. A primary can acknowledge the acquire — and the token `INCR` — before it has propagated to a replica, and a failover to that replica can lose it. In that window the monotonicity the fence depends on can be violated, and two leaders could in principle obtain non-ordered tokens. This is the same limitation that affects *every* Redis-based lock, fencing or not.

On **Redis Cluster** there is also a separate, non-negotiable requirement: the lock, fence, and applied keys all derive from `Config.Name`, and a single Lua script touches more than one of them, so they must hash to the **same slot**. Wrap `Name` in a hash tag — e.g. `"{report-builder}"` — so every derived key shares it. Without one they scatter across slots and the acquire script fails with `CROSSSLOT`.

So redlease is the right tool when:

- you run Redis **single-instance**, **or**
- a brief, rare token regression on Redis failover is acceptable for your workload.

If you need a fencing guarantee that survives failover, source the token from a **linearizable** store and apply it against your resource. redlease deliberately does not pretend Redis can provide that.

---

## License

MIT
