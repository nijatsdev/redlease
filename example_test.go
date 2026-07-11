package redlease_test

import (
	"context"
	"database/sql"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nijatsdev/redlease"
)

// This example runs a periodic job on exactly one instance at a time. Several
// instances call Run with the same Config.Name; only the elected leader executes
// the job. Each write the leader makes carries its leadership term's fencing
// token, so a stale leader that has not yet noticed it lost the lock cannot
// overwrite a newer leader's state.
func Example() {
	rc := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

	e, err := redlease.New(rc, redlease.Config{
		Name: "report-builder",
		TTL:  5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run blocks until ctx is cancelled. The callback runs only while this
	// instance is the leader; its context is cancelled the moment leadership
	// is lost. The Fencer carries this term's fencing token — use it for every
	// write to shared state.
	e.Run(ctx, func(leaderCtx context.Context, f redlease.Fencer) {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
				// Record progress under the fencing token. If a newer leader
				// has taken over, applied is false: this term is stale and
				// must stop working.
				applied, err := f.HSet(leaderCtx, "jobs:report", "status", "running")
				if err != nil {
					log.Printf("write failed: %v", err)
					continue
				}

				if !applied {
					log.Printf("fenced out by a newer leader; stopping")
					return
				}
			}
		}
	})
}

// This example fences a write to a resource other than Redis — here a SQL
// database. redlease enforces the fence for you only on Redis writes; for any
// other store it gives you the term's fencing token and you enforce it at the
// resource, atomically with your write. In SQL that means a conditional UPDATE
// that only applies when the row's stored fence is not newer than your token.
//
// The table is assumed to hold a fence column alongside the value:
//
//	CREATE TABLE state (id text PRIMARY KEY, value text, fence bigint NOT NULL DEFAULT 0);
//
// sql.Open needs a registered driver; a real program imports one, e.g.
//
//	import _ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
//
// The LeaderFunc here performs one write and returns, which ends the term (see
// [LeaderFunc]); a leader with ongoing work loops until leaderCtx is done, as
// in the package example above.
func Example_databaseFencing() {
	rc := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

	db, err := sql.Open("postgres", "postgres://localhost/app")
	if err != nil {
		log.Fatal(err)
	}

	defer func() { _ = db.Close() }()

	e, err := redlease.New(rc, redlease.Config{Name: "report-builder", TTL: 5 * time.Second})
	if err != nil {
		log.Print(err)
		return
	}

	ctx := context.Background()

	e.Run(ctx, func(leaderCtx context.Context, f redlease.Fencer) {
		// Take the term's token from the Fencer and enforce it yourself in SQL.
		// The UPDATE applies only when our token is at least the stored fence,
		// then advances it; a stale leader's lower token matches no row.
		const q = `UPDATE state
		             SET value = $1, fence = $2
		           WHERE id = $3 AND fence <= $2`

		res, err := db.ExecContext(leaderCtx, q, "running", f.Token(), "report")
		if err != nil {
			log.Printf("write failed: %v", err)
			return
		}

		if n, _ := res.RowsAffected(); n == 0 {
			// No row updated: a newer leader has advanced the fence past our
			// token. We are stale and must stop working.
			log.Printf("fenced out by a newer leader; stopping")
			return
		}
	})
}
