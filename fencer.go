package redlease

import "context"

// Fencer binds a leadership term's fencing token to the Elector that minted it,
// so a single value carries everything a fenced write needs. It is what a
// [LeaderFunc] receives, and what you pass down into the code that writes shared
// state.
//
// A Fencer is a small value; copy it freely. Its methods are the bound
// equivalents of the Elector's Fence* helpers: each applies its write only when
// the bound token is at least the fence high-water mark, and returns
// applied == false when the token is stale (a newer term has been elected or
// has written), so the caller can stop. A Fencer is valid only for its term:
// its writes are fenced out once a successor is elected. Between a voluntary
// step-down and the next election, writes under the old token may still apply
// (there is no newer state to protect); treat the term context's cancellation,
// not a failing write, as the signal to stop working.
type Fencer struct {
	// e supplies the Redis client and fenced-write scripts. token is pinned to
	// the term this Fencer was minted for and is not read from e, so an outdated
	// Fencer keeps its old token and its writes are fenced out.
	e     *Elector
	token int64
}

// NewFencer binds an explicit token to e, for callers that already hold a token
// — for example one persisted from a prior leadership term, or supplied in a
// test. Most code should instead take the Fencer a [LeaderFunc] receives or call
// [Elector.Fencer]. The token is not validated against the elector's current
// term (the fence is enforced at write time), but it must originate from this
// package's electors or stay within their magnitude: the fence comparison is
// exact only up to 2^53, and stamping an arbitrarily large token would
// permanently fence out every legitimate writer.
//
// e must be non-nil — the Fencer's methods call through it. Likewise a
// hand-declared zero Fencer{} panics on use; obtain Fencers from the elector.
func NewFencer(e *Elector, token int64) Fencer {
	return Fencer{e: e, token: token}
}

// Token returns the fencing token this Fencer carries — the value the elector
// assigned to this leadership term.
func (f Fencer) Token() int64 { return f.token }

// HSet performs a fenced HSET of field=value into hashKey. See
// [Elector.FenceHSet] for the full semantics.
func (f Fencer) HSet(ctx context.Context, hashKey, field, value string) (applied bool, err error) {
	return f.e.FenceHSet(ctx, f.token, hashKey, field, value)
}

// Set performs a fenced SET of key=value. See [Elector.FenceSet].
func (f Fencer) Set(ctx context.Context, key, value string) (applied bool, err error) {
	return f.e.FenceSet(ctx, f.token, key, value)
}

// Eval fences an arbitrary Lua write. See [Elector.FenceEval].
func (f Fencer) Eval(ctx context.Context, body string, writeKeys []string, args ...any) (applied bool, err error) {
	return f.e.FenceEval(ctx, f.token, body, writeKeys, args...)
}
