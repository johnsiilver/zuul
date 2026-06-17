package context

import "time"

// EnsureDeadline returns ctx unchanged (with a nil CancelFunc) when it already carries a
// deadline, otherwise a child context bounded by fallback. It lets a caller guarantee an
// upper bound on work without overriding a tighter deadline the caller already set. The
// nil CancelFunc in the pass-through case is intentional; callers must nil-check it (or
// guard the defer) before calling.
func EnsureDeadline(ctx Context, fallback time.Duration) (Context, CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, nil
	}
	return WithTimeout(ctx, fallback)
}
