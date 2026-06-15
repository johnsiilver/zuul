// Package deadline bounds contexts that arrive without one.
package deadline

import (
	"time"

	"github.com/gostdlib/base/context"
)

// Ensure returns ctx unchanged (nil CancelFunc) if it already carries a deadline,
// otherwise a child bounded by fallback.
func Ensure(ctx context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, nil
	}
	return context.WithTimeout(ctx, fallback)
}
