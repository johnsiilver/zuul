package context

import "github.com/gostdlib/base/context"

// Everything below is a re-export of the standard library context surface (via
// base/context) so callers need only import this package, never the stdlib context.
// These are var aliases so they introduce no extra stack frame.
var (
	// WithCancel returns a copy of parent with a new Done channel, closed when the returned
	// cancel function is called or the parent is canceled.
	WithCancel = context.WithCancel
	// WithCancelCause behaves like WithCancel but its cancel records a cause.
	WithCancelCause = context.WithCancelCause
	// WithDeadline returns a copy of parent with the deadline adjusted to be no later than d.
	WithDeadline = context.WithDeadline
	// WithDeadlineCause behaves like WithDeadline but also sets a cause on deadline.
	WithDeadlineCause = context.WithDeadlineCause
	// WithTimeout returns WithDeadline(parent, time.Now().Add(timeout)).
	WithTimeout = context.WithTimeout
	// WithTimeoutCause behaves like WithTimeout but also sets a cause on timeout.
	WithTimeoutCause = context.WithTimeoutCause
	// WithoutCancel returns a copy of parent that is not canceled when parent is canceled.
	WithoutCancel = context.WithoutCancel
	// WithValue returns a copy of parent in which the value for key is val.
	WithValue = context.WithValue
	// AfterFunc arranges to call f in its own goroutine after ctx is done.
	AfterFunc = context.AfterFunc
	// Cause returns a non-nil error explaining why the context was canceled.
	Cause = context.Cause
	// TODO returns a non-nil, empty Context for when it is unclear which Context to use.
	TODO = context.TODO
)
