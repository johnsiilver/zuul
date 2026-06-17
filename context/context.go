/*
Package context is zuul's service context package, built on
github.com/gostdlib/base/context. It is the single context package for the service:
import it as "context" everywhere (it is a drop-in superset of the standard library's
context package, see stdlib.go) and reach the whole API through it — Background, Attach,
Log, Pool, Tasks, Meter, the span/metric helpers, and the stdlib context surface.

It wraps github.com/gostdlib/base/context, which attaches the service's logger, metrics,
worker pool, and background task manager to the Context in Background(). Call Background()
after init.Service() so those clients are initialized. Attach custom clients only at the
marked insertion points in Background() and Attach(); the re-exports below are not meant
to be rewritten.
*/
package context

import "github.com/gostdlib/base/context"

// Background returns the top-level, non-cancelable Context with the service's clients
// (logger, metrics, worker pool, background tasks) attached. Call it from main, init, and
// tests, and as the root for incoming requests. Call it after init.Service() so the
// attached clients reflect the configured defaults.
func Background() Context {
	// Insert here anything you want attached to your Context object.
	return context.Background()
}

// Attach attaches the service's clients to an already-existing context, for entry points
// (e.g. RPC packages) that did not originate from Background(). DO NOT use this on a
// context returned by Background().
func Attach(ctx Context) Context {
	ctx = context.Attach(ctx)
	// Add your own clients here if you need to attach at some other entry point.
	return ctx
}

// Types re-exported from base/context so callers need only this package.
type (
	// Context carries a deadline, a cancellation signal, and other values across API
	// boundaries.
	Context = context.Context
	// CancelFunc tells an operation to abandon its work.
	CancelFunc = context.CancelFunc
	// CancelCauseFunc behaves like CancelFunc but additionally records a cancellation cause.
	CancelCauseFunc = context.CancelCauseFunc
	// Logger is the context-aware logger returned by Log.
	Logger = context.Logger
)

// Sentinel errors re-exported from base/context.
var (
	// Canceled is the error returned by Context.Err when the context is canceled.
	Canceled = context.Canceled
	// DeadlineExceeded is the error returned by Context.Err when the deadline passes.
	DeadlineExceeded = context.DeadlineExceeded
)

// base/context additions re-exported so callers need only this package. These are var
// aliases (not wrappers) so they introduce no extra stack frame — Meter/Log scope their
// output to the real caller's package.
var (
	// Log returns the Logger attached to ctx, falling back to the default logger.
	Log = context.Log
	// Pool returns the worker pool attached to ctx, falling back to the default pool.
	Pool = context.Pool
	// Tasks returns the background task manager attached to ctx, falling back to default.
	Tasks = context.Tasks
	// Meter returns a metric.Meter scoped to the calling package.
	Meter = context.Meter
	// MeterWithStackFrame returns a metric.Meter scoped to the given stack frame.
	MeterWithStackFrame = context.MeterWithStackFrame
	// MeterProvider returns the metric.MeterProvider attached to ctx.
	MeterProvider = context.MeterProvider
	// NewSpan creates and starts a child span from the span stored in ctx.
	NewSpan = context.NewSpan
	// Span returns the current span from ctx (a noop span if none is attached).
	Span = context.Span
	// AddAttrs adds slog attributes to ctx for logging, tracing, and errors.
	AddAttrs = context.AddAttrs
	// Attrs returns the slog attributes attached to ctx.
	Attrs = context.Attrs
	// SetShouldTrace records on ctx whether the request should be traced.
	SetShouldTrace = context.SetShouldTrace
	// ShouldTrace reports whether SetShouldTrace marked ctx for tracing.
	ShouldTrace = context.ShouldTrace
	// EOptions returns the per-call error options attached to ctx.
	EOptions = context.EOptions
	// SetEOptions attaches per-call error options to ctx.
	SetEOptions = context.SetEOptions
)
