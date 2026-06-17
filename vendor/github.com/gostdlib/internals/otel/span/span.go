/*
Package span provides access to OpenTelemetry Span objects for the purpose of logging events
for the gostdlib packages.

OTEL spans are not well defined. For our purposes, we will generally create a child span on
interesting function calls within a package.
*/
package span

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type tracerKeyType int

var tracerKey = tracerKeyType(0)

const tracerName = "instrumentation/github.com/gostdlib"

// Span represents an OTEL span for recording events to. This superclass handles noops
// for the trace.Span and events we record. You get a child Span object by using New() or
// an existing Span object by using Get(). We have standardized how we records events and
// other type of actions on the span. The original Span object is available for other needs.
type Span struct {
	// Span is the OTEL trace.Span.
	Span trace.Span
}

// New creates a new child span object from the span stored in Context. If that Span is
// a noOp, the child span will be a noop too.
func New(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, Span) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return ctx, Span{Span: span}
	}
	ctx, tracer := getTracer(ctx, span)

	var sp trace.Span
	ctx, sp = tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindInternal))

	return ctx, Span{Span: sp}
}

// getTracer gets a trace.Tracer from the Context or the span. This SHOULD ONLY BE USED
// IF spans.IsRecording() is true. Otherwise it panics.
func getTracer(ctx context.Context, span trace.Span) (context.Context, trace.Tracer) {
	if span == nil || !span.IsRecording() {
		panic("getTracer called when span.IsRecording() is false")
	}
	if t := ctx.Value(tracerKey); t != nil {
		return ctx, t.(trace.Tracer)
	}
	tracer := span.TracerProvider().Tracer(tracerName)
	ctx = context.WithValue(ctx, tracerKey, tracer)

	return ctx, tracer
}

// Get returns the Span for recording events. This retrieves the OTEL span from the
// Context object. If the OTEL span is a noop, then all events will also be a noop.
func Get(ctx context.Context) Span {
	return Span{Span: trace.SpanFromContext(ctx)}
}

// Event records an event with name and keyvalues. keyvalues must be an even number
// with every even value a string representing the key, with the following value representing
// the value associated with that key. The following values are supported:
//   - bool/[]bool
//   - float64/[]float64
//   - int/[]int
//   - int64/[]int64
//   - string/[]string
//   - time.Duration/[]time.Duration
func (s Span) Event(name string, keyValues ...any) error {
	if !s.IsRecording() {
		return nil
	}

	if name == "" {
		return fmt.Errorf("must provide an event name")
	}

	attrs, err := makeKeyValues(keyValues)
	if err != nil {
		return err
	}
	s.Span.AddEvent(
		name,
		trace.WithAttributes(attrs...),
		trace.WithTimestamp(time.Now().UTC()),
	)

	return nil
}

// IsRecording returns true if the span is recording events. This
// is safe to call even if the span is nil.
func (s Span) IsRecording() bool {
	if s.Span == nil || !s.Span.IsRecording() {
		return false
	}
	return true
}

// Error records an error on the Span. This does not set the status of the span. If you
// want to set the status as well, use Status(). keyvalues must be an even number with every
// even value a string representing the key, with the following value representing the value
// associated with that key. The following values are supported:
//   - bool/[]bool
//   - float64/[]float64
//   - int/[]int
//   - int64/[]int64
//   - string/[]string
//   - time.Duration/[]time.Duration
func (s Span) Error(e error, keyValues ...any) error {
	if !s.IsRecording() {
		return nil
	}

	attrs, err := makeKeyValues(keyValues)
	if err != nil {
		return err
	}
	s.Span.RecordError(
		e,
		trace.WithAttributes(attrs...),
		trace.WithTimestamp(time.Now().UTC()),
	)
	return nil
}

// Status records a status for the span.
func (s Span) Status(code codes.Code, description string) {
	s.Span.SetStatus(code, description)
}

// End ends the span.
func (s Span) End() {
	s.Span.End()
}

func makeKeyValues(keyValues ...any) ([]attribute.KeyValue, error) {
	if len(keyValues)%2 != 0 {
		return nil, fmt.Errorf("event keyvalues must be an even number")
	}
	attrs := make([]attribute.KeyValue, 0, len(keyValues)/2)
	var key string
	var err error
	for i, v := range keyValues {
		if i%2 == 0 {
			var ok bool
			key, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("keyvalue(%v) was not a string type required for keys", v)
			}
		} else {
			if attrs, err = addKeyValue(key, v, attrs); err != nil {
				return nil, fmt.Errorf("keyvalue(%v) had error: %s", key, err)
			}
		}
	}
	return attrs, nil
}

func addKeyValue(k string, i any, attrs []attribute.KeyValue) ([]attribute.KeyValue, error) {
	switch v := i.(type) {
	case bool:
		attrs = append(attrs, attribute.Bool(k, v))
	case []bool:
		attrs = append(attrs, attribute.BoolSlice(k, v))
	case float64:
		attrs = append(attrs, attribute.Float64(k, v))
	case []float64:
		attrs = append(attrs, attribute.Float64Slice(k, v))
	case int:
		attrs = append(attrs, attribute.Int(k, v))
	case []int:
		attrs = append(attrs, attribute.IntSlice(k, v))
	case int64:
		attrs = append(attrs, attribute.Int64(k, v))
	case []int64:
		attrs = append(attrs, attribute.Int64Slice(k, v))
	case string:
		attrs = append(attrs, attribute.String(k, v))
	case []string:
		attrs = append(attrs, attribute.StringSlice(k, v))
	case time.Duration:
		attrs = append(attrs, attribute.String(k, v.String()))
	case []time.Duration:
		sl := make([]string, 0, len(v))
		for _, d := range v {
			sl = append(sl, d.String())
		}
		attrs = append(attrs, attribute.StringSlice(k, sl))
	default:
		return nil, fmt.Errorf("bug: event.Add(): receiveing %T which is not supported", v)
	}
	return attrs, nil
}
