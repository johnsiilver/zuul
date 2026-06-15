package tracing

import (
	"testing"

	"github.com/gostdlib/base/context"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestTracePropagation proves the client interceptor injects the trace context and
// the server interceptor continues the same trace — so a forwarded write links across
// nodes — and that both spans are recorded.
func TestTracePropagation(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))

	var clientTrace, serverTrace oteltrace.TraceID

	handler := func(ctx context.Context, _ any) (any, error) {
		serverTrace = oteltrace.SpanContextFromContext(ctx).TraceID()
		return "ok", nil
	}
	serverInt := ServerUnaryInterceptor()

	// The client invoker captures its own trace id and replays the outgoing metadata
	// into the server interceptor as if it had crossed the wire.
	invoker := func(ctx context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		clientTrace = oteltrace.SpanContextFromContext(ctx).TraceID()
		md, _ := metadata.FromOutgoingContext(ctx)
		inCtx := metadata.NewIncomingContext(context.Background(), md)
		_, err := serverInt(inCtx, req, &grpc.UnaryServerInfo{FullMethod: "/zuul.v1.Locker/Lock"}, handler)
		return err
	}
	if err := ClientUnaryInterceptor()(context.Background(), "/zuul.v1.Locker/Lock", nil, nil, nil, invoker); err != nil {
		t.Fatalf("TestTracePropagation: client interceptor: %s", err)
	}

	if !clientTrace.IsValid() {
		t.Fatalf("TestTracePropagation: client trace id is invalid")
	}
	if clientTrace != serverTrace {
		t.Errorf("TestTracePropagation: trace not propagated: client=%s server=%s", clientTrace, serverTrace)
	}
	if got := len(rec.Ended()); got < 2 {
		t.Errorf("TestTracePropagation: recorded %d spans, want >= 2 (client + server)", got)
	}
}

// TestServerSpanName proves the server interceptor names its span after the RPC method.
func TestServerSpanName(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))

	handler := func(context.Context, any) (any, error) { return nil, nil }
	if _, err := ServerUnaryInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/zuul.v1.Locker/Unlock"}, handler); err != nil {
		t.Fatalf("TestServerSpanName: %s", err)
	}
	ended := rec.Ended()
	if len(ended) != 1 || ended[0].Name() != "/zuul.v1.Locker/Unlock" {
		t.Errorf("TestServerSpanName: got %d spans named %v, want 1 named /zuul.v1.Locker/Unlock", len(ended), spanNames(ended))
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}
