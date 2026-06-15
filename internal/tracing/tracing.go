// Package tracing provides OpenTelemetry span interceptors for Zuul's gRPC planes,
// with W3C trace-context propagation so a client request's trace links across the
// node it lands on and any node a write is forwarded to. Spans come from the
// process's configured tracer provider (no-op when none is set, so it is free when
// tracing is off — the gostdlib/base telemetry setup wires one in production).
package tracing

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/gostdlib/base/context"
)

const tracerName = "github.com/johnsiilver/zuul"

// propagator carries the trace context across the gRPC boundary (W3C traceparent).
var propagator = propagation.TraceContext{}

func tracer() trace.Tracer { return otel.Tracer(tracerName) }

// ServerUnaryInterceptor starts a server span for each unary RPC, continuing any
// trace propagated in the request metadata.
func ServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extract(ctx)
		ctx, span := tracer().Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return resp, err
	}
}

// ServerStreamInterceptor starts a server span for each streaming RPC.
func ServerStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extract(ss.Context())
		ctx, span := tracer().Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		err := handler(srv, &tracedStream{ServerStream: ss, ctx: ctx})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

// ClientUnaryInterceptor starts a client span for each forwarded unary RPC and
// injects the trace context into the request metadata.
func ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := tracer().Start(ctx, method, trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
		ctx = inject(ctx)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
}

// tracedStream carries the span-bearing context to the stream handler.
type tracedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *tracedStream) Context() context.Context { return s.ctx }

// extract pulls a propagated trace context out of the incoming request metadata.
func extract(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	carrier := propagation.MapCarrier{}
	for k, vs := range md {
		if len(vs) > 0 {
			carrier[k] = vs[0]
		}
	}
	return propagator.Extract(ctx, carrier)
}

// inject writes the current trace context into the outgoing request metadata.
func inject(ctx context.Context) context.Context {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	for k, v := range carrier {
		ctx = metadata.AppendToOutgoingContext(ctx, k, v)
	}
	return ctx
}
