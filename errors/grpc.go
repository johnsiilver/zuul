package errors

import (
	"github.com/gostdlib/base/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor maps any error a unary handler returns through Status, so every
// error leaving the server carries a gRPC code from the taxonomy and no unwrapped error
// reaches the client. Install it as the outermost server interceptor.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, Status(ctx, err)
		}
		return resp, nil
	}
}

// StreamServerInterceptor maps any error a stream handler returns through Status.
// Install it as the outermost server stream interceptor.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return Status(ss.Context(), handler(srv, ss))
	}
}

// Status converts err into a gRPC status error for a server handler to return. A nil err
// returns nil. An err that already carries a gRPC status is returned unchanged. An Error
// uses its Category's gRPC code and its (redacted) message. Any other error is classified
// as Internal so an unclassified error never leaves the server without a code.
func Status(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	var e Error
	if As(err, &e) {
		c := UnknownCategory
		if cat, ok := e.Category.(Category); ok {
			c = cat
		}
		return status.Error(c.Code(), e.Error())
	}
	e = E(ctx, CatInternal, UnknownType, err)
	return status.Error(codes.Internal, e.Error())
}

// FromStatus maps a gRPC status error received by a client back into a classified Error,
// so an unwrapped transport error never reaches the user. The original error is kept in
// the chain, so status.Code(FromStatus(ctx, err)) still reports the original code. A nil
// err returns nil.
func FromStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return E(ctx, CatInternal, UnknownType, err)
	}
	return E(ctx, codeToCat(st.Code()), UnknownType, err)
}

// UnaryClientInterceptor maps the error of every unary RPC through FromStatus, so an
// unwrapped transport error never reaches the caller. Install it as the outermost client
// interceptor.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return FromStatus(ctx, invoker(ctx, method, req, reply, cc, opts...))
	}
}

// StreamClientInterceptor maps a stream-open error through FromStatus. Per-message
// Recv/Send errors are returned by the stream itself and are not seen here.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, FromStatus(ctx, err)
		}
		return cs, nil
	}
}

// codeToCat maps a gRPC status code back to a Category.
func codeToCat(c codes.Code) Category {
	switch c {
	case codes.InvalidArgument:
		return CatRequest
	case codes.NotFound:
		return CatNotFound
	case codes.Unauthenticated:
		return CatUnauthenticated
	case codes.PermissionDenied:
		return CatPermission
	case codes.FailedPrecondition:
		return CatPrecondition
	case codes.ResourceExhausted:
		return CatResourceExhausted
	case codes.Unavailable:
		return CatUnavailable
	case codes.Unimplemented:
		return CatUnimplemented
	case codes.Internal:
		return CatInternal
	}
	return UnknownCategory
}
