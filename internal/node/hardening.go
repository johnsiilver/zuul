package node

import (
	"fmt"
	"math"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
)

// baseHardening returns the connection-level gRPC server options that protect the
// node from oversized messages, too many streams, and abusive keepalive pings.
func baseHardening(cfg Config) []grpc.ServerOption {
	maxRecv := cfg.MaxRecvBytes
	if maxRecv <= 0 {
		maxRecv = 1 << 20 // 1 MiB; lock/election requests are tiny
	}
	maxStreams := cfg.MaxConcurrentStreams
	if maxStreams == 0 {
		maxStreams = 4096
	}
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxRecv),
		grpc.MaxConcurrentStreams(maxStreams),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// rateLimiter returns a token-bucket limiter for the configured request rate, or nil
// when rate limiting is disabled.
func rateLimiter(cfg Config) *rate.Limiter {
	if cfg.RateLimitPerSec <= 0 {
		return nil
	}
	burst := cfg.RateBurst
	if burst <= 0 {
		burst = int(math.Ceil(cfg.RateLimitPerSec)) + 1
	}
	return rate.NewLimiter(rate.Limit(cfg.RateLimitPerSec), burst)
}

// rateLimitUnary rejects a unary call with ResourceExhausted when the limiter is
// drained.
func rateLimitUnary(l *rate.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !l.Allow() {
			return nil, errors.E(ctx, errors.CatResourceExhausted, errors.TypeRateLimited, fmt.Errorf("rate limit exceeded"))
		}
		return handler(ctx, req)
	}
}

// rateLimitStream rejects a new stream with ResourceExhausted when the limiter is
// drained (charged once at stream open).
func rateLimitStream(l *rate.Limiter) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !l.Allow() {
			return errors.E(ss.Context(), errors.CatResourceExhausted, errors.TypeRateLimited, fmt.Errorf("rate limit exceeded"))
		}
		return handler(srv, ss)
	}
}
