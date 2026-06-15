package node

import (
	"testing"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gostdlib/base/context"
)

// TestRateLimitUnary proves the unary interceptor admits up to the burst then rejects
// with ResourceExhausted.
func TestRateLimitUnary(t *testing.T) {
	lim := rate.NewLimiter(rate.Limit(0.0001), 1) // burst 1, effectively no refill
	intercept := rateLimitUnary(lim)
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/zuul.v1.Locker/Lock"}

	if _, err := intercept(t.Context(), nil, info, handler); err != nil {
		t.Fatalf("TestRateLimitUnary: first call: got err == %s, want nil (within burst)", err)
	}
	_, err := intercept(t.Context(), nil, info, handler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("TestRateLimitUnary: second call: got code %s, want ResourceExhausted", status.Code(err))
	}
}

// TestRateLimiterDisabled proves no limiter is built when the rate is zero.
func TestRateLimiterDisabled(t *testing.T) {
	if rateLimiter(Config{RateLimitPerSec: 0}) != nil {
		t.Errorf("TestRateLimiterDisabled: got a limiter, want nil when RateLimitPerSec==0")
	}
	if rateLimiter(Config{RateLimitPerSec: 100}) == nil {
		t.Errorf("TestRateLimiterDisabled: got nil, want a limiter when RateLimitPerSec>0")
	}
}
