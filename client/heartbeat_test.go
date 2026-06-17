package client

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/retry/exponential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// fakeStream is a Session_KeepAliveClient that records CloseSend calls. Only the
// methods the client exercises are implemented; the rest come from the embedded (nil)
// grpc.ClientStream and are never called.
type fakeStream struct {
	grpc.ClientStream
	closeSends atomic.Int64
}

func (f *fakeStream) Send(*zuulv1.KeepAliveRequest) error { return nil }
func (f *fakeStream) Recv() (*zuulv1.KeepAliveResponse, error) {
	return &zuulv1.KeepAliveResponse{}, nil
}
func (f *fakeStream) CloseSend() error {
	f.closeSends.Add(1)
	return nil
}

// fakeSession is a SessionClient whose KeepAlive returns a configured stream/error and
// counts how many times it was called.
type fakeSession struct {
	stream         zuulv1.Session_KeepAliveClient
	err            error
	keepAliveCalls atomic.Int64
}

func (f *fakeSession) KeepAlive(ctx context.Context, opts ...grpc.CallOption) (zuulv1.Session_KeepAliveClient, error) {
	f.keepAliveCalls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return f.stream, nil
}

// TestSetStreamClosesPrevious is a regression test for the keepalive stream leak: each
// reconnect swapped in a new stream without closing the old one, leaking a stream per
// failover. setStream must CloseSend the previously installed stream.
func TestSetStreamClosesPrevious(t *testing.T) {
	c := &Client{done: make(chan struct{})}
	first := &fakeStream{}
	second := &fakeStream{}

	c.setStream(first, func() {})
	c.setStream(second, func() {})

	if got := first.closeSends.Load(); got != 1 {
		t.Errorf("TestSetStreamClosesPrevious: previous stream CloseSend calls = %d, want 1", got)
	}
	if got := second.closeSends.Load(); got != 0 {
		t.Errorf("TestSetStreamClosesPrevious: current stream CloseSend calls = %d, want 0", got)
	}
}

// recvErrStream is a Session_KeepAliveClient whose Recv returns a configured error,
// used to drive a per-message transport failure through pulse.
type recvErrStream struct {
	grpc.ClientStream
	recvErr error
}

func (s *recvErrStream) Send(*zuulv1.KeepAliveRequest) error { return nil }
func (s *recvErrStream) Recv() (*zuulv1.KeepAliveResponse, error) {
	return nil, s.recvErr
}
func (s *recvErrStream) CloseSend() error { return nil }

// blockStream is a Session_KeepAliveClient whose Recv blocks until released, used to
// drive the pulse round-trip timeout deterministically.
type blockStream struct {
	grpc.ClientStream
	release chan struct{}
}

func (s *blockStream) Send(*zuulv1.KeepAliveRequest) error { return nil }
func (s *blockStream) Recv() (*zuulv1.KeepAliveResponse, error) {
	<-s.release
	return nil, status.Error(codes.Canceled, "released")
}
func (s *blockStream) CloseSend() error { return nil }

// TestPulseClassifiesError is a regression test for the lost-session error classification:
// pulse's failure becomes c.lostErr (via markLost) and is returned to the user by Err, so
// it must be a classified zuul error carrying a gRPC code — not a raw transport status or
// a bare context error. Before the fix pulse returned the unwrapped errNoStream / transport
// status / context.DeadlineExceeded.
func TestPulseClassifiesError(t *testing.T) {
	tests := []struct {
		name     string
		stream   zuulv1.Session_KeepAliveClient
		wantCode codes.Code
		wantIs   error
	}{
		{
			name:     "Error: a per-message transport failure is classified and keeps its code",
			stream:   &recvErrStream{recvErr: status.Error(codes.Unavailable, "peer gone")},
			wantCode: codes.Unavailable,
		},
		{
			name:     "Error: a nil stream is classified Unavailable and still matches errNoStream",
			stream:   nil,
			wantCode: codes.Unavailable,
			wantIs:   errNoStream,
		},
	}

	for _, test := range tests {
		c := &Client{ttl: 30 * time.Second, sessCtx: t.Context(), done: make(chan struct{})}
		if test.stream != nil {
			c.setStream(test.stream, func() {})
		}
		err := c.pulse(t.Context())
		switch {
		case err == nil:
			t.Errorf("TestPulseClassifiesError(%s): got nil error, want non-nil", test.name)
			continue
		case status.Code(err) != test.wantCode:
			t.Errorf("TestPulseClassifiesError(%s): got code == %s, want %s", test.name, status.Code(err), test.wantCode)
		}
		var ze errors.Error
		if !errors.As(err, &ze) {
			t.Errorf("TestPulseClassifiesError(%s): error is not a classified zuul errors.Error", test.name)
		}
		if test.wantIs != nil && !errors.Is(err, test.wantIs) {
			t.Errorf("TestPulseClassifiesError(%s): got Is(errNoStream) == false, want true", test.name)
		}
	}
}

// TestPulseTimeoutClassified proves the half-open path (pulse round-trip exceeds its
// budget) surfaces a classified Unavailable error rather than a bare context error.
func TestPulseTimeoutClassified(t *testing.T) {
	bs := &blockStream{release: make(chan struct{})}
	defer close(bs.release) // unblock the submitted Recv after the timeout fires.

	// ttl/3 is the pulse budget; a small ttl keeps the timeout quick and deterministic.
	c := &Client{ttl: 30 * time.Millisecond, sessCtx: t.Context(), done: make(chan struct{})}
	c.setStream(bs, func() {})

	err := c.pulse(t.Context())
	switch {
	case err == nil:
		t.Fatalf("TestPulseTimeoutClassified: got nil error, want non-nil")
	case status.Code(err) != codes.Unavailable:
		t.Errorf("TestPulseTimeoutClassified: got code == %s, want Unavailable", status.Code(err))
	}
	var ze errors.Error
	if !errors.As(err, &ze) {
		t.Errorf("TestPulseTimeoutClassified: error is not a classified zuul errors.Error")
	}
}

// TestReestablishPermanentAuth is a regression test for retry-forever-on-auth: a
// non-retryable credential rejection must stop reconnect attempts immediately instead
// of being retried until the lease deadline. With the fix, exactly one attempt is made.
func TestReestablishPermanentAuth(t *testing.T) {
	boff, err := exponential.New()
	if err != nil {
		t.Fatalf("TestReestablishPermanentAuth: exponential.New: %s", err)
	}
	sessCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	fs := &fakeSession{err: status.Error(codes.Unauthenticated, "bad credentials")}
	c := &Client{
		session: fs,
		ttl:     30 * time.Second,
		sessCtx: sessCtx,
		boff:    boff,
		done:    make(chan struct{}),
	}
	c.renewed() // give the lease a live deadline so reestablish is allowed to try.

	if ok := c.reestablish(sessCtx); ok {
		t.Fatalf("TestReestablishPermanentAuth: got reestablish == true, want false")
	}
	if got := fs.keepAliveCalls.Load(); got != 1 {
		t.Errorf("TestReestablishPermanentAuth: KeepAlive attempts = %d, want 1 (auth rejection must not retry)", got)
	}
}

// TestReestablishLapsedLease proves reestablish gives up at once once the lease deadline
// has already passed, rather than reconnecting to a session whose locks are already gone.
func TestReestablishLapsedLease(t *testing.T) {
	boff, err := exponential.New()
	if err != nil {
		t.Fatalf("TestReestablishLapsedLease: exponential.New: %s", err)
	}
	fs := &fakeSession{stream: &fakeStream{}}
	c := &Client{
		session: fs,
		ttl:     30 * time.Second,
		sessCtx: t.Context(),
		boff:    boff,
		done:    make(chan struct{}),
	}
	// deadlineNano left at zero (epoch) → already lapsed.

	if ok := c.reestablish(t.Context()); ok {
		t.Fatalf("TestReestablishLapsedLease: got reestablish == true, want false")
	}
	if got := fs.keepAliveCalls.Load(); got != 0 {
		t.Errorf("TestReestablishLapsedLease: KeepAlive attempts = %d, want 0 (lapsed lease must not reconnect)", got)
	}
}
