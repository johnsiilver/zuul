package forward

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
)

// fakeLocal is a Local that reports this node as a non-leader follower of a known remote
// leader, so dispatch routes to the peer forwarding path.
type fakeLocal struct{}

func (fakeLocal) ReplicaID() uint64              { return 1 }
func (fakeLocal) IsLeader(shardID uint64) bool   { return false }
func (fakeLocal) LeaderID(uint64) (uint64, bool) { return 2, true }

func (fakeLocal) Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error) {
	return nil, nil
}

func (fakeLocal) AddReplicaShard(ctx context.Context, shardID, replicaID uint64, raftAddr string) error {
	return nil
}

func (fakeLocal) RemoveReplicaShard(ctx context.Context, shardID, replicaID uint64) error {
	return nil
}

// fakeResolver maps the remote leader's replica id to a forwarding address.
type fakeResolver struct{}

func (fakeResolver) Addr(replicaID uint64) (string, bool) { return "peer.invalid:1234", true }

// TestClassifyPermanent is a regression test for dispatch error classification. A
// structurally permanent failure (errStopRetry, here a closed pool reached on the peer
// path) must surface as a non-retryable Internal error, not a retryable Unavailable one —
// otherwise a caller keeps retrying a request that can never succeed. A transient failure
// must still be classified Unavailable.
func TestClassifyPermanent(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantPerm bool
	}{
		{
			name:     "Success: a structurally permanent errStopRetry failure is Internal and non-retryable",
			err:      fmt.Errorf("dial peer.invalid:1234: %w", errStopRetry),
			wantCode: codes.Internal,
			wantPerm: true,
		},
		{
			name:     "Success: a transient consensus failure is retryable Unavailable",
			err:      errNoLeader,
			wantCode: codes.Unavailable,
			wantPerm: false,
		},
	}

	d := &Dispatcher{}
	for _, test := range tests {
		got := d.classify(t.Context(), test.err)
		if got == nil {
			t.Errorf("TestClassifyPermanent(%s): got nil error, want non-nil", test.name)
			continue
		}
		if c := status.Code(got); c != test.wantCode {
			t.Errorf("TestClassifyPermanent(%s): got code == %s, want %s", test.name, c, test.wantCode)
		}
		if perm := errors.Is(got, errors.ErrPermanent); perm != test.wantPerm {
			t.Errorf("TestClassifyPermanent(%s): got Is(ErrPermanent) == %t, want %t", test.name, perm, test.wantPerm)
		}
	}
}

// TestAddReplicaPermanentClassification exercises the full AddReplica path: with the pool
// closed, routing to the remote leader fails with errPoolClosed wrapped as errStopRetry.
// The returned error must be a non-retryable Internal error (the bug returned Unavailable).
func TestAddReplicaPermanentClassification(t *testing.T) {
	d, err := NewDispatcher(fakeLocal{}, fakeResolver{})
	if err != nil {
		t.Fatalf("TestAddReplicaPermanentClassification: NewDispatcher: %s", err)
	}
	d.Close() // closing the pool makes the peer dial path return errPoolClosed (permanent).

	got := d.AddReplica(t.Context(), 5, 9, "10.0.0.9:7000")
	switch {
	case got == nil:
		t.Fatalf("TestAddReplicaPermanentClassification: got nil error, want non-nil")
	case !errors.Is(got, errStopRetry):
		t.Errorf("TestAddReplicaPermanentClassification: got Is(errStopRetry) == false, want true")
	case !errors.Is(got, errors.ErrPermanent):
		t.Errorf("TestAddReplicaPermanentClassification: got Is(ErrPermanent) == false, want true (permanent failure must not be retryable)")
	case status.Code(got) != codes.Internal:
		t.Errorf("TestAddReplicaPermanentClassification: got code == %s, want Internal", status.Code(got))
	}
}
