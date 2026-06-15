package fsm

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kylelemons/godebug/pretty"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
)

// richState builds a non-trivial FSM: held locks with a waiter, an election, and a
// lease that holds nothing — enough to exercise every snapshot field and the
// held/waiting index rebuild on restore.
func richState(t *testing.T) *FSM {
	t.Helper()
	f := New(nil)
	cmds := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		leaseGrant("c", ttlMS, now0),
		leaseGrant("d", ttlMS, now0), // holds nothing
		acquire("L1", "a"),           // a holds L1
		acquire("L1", "b"),           // b waits on L1
		campaign("E", "a", []byte("ev")),
		acquire("L2", "c"), // c holds L2
	}
	if _, err := applyAll(f, cmds); err != nil {
		t.Fatalf("richState: got err == %s, want err == nil", err)
	}
	return f
}

// TestSnapshotRestoreRoundTrip proves that snapshot -> restore reproduces byte-for-
// byte identical state (including seq/revision) and that the rebuilt lease indexes
// behave correctly under a subsequent release.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	orig := richState(t)
	snap := orig.Snapshot()

	restored := New(nil)
	restored.Restore(snap)

	if diff := cmp.Diff(snap, restored.Snapshot(), protocmp.Transform()); diff != "" {
		t.Errorf("TestSnapshotRestoreRoundTrip: restored snapshot: -want +got:\n%s", diff)
	}

	// The rebuilt waiting index must still promote b when a releases L1.
	if _, err := restored.Apply(release("L1", "a", 1)); err != nil {
		t.Fatalf("TestSnapshotRestoreRoundTrip: release: got err == %s, want err == nil", err)
	}
	got, _ := restored.Query(StatusQuery{Name: "L1"})
	// seq before restore was 4 (L1 grant=1, L1 enqueue=2, E grant=3, L2 grant=4);
	// the promotion takes seq 5, so b's token is 5.
	want := Status{Held: true, Holder: "b", Token: 5, QueueDepth: 0, Revision: snap.GetRevision() + 1}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestSnapshotRestoreRoundTrip: post-restore promotion: -want +got:\n%s", diff)
	}
}

// TestNotifierEvents asserts the exact ownership-change events emitted across a
// grant -> proclaim -> resign(free) flow and a promotion flow. These events are
// what the per-node watch hub turns into client wakeups.
func TestNotifierEvents(t *testing.T) {
	tests := []struct {
		name string
		cmds []*fsmpb.Command
		want []Event
	}{
		{
			name: "Success: grant, value update, and going-free each emit one event",
			cmds: []*fsmpb.Command{
				leaseGrant("a", ttlMS, now0),
				campaign("E", "a", []byte("v1")),
				proclaim("E", "a", 1, []byte("v2")),
				resign("E", "a", 1),
			},
			want: []Event{
				{Key: "E", Holder: "a", Token: 1, Value: []byte("v1"), Revision: 2},
				{Key: "E", Holder: "a", Token: 1, Value: []byte("v2"), Revision: 3},
				{Key: "E", Holder: "", Token: 0, Value: nil, Revision: 4},
			},
		},
		{
			name: "Success: a release that promotes a waiter emits the new holder",
			cmds: []*fsmpb.Command{
				leaseGrant("a", ttlMS, now0),
				leaseGrant("b", ttlMS, now0),
				acquire("L", "a"), // emit holder a token 1
				acquire("L", "b"), // queued: no event
				release("L", "a", 1),
			},
			want: []Event{
				{Key: "L", Holder: "a", Token: 1, Revision: 3},
				{Key: "L", Holder: "b", Token: 3, Revision: 5},
			},
		},
	}

	for _, test := range tests {
		n := &recordingNotifier{}
		f := New(n)
		if _, err := applyAll(f, test.cmds); err != nil {
			t.Fatalf("TestNotifierEvents(%s): got err == %s, want err == nil", test.name, err)
		}
		if diff := pretty.Compare(test.want, n.events); diff != "" {
			t.Errorf("TestNotifierEvents(%s): events: -want +got:\n%s", test.name, diff)
		}
	}
}

// recordingNotifier captures emitted events in order for assertion.
type recordingNotifier struct {
	events []Event
}

func (r *recordingNotifier) Notify(e Event) {
	r.events = append(r.events, e)
}
