package fsm

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kylelemons/godebug/pretty"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
)

// TestElectionCommands covers Campaign/Proclaim/Resign result outcomes. Election is
// the lock primitive plus a published value, so it shares the fencing/queue rules.
func TestElectionCommands(t *testing.T) {
	aVal := []byte("a-value")
	bVal := []byte("b-value")
	newVal := []byte("a-value-2")

	tests := []struct {
		name string
		cmds []*fsmpb.Command
		want *fsmpb.CommandResult
	}{
		{
			name: "Success: campaign for a free key wins leadership with its value",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), campaign("E", "a", aVal)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_GRANTED, LockKey: "E", FencingToken: 1, Value: aVal},
		},
		{
			name: "Success: campaign for a held key enqueues the candidate",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), campaign("E", "a", aVal), campaign("E", "b", bVal)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_QUEUED, LockKey: "E", QueuePosition: 1},
		},
		{
			name: "Success: leader proclaims a new value",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), campaign("E", "a", aVal), proclaim("E", "a", 1, newVal)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_VALUE_UPDATED, LockKey: "E", FencingToken: 1, Value: newVal},
		},
		{
			name: "Success: proclaim with a stale token is rejected",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), campaign("E", "a", aVal), proclaim("E", "a", 999, newVal)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_STALE_TOKEN, LockKey: "E"},
		},
		{
			name: "Success: proclaim by a non-leader is rejected",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), campaign("E", "a", aVal), proclaim("E", "b", 1, bVal)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NOT_HOLDER, LockKey: "E"},
		},
		{
			name: "Success: leader resigns and the key is released",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), campaign("E", "a", aVal), resign("E", "a", 1)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_RELEASED, LockKey: "E"},
		},
	}

	for _, test := range tests {
		f := New(nil)
		got, err := applyAll(f, test.cmds)
		if err != nil {
			t.Errorf("TestElectionCommands(%s): got err == %s, want err == nil", test.name, err)
			continue
		}
		test.want.Revision = uint64(len(test.cmds))
		if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
			t.Errorf("TestElectionCommands(%s): result: -want +got:\n%s", test.name, diff)
		}
	}
}

// TestElectionLeadershipTransfer proves that on resign the next candidate becomes
// leader carrying *its own* value, with a strictly larger fencing token.
func TestElectionLeadershipTransfer(t *testing.T) {
	aVal := []byte("a-value")
	bVal := []byte("b-value")
	f := New(nil)

	setup := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		campaign("E", "a", aVal), // a leads, token 1 (seq 1)
		campaign("E", "b", bVal), // b queued (seq 2)
	}
	if _, err := applyAll(f, setup); err != nil {
		t.Fatalf("TestElectionLeadershipTransfer: setup: got err == %s, want err == nil", err)
	}

	gotA, _ := f.Query(StatusQuery{Name: "E"})
	wantA := Status{Held: true, Holder: "a", Token: 1, Value: aVal, QueueDepth: 1, Revision: 4}
	if diff := pretty.Compare(wantA, gotA); diff != "" {
		t.Errorf("TestElectionLeadershipTransfer(a leads): -want +got:\n%s", diff)
	}

	if _, err := f.Apply(resign("E", "a", 1)); err != nil {
		t.Fatalf("TestElectionLeadershipTransfer: resign: got err == %s, want err == nil", err)
	}
	gotB, _ := f.Query(StatusQuery{Name: "E"})
	wantB := Status{Held: true, Holder: "b", Token: 3, Value: bVal, QueueDepth: 0, Revision: 5}
	if diff := pretty.Compare(wantB, gotB); diff != "" {
		t.Errorf("TestElectionLeadershipTransfer(b leads): -want +got:\n%s", diff)
	}
}
