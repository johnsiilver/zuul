package fsm

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/johnsiilver/zuul/internal/raft/fsm/fsmpb"
)

// setupEnum builds an FSM with two lease holders and a few locks/elections for the
// enumeration tests: "a" holds /alice/lock1 and an election /alice/elect (value "v");
// "b" holds /bob/lock and is queued behind "a" on /alice/lock1.
func setupEnum(t *testing.T) *FSM {
	t.Helper()
	f := New(nil)
	cmds := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		acquire("/alice/lock1", "a"),               // a holds (token 1)
		acquire("/alice/lock1", "b"),               // b queued behind a (seq 2)
		acquire("/bob/lock", "b"),                  // b holds (token 3)
		campaign("/alice/elect", "a", []byte("v")), // a leads (token 4), value "v"
	}
	if _, err := applyAll(f, cmds); err != nil {
		t.Fatalf("setupEnum: applyAll: %s", err)
	}
	return f
}

// TestListLocks proves ListLocksQuery enumerates held locks/elections sorted by key,
// flags HasValue for elections, reports queue depth, and honors a path-boundary prefix.
func TestListLocks(t *testing.T) {
	f := setupEnum(t)

	tests := []struct {
		name   string
		prefix string
		want   []LockSummary
	}{
		{
			name:   "Success: empty prefix returns all held keys sorted",
			prefix: "",
			want: []LockSummary{
				{Name: "/alice/elect", Held: true, Holder: "a", Token: 4, HasValue: true},
				{Name: "/alice/lock1", Held: true, Holder: "a", Token: 1, QueueDepth: 1},
				{Name: "/bob/lock", Held: true, Holder: "b", Token: 3},
			},
		},
		{
			name:   "Success: prefix narrows to a namespace",
			prefix: "/alice",
			want: []LockSummary{
				{Name: "/alice/elect", Held: true, Holder: "a", Token: 4, HasValue: true},
				{Name: "/alice/lock1", Held: true, Holder: "a", Token: 1, QueueDepth: 1},
			},
		},
		{
			name:   "Success: prefix matches on a path boundary, not a substring",
			prefix: "/alice/lock",
			want:   nil, // "/alice/lock1" is a sibling of prefix "/alice/lock", not under it
		},
	}

	for _, test := range tests {
		got, err := f.Query(ListLocksQuery{Prefix: test.prefix})
		if err != nil {
			t.Errorf("TestListLocks(%s): got err == %s, want err == nil", test.name, err)
			continue
		}
		ls := got.(LockSummaries)
		if diff := pretty.Compare(test.want, ls.Locks); diff != "" {
			t.Errorf("TestListLocks(%s): -want +got:\n%s", test.name, diff)
		}
	}
}

// TestWaiters proves WaitersQuery returns the holder plus the FIFO contenders with
// 1-based positions, and an empty result for an unheld key.
func TestWaiters(t *testing.T) {
	f := setupEnum(t)

	tests := []struct {
		name    string
		key     string
		wantHld bool
		holder  string
		entries []WaiterInfo
	}{
		{
			name:    "Success: held key reports holder and queued contenders",
			key:     "/alice/lock1",
			wantHld: true,
			holder:  "a",
			entries: []WaiterInfo{{ClientID: "b", Seq: 2, Position: 1}},
		},
		{
			name:    "Success: unheld key reports not held with no contenders",
			key:     "/nobody/here",
			wantHld: false,
		},
	}

	for _, test := range tests {
		got, err := f.Query(WaitersQuery{Name: test.key})
		if err != nil {
			t.Errorf("TestWaiters(%s): got err == %s, want err == nil", test.name, err)
			continue
		}
		w := got.(Waiters)
		switch {
		case w.Held != test.wantHld:
			t.Errorf("TestWaiters(%s): Held = %v, want %v", test.name, w.Held, test.wantHld)
		case w.Held && w.Holder != test.holder:
			t.Errorf("TestWaiters(%s): Holder = %q, want %q", test.name, w.Holder, test.holder)
		}
		if diff := pretty.Compare(test.entries, w.Entries); diff != "" {
			t.Errorf("TestWaiters(%s): entries -want +got:\n%s", test.name, diff)
		}
	}
}
