package fsm

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kylelemons/godebug/pretty"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/zuul/internal/raft/fsm/fsmpb"
)

// Clock anchors shared across tests. now0 is an arbitrary apply-time; deadline0 is
// now0 + ttl. Times are leader-stamped, so they are just data here.
const (
	now0      = int64(1_000_000_000)
	ttlMS     = int64(30_000)
	deadline0 = now0 + ttlMS*1_000_000
)

// Command constructors — keep the tables readable.

func leaseGrant(client string, ttl, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseGrant{LeaseGrant: &fsmpb.LeaseGrant{ClientId: client, TtlMs: ttl, NowUnixNano: now}}}
}

func keepAlive(client string, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseKeepAlive{LeaseKeepAlive: &fsmpb.LeaseKeepAlive{ClientId: client, NowUnixNano: now}}}
}

func revoke(client string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseRevoke{LeaseRevoke: &fsmpb.LeaseRevoke{ClientId: client}}}
}

func expire(client string, now int64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_LeaseExpire{LeaseExpire: &fsmpb.LeaseExpire{ClientId: client, NowUnixNano: now}}}
}

func acquire(name, client string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_AcquireLock{AcquireLock: &fsmpb.AcquireLock{Name: name, ClientId: client}}}
}

func tryAcquire(name, client string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_AcquireLock{AcquireLock: &fsmpb.AcquireLock{Name: name, ClientId: client, TryLock: true}}}
}

func release(name, client string, token uint64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_ReleaseLock{ReleaseLock: &fsmpb.ReleaseLock{Name: name, ClientId: client, FencingToken: token}}}
}

func cancelWait(name, client string) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_CancelWait{CancelWait: &fsmpb.CancelWait{Name: name, ClientId: client}}}
}

func campaign(name, client string, value []byte) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Campaign{Campaign: &fsmpb.Campaign{Name: name, ClientId: client, Value: value}}}
}

func proclaim(name, client string, token uint64, value []byte) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Proclaim{Proclaim: &fsmpb.Proclaim{Name: name, ClientId: client, FencingToken: token, Value: value}}}
}

func resign(name, client string, token uint64) *fsmpb.Command {
	return &fsmpb.Command{Cmd: &fsmpb.Command_Resign{Resign: &fsmpb.Resign{Name: name, ClientId: client, FencingToken: token}}}
}

// applyAll applies every command in order and returns the last result and error.
func applyAll(f *FSM, cmds []*fsmpb.Command) (*fsmpb.CommandResult, error) {
	var (
		last *fsmpb.CommandResult
		err  error
	)
	for _, c := range cmds {
		last, err = f.Apply(c)
		if err != nil {
			return last, err
		}
	}
	return last, err
}

// TestApply covers the lock/lease command paths through Apply, asserting the
// result of the final command. revision is deterministic (one bump per command),
// so it is filled in from the command count rather than hand-written per case.
func TestApply(t *testing.T) {
	tests := []struct {
		name    string
		cmds    []*fsmpb.Command
		want    *fsmpb.CommandResult
		wantErr bool
	}{
		{
			name:    "Error: nil command",
			cmds:    []*fsmpb.Command{nil},
			wantErr: true,
		},
		{
			name:    "Error: command with no inner command set",
			cmds:    []*fsmpb.Command{{}},
			wantErr: true,
		},
		{
			name: "Success: lease grant returns the computed deadline",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_LEASE_GRANTED, LeaseDeadlineUnixNano: deadline0},
		},
		{
			name: "Success: keepalive renews an existing lease",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), keepAlive("a", now0+5)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_LEASE_RENEWED, LeaseDeadlineUnixNano: now0 + 5 + ttlMS*1_000_000},
		},
		{
			name: "Success: keepalive on a missing lease is NOT_FOUND",
			cmds: []*fsmpb.Command{keepAlive("ghost", now0)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NOT_FOUND},
		},
		{
			name: "Error: acquire without a lease is rejected (domain outcome, not a Go error)",
			cmds: []*fsmpb.Command{acquire("L", "a")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NO_LEASE, LockKey: "L"},
		},
		{
			name: "Success: acquire a free lock grants fencing token 1",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), acquire("L", "a")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_GRANTED, LockKey: "L", FencingToken: 1},
		},
		{
			name: "Success: re-acquire by the holder is an idempotent grant with the same token",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), acquire("L", "a"), acquire("L", "a")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_GRANTED, LockKey: "L", FencingToken: 1},
		},
		{
			name: "Success: blocking acquire of a held lock enqueues at position 1",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), acquire("L", "a"), acquire("L", "b")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_QUEUED, LockKey: "L", QueuePosition: 1},
		},
		{
			name: "Success: re-enqueue of an already-queued waiter is idempotent at its position",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), acquire("L", "a"), acquire("L", "b"), acquire("L", "b")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_QUEUED, LockKey: "L", QueuePosition: 1},
		},
		{
			name: "Success: try-lock on a held lock fails fast and names the holder",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), acquire("L", "a"), tryAcquire("L", "b")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NOT_ACQUIRED, LockKey: "L", CurrentHolder: "a"},
		},
		{
			name: "Success: try-lock on a free lock acquires it",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), tryAcquire("L", "a")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_GRANTED, LockKey: "L", FencingToken: 1},
		},
		{
			name: "Success: release by the holder with the right token succeeds",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), acquire("L", "a"), release("L", "a", 1)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_RELEASED, LockKey: "L"},
		},
		{
			name: "Success: release with a stale token is rejected",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), acquire("L", "a"), release("L", "a", 999)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_STALE_TOKEN, LockKey: "L"},
		},
		{
			name: "Success: release by a non-holder is rejected",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), acquire("L", "a"), release("L", "b", 1)},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NOT_HOLDER, LockKey: "L"},
		},
		{
			name: "Success: cancel-wait dequeues a waiter",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), leaseGrant("b", ttlMS, now0), acquire("L", "a"), acquire("L", "b"), cancelWait("L", "b")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_RELEASED, LockKey: "L"},
		},
		{
			name: "Success: cancel-wait for a client that is not queued is a no-op",
			cmds: []*fsmpb.Command{leaseGrant("a", ttlMS, now0), acquire("L", "a"), cancelWait("L", "b")},
			want: &fsmpb.CommandResult{Outcome: fsmpb.Outcome_OUTCOME_NOOP, LockKey: "L"},
		},
	}

	for _, test := range tests {
		f := New(nil)
		got, err := applyAll(f, test.cmds)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestApply(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestApply(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		if test.want != nil {
			test.want.Revision = uint64(len(test.cmds))
		}
		if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
			t.Errorf("TestApply(%s): result: -want +got:\n%s", test.name, diff)
		}
	}
}

// TestFIFOPromotion drives a multi-waiter contention scenario and checks, via
// Status reads, that ownership passes in strict arrival order with strictly
// increasing fencing tokens — the core mutual-exclusion + fairness property.
func TestFIFOPromotion(t *testing.T) {
	f := New(nil)
	setup := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		leaseGrant("c", ttlMS, now0),
		acquire("L", "a"), // a holds, token 1
		acquire("L", "b"), // b queued (pos 1)
		acquire("L", "c"), // c queued (pos 2)
	}
	if _, err := applyAll(f, setup); err != nil {
		t.Fatalf("TestFIFOPromotion: setup: got err == %s, want err == nil", err)
	}

	want := []struct {
		name   string
		status Status
	}{
		{
			name:   "a holds with two waiters",
			status: Status{Held: true, Holder: "a", Token: 1, QueueDepth: 2, Revision: 6},
		},
	}
	for _, w := range want {
		got, err := f.Query(StatusQuery{Name: "L"})
		if err != nil {
			t.Fatalf("TestFIFOPromotion(%s): Query: got err == %s, want err == nil", w.name, err)
		}
		if diff := pretty.Compare(w.status, got); diff != "" {
			t.Errorf("TestFIFOPromotion(%s): -want +got:\n%s", w.name, diff)
		}
	}

	// a releases -> b promoted with token 4 (seq: 1 grant, 2/3 enqueues, 4 promote).
	if _, err := f.Apply(release("L", "a", 1)); err != nil {
		t.Fatalf("TestFIFOPromotion: release a: got err == %s, want err == nil", err)
	}
	gotB, _ := f.Query(StatusQuery{Name: "L"})
	wantB := Status{Held: true, Holder: "b", Token: 4, QueueDepth: 1, Revision: 7}
	if diff := pretty.Compare(wantB, gotB); diff != "" {
		t.Errorf("TestFIFOPromotion(b promoted): -want +got:\n%s", diff)
	}

	// b releases -> c promoted with token 5 (> 4: monotonic).
	if _, err := f.Apply(release("L", "b", 4)); err != nil {
		t.Fatalf("TestFIFOPromotion: release b: got err == %s, want err == nil", err)
	}
	gotC, _ := f.Query(StatusQuery{Name: "L"})
	wantC := Status{Held: true, Holder: "c", Token: 5, QueueDepth: 0, Revision: 8}
	if diff := pretty.Compare(wantC, gotC); diff != "" {
		t.Errorf("TestFIFOPromotion(c promoted): -want +got:\n%s", diff)
	}

	// c releases -> lock is now free and removed from state.
	if _, err := f.Apply(release("L", "c", 5)); err != nil {
		t.Fatalf("TestFIFOPromotion: release c: got err == %s, want err == nil", err)
	}
	gotFree, _ := f.Query(StatusQuery{Name: "L"})
	wantFree := Status{Revision: 9}
	if diff := pretty.Compare(wantFree, gotFree); diff != "" {
		t.Errorf("TestFIFOPromotion(free): -want +got:\n%s", diff)
	}
}

// TestLeaseExpiryReleasesLocks proves a lease drop (expiry / revoke) releases every
// lock the client held and promotes the next waiter, and that an expire whose
// deadline was pushed out by a later keepalive is a no-op.
func TestLeaseExpiryReleasesLocks(t *testing.T) {
	tests := []struct {
		name       string
		drop       *fsmpb.Command
		wantStatus Status // status of "L" after the drop
		wantOutQ   LeaseInfo
	}{
		{
			name:       "Success: expiry past the deadline releases the held lock to the waiter",
			drop:       expire("a", deadline0+1),
			wantStatus: Status{Held: true, Holder: "b", Token: 3, QueueDepth: 0, Revision: 5},
			wantOutQ:   LeaseInfo{Exists: true, TTLMillis: ttlMS, ExpireAtUnixNano: deadline0, HeldKeys: []string{"L"}, Revision: 5},
		},
		{
			name:       "Success: revoke releases the held lock to the waiter",
			drop:       revoke("a"),
			wantStatus: Status{Held: true, Holder: "b", Token: 3, QueueDepth: 0, Revision: 5},
			wantOutQ:   LeaseInfo{Exists: true, TTLMillis: ttlMS, ExpireAtUnixNano: deadline0, HeldKeys: []string{"L"}, Revision: 5},
		},
		{
			name:       "Success: expire before the deadline is a no-op and keeps the holder",
			drop:       expire("a", deadline0-1),
			wantStatus: Status{Held: true, Holder: "a", Token: 1, QueueDepth: 1, Revision: 5},
			wantOutQ:   LeaseInfo{Exists: true, TTLMillis: ttlMS, ExpireAtUnixNano: deadline0, Revision: 5},
		},
	}

	for _, test := range tests {
		f := New(nil)
		setup := []*fsmpb.Command{
			leaseGrant("a", ttlMS, now0),
			leaseGrant("b", ttlMS, now0),
			acquire("L", "a"), // a holds, token 1
			acquire("L", "b"), // b queued, seq 2
		}
		if _, err := applyAll(f, setup); err != nil {
			t.Fatalf("TestLeaseExpiryReleasesLocks(%s): setup: got err == %s, want err == nil", test.name, err)
		}
		if _, err := f.Apply(test.drop); err != nil {
			t.Fatalf("TestLeaseExpiryReleasesLocks(%s): drop: got err == %s, want err == nil", test.name, err)
		}

		gotStatus, _ := f.Query(StatusQuery{Name: "L"})
		if diff := pretty.Compare(test.wantStatus, gotStatus); diff != "" {
			t.Errorf("TestLeaseExpiryReleasesLocks(%s): status: -want +got:\n%s", test.name, diff)
		}
		gotLease, _ := f.Query(LeaseQuery{ClientID: "b"})
		if diff := pretty.Compare(test.wantOutQ, gotLease); diff != "" {
			t.Errorf("TestLeaseExpiryReleasesLocks(%s): waiter lease: -want +got:\n%s", test.name, diff)
		}
	}
}

// TestLeaseExpiryDequeuesWaiter proves that expiring a client who is only *waiting*
// (not holding) removes it from the queue without disturbing the holder.
func TestLeaseExpiryDequeuesWaiter(t *testing.T) {
	f := New(nil)
	setup := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		acquire("L", "a"), // a holds
		acquire("L", "b"), // b waits
		expire("b", deadline0+1),
	}
	if _, err := applyAll(f, setup); err != nil {
		t.Fatalf("TestLeaseExpiryDequeuesWaiter: setup: got err == %s, want err == nil", err)
	}

	got, _ := f.Query(StatusQuery{Name: "L"})
	want := Status{Held: true, Holder: "a", Token: 1, QueueDepth: 0, Revision: 5}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestLeaseExpiryDequeuesWaiter: -want +got:\n%s", diff)
	}
}

// TestLeaseDropReleasesMultipleLocks exercises the multi-key release path, where a
// single dropped lease hands several held locks to their waiters. The held keys are
// promoted in sorted order (L1 then L2), so the fencing tokens are assigned
// deterministically — the property that keeps replicas byte-identical.
func TestLeaseDropReleasesMultipleLocks(t *testing.T) {
	f := New(nil)
	setup := []*fsmpb.Command{
		leaseGrant("a", ttlMS, now0),
		leaseGrant("b", ttlMS, now0),
		acquire("L1", "a"), // seq 1, a holds L1
		acquire("L2", "a"), // seq 2, a holds L2
		acquire("L1", "b"), // seq 3, b waits L1
		acquire("L2", "b"), // seq 4, b waits L2
		revoke("a"),        // drop: promote L1 (seq 5), then L2 (seq 6)
	}
	if _, err := applyAll(f, setup); err != nil {
		t.Fatalf("TestLeaseDropReleasesMultipleLocks: setup: got err == %s, want err == nil", err)
	}

	checks := []struct {
		key  string
		want Status
	}{
		{key: "L1", want: Status{Held: true, Holder: "b", Token: 5, QueueDepth: 0, Revision: 7}},
		{key: "L2", want: Status{Held: true, Holder: "b", Token: 6, QueueDepth: 0, Revision: 7}},
	}
	for _, c := range checks {
		got, _ := f.Query(StatusQuery{Name: c.key})
		if diff := pretty.Compare(c.want, got); diff != "" {
			t.Errorf("TestLeaseDropReleasesMultipleLocks(%s): -want +got:\n%s", c.key, diff)
		}
	}

	gotLease, _ := f.Query(LeaseQuery{ClientID: "b"})
	wantLease := LeaseInfo{Exists: true, TTLMillis: ttlMS, ExpireAtUnixNano: deadline0, HeldKeys: []string{"L1", "L2"}, Revision: 7}
	if diff := pretty.Compare(wantLease, gotLease); diff != "" {
		t.Errorf("TestLeaseDropReleasesMultipleLocks(b lease): -want +got:\n%s", diff)
	}
}
