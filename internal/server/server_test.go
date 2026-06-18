package server

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/raft/fsm"
	"github.com/johnsiilver/zuul/internal/raft/fsm/fsmpb"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// fakeReader is a Reader whose Read/StaleRead return a fixed status, used to stand in
// for the local node's FSM read path.
type fakeReader struct {
	status fsm.Status
}

func (f fakeReader) Read(ctx context.Context, shardID uint64, query any) (any, error) {
	return f.status, nil
}

func (f fakeReader) StaleRead(shardID uint64, query any) (any, error) {
	return f.status, nil
}

// ctxReader is a Reader that honors context cancellation: it serves the fixed status
// only while ctx is live and returns ctx.Err() once it is done. It proves the bounded
// wait reserves a grace window before the RPC deadline, so the confirm read runs on a
// live ctx instead of a cancelled one.
type ctxReader struct {
	status fsm.Status
}

func (r ctxReader) Read(ctx context.Context, shardID uint64, query any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.status, nil
}

func (r ctxReader) StaleRead(shardID uint64, query any) (any, error) {
	return r.status, nil
}

// fakeProposer is a Proposer that records the last command it received and returns an
// empty (NOOP) marshalled CommandResult, standing in for the forward dispatcher in the
// cancel-wait path.
type fakeProposer struct {
	called bool
}

func (f *fakeProposer) Propose(ctx context.Context, shardID uint64, c []byte) ([]byte, error) {
	f.called = true
	return (&fsmpb.CommandResult{}).MarshalVT()
}

// TestWaitForLockCoalescedGrant is a regression test for the hub-coalescing miss-grant:
// the watch hub keeps only the latest event per key, so a waiter's own promotion can be
// overwritten by a later event for the same key before the waiter reads it. waitForLock
// must still report the lock acquired by confirming holding against an authoritative
// read, rather than blocking until its wait deadline. Before the fix this returned
// Acquired:false; after the fix it returns Acquired:true.
func TestWaitForLockCoalescedGrant(t *testing.T) {
	const (
		key      = "/alice/lock"
		clientID = "client-1"
		shardID  = uint64(1)
	)

	hub := watch.New()
	sub := hub.Subscribe(watch.SubArgs{Key: key})
	defer sub.Close()

	// The authoritative FSM read reports this client as the current holder: it WAS
	// granted, even though the event it would have seen got coalesced away.
	reader := fakeReader{status: fsm.Status{Held: true, Holder: clientID, Token: 7, Revision: 9}}
	s := &Server{cfg: Config{Reader: reader, Hub: hub}}

	// Simulate coalescing: the promotion event (Holder == clientID) is overwritten by a
	// later event for the same key, so the only event the waiter can read names a
	// different holder.
	hub.Notify(fsm.Event{Key: key, Holder: "someone-else", Revision: 8})

	req := &zuulv1.LockRequest{
		Name:                 key,
		ClientId:             clientID,
		WaitDeadlineUnixNano: time.Now().Add(2 * time.Second).UnixNano(),
	}

	resp, err := s.waitForLock(t.Context(), req, sub, shardID)
	switch {
	case err != nil:
		t.Fatalf("TestWaitForLockCoalescedGrant: got err == %s, want err == nil", err)
	case !resp.GetAcquired():
		t.Fatalf("TestWaitForLockCoalescedGrant: got Acquired == false, want true (coalesced promotion was missed)")
	case resp.GetFencingToken() != 7:
		t.Errorf("TestWaitForLockCoalescedGrant: got FencingToken == %d, want 7", resp.GetFencingToken())
	case resp.GetRevision() != 9:
		t.Errorf("TestWaitForLockCoalescedGrant: got Revision == %d, want 9", resp.GetRevision())
	}
}

// TestWaitForLockTimeoutGrant is a regression test for the bounded-wait timeout path of
// the same hub-coalescing miss-grant: here the waiter's promotion event is coalesced away
// entirely (no matching event is ever delivered), so sub.Next returns the wait-deadline
// error before any event arrives. Because the FSM cancel-wait never releases a holder,
// reporting Acquired:false while we are in fact the granted holder would strand the lock
// until our lease lapses. waitForLock must confirm against an authoritative read before
// giving up. Before the fix this returned Acquired:false (and proposed a useless cancel);
// after the fix it returns Acquired:true without cancelling.
func TestWaitForLockTimeoutGrant(t *testing.T) {
	const (
		key      = "/alice/lock"
		clientID = "client-1"
		shardID  = uint64(1)
	)

	hub := watch.New()
	sub := hub.Subscribe(watch.SubArgs{Key: key})
	defer sub.Close()

	// The authoritative FSM read reports this client as the current holder: it WAS
	// granted, even though its promotion event was coalesced away before it could read it.
	reader := fakeReader{status: fsm.Status{Held: true, Holder: clientID, Token: 7, Revision: 9}}
	prop := &fakeProposer{}
	s := &Server{cfg: Config{Reader: reader, Hub: hub, Proposer: prop}}

	// No event is delivered for the key, so sub.Next blocks until the short wait deadline
	// fires — exercising the timeout branch rather than the per-event loop body.
	req := &zuulv1.LockRequest{
		Name:                 key,
		ClientId:             clientID,
		WaitDeadlineUnixNano: time.Now().Add(100 * time.Millisecond).UnixNano(),
	}

	resp, err := s.waitForLock(t.Context(), req, sub, shardID)
	switch {
	case err != nil:
		t.Fatalf("TestWaitForLockTimeoutGrant: got err == %s, want err == nil", err)
	case !resp.GetAcquired():
		t.Fatalf("TestWaitForLockTimeoutGrant: got Acquired == false, want true (granted holder stranded on wait timeout)")
	case resp.GetFencingToken() != 7:
		t.Errorf("TestWaitForLockTimeoutGrant: got FencingToken == %d, want 7", resp.GetFencingToken())
	case resp.GetRevision() != 9:
		t.Errorf("TestWaitForLockTimeoutGrant: got Revision == %d, want 9", resp.GetRevision())
	case prop.called:
		t.Errorf("TestWaitForLockTimeoutGrant: got cancel-wait proposed == true, want false (must not cancel a lock it actually holds)")
	}
}

// TestWaitForLockGraceWindow proves the grace window the bounded wait reserves: the client
// derives the RPC deadline from the same instant it puts in WaitDeadlineUnixNano, so the
// parent ctx here carries that deadline. The confirm read honors cancellation (ctxReader),
// so it can only return Acquired:true if it runs before the RPC deadline — i.e. inside the
// reserved grace window. Without the grace, the internal wait and the RPC deadline fire
// together and the confirm would read on a cancelled ctx and fail.
func TestWaitForLockGraceWindow(t *testing.T) {
	const (
		key      = "/alice/lock"
		clientID = "client-1"
		shardID  = uint64(1)
	)

	hub := watch.New()
	sub := hub.Subscribe(watch.SubArgs{Key: key})
	defer sub.Close()

	reader := ctxReader{status: fsm.Status{Held: true, Holder: clientID, Token: 7, Revision: 9}}
	prop := &fakeProposer{}
	s := &Server{cfg: Config{Reader: reader, Hub: hub, Proposer: prop}}

	// The RPC ctx deadline coincides with the requested wait deadline, as it does on the
	// wire: the client sets both from the same instant.
	deadline := time.Now().Add(300 * time.Millisecond)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	req := &zuulv1.LockRequest{
		Name:                 key,
		ClientId:             clientID,
		WaitDeadlineUnixNano: deadline.UnixNano(),
	}

	resp, err := s.waitForLock(ctx, req, sub, shardID)
	switch {
	case err != nil:
		t.Fatalf("TestWaitForLockGraceWindow: got err == %s, want err == nil (confirm read raced the RPC deadline)", err)
	case !resp.GetAcquired():
		t.Fatalf("TestWaitForLockGraceWindow: got Acquired == false, want true (grace window must let the confirm read run on a live ctx)")
	case resp.GetFencingToken() != 7:
		t.Errorf("TestWaitForLockGraceWindow: got FencingToken == %d, want 7", resp.GetFencingToken())
	}
}

// TestWaitForLeadershipTimeoutGrant is the election counterpart of
// TestWaitForLockTimeoutGrant: a candidate's promotion to leader is coalesced away, so
// sub.Next returns the wait-deadline error before any matching event arrives. cancelWait
// never demotes a leader, so reporting Leadership:false while we are in fact the leader
// would strand it until our lease lapses. waitForLeadership must confirm against an
// authoritative read before giving up.
func TestWaitForLeadershipTimeoutGrant(t *testing.T) {
	const (
		key      = "/alice/leader"
		clientID = "client-1"
		shardID  = uint64(1)
	)

	hub := watch.New()
	sub := hub.Subscribe(watch.SubArgs{Key: key})
	defer sub.Close()

	reader := fakeReader{status: fsm.Status{Held: true, Holder: clientID, Token: 7, Revision: 9}}
	prop := &fakeProposer{}
	s := &Server{cfg: Config{Reader: reader, Hub: hub, Proposer: prop}}

	req := &zuulv1.CampaignRequest{
		Name:                 key,
		ClientId:             clientID,
		WaitDeadlineUnixNano: time.Now().Add(100 * time.Millisecond).UnixNano(),
	}

	resp, err := s.waitForLeadership(t.Context(), req, sub, shardID)
	switch {
	case err != nil:
		t.Fatalf("TestWaitForLeadershipTimeoutGrant: got err == %s, want err == nil", err)
	case !resp.GetLeadership():
		t.Fatalf("TestWaitForLeadershipTimeoutGrant: got Leadership == false, want true (granted leader stranded on wait timeout)")
	case resp.GetFencingToken() != 7:
		t.Errorf("TestWaitForLeadershipTimeoutGrant: got FencingToken == %d, want 7", resp.GetFencingToken())
	case resp.GetRevision() != 9:
		t.Errorf("TestWaitForLeadershipTimeoutGrant: got Revision == %d, want 9", resp.GetRevision())
	case prop.called:
		t.Errorf("TestWaitForLeadershipTimeoutGrant: got cancel-wait proposed == true, want false (must not cancel leadership it actually holds)")
	}
}
