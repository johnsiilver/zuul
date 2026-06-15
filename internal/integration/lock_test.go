package integration

import (
	"testing"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestSingleNodeLifecycle is a sanity check that the multi-shard stack behaves
// correctly with one node and one shard: acquire, contend, fenced unlock, re-acquire.
func TestSingleNodeLifecycle(t *testing.T) {
	c := newCluster(t, 1, 1)
	ctx := t.Context()
	n := c.nodes[0]
	openSession(n, "a")
	openSession(n, "b")

	got, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/L", ClientId: "a"})
	if err != nil {
		t.Fatalf("TestSingleNodeLifecycle: TryLock a: got err == %s, want err == nil", err)
	}
	if !got.GetAcquired() || got.GetFencingToken() != 1 {
		t.Fatalf("TestSingleNodeLifecycle: TryLock a: got acquired=%v token=%d, want true/1", got.GetAcquired(), got.GetFencingToken())
	}

	deny, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/L", ClientId: "b"})
	if err != nil {
		t.Fatalf("TestSingleNodeLifecycle: TryLock b: got err == %s, want err == nil", err)
	}
	if deny.GetAcquired() || deny.GetCurrentHolder() != "a" {
		t.Errorf("TestSingleNodeLifecycle: TryLock b: got acquired=%v holder=%q, want false/a", deny.GetAcquired(), deny.GetCurrentHolder())
	}

	if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: "/test/L", ClientId: "a", FencingToken: 999}); err == nil {
		t.Errorf("TestSingleNodeLifecycle: Unlock stale token: got err == nil, want err != nil")
	}
	if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: "/test/L", ClientId: "a", FencingToken: 1}); err != nil {
		t.Fatalf("TestSingleNodeLifecycle: Unlock a: got err == %s, want err == nil", err)
	}

	reacq, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/L", ClientId: "b"})
	if err != nil {
		t.Fatalf("TestSingleNodeLifecycle: re-acquire b: got err == %s, want err == nil", err)
	}
	if !reacq.GetAcquired() || reacq.GetFencingToken() <= 1 {
		t.Errorf("TestSingleNodeLifecycle: re-acquire b: got acquired=%v token=%d, want true/>1", reacq.GetAcquired(), reacq.GetFencingToken())
	}
}

// TestForwardedLock proves a write submitted to a node that does not lead the key's
// shard is transparently forwarded to the leader and succeeds, and that the lock is
// then visible from another node's linearizable read.
func TestForwardedLock(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	const key = "/test/forwarded-key"
	shardID := c.router.Shard(key)
	leaderID := c.leaderReplica(t, shardID)
	client := c.otherNode(leaderID) // a node that is NOT the shard leader
	openSession(client, "c1")

	got, err := client.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "c1"})
	if err != nil {
		t.Fatalf("TestForwardedLock: TryLock via non-leader node %d: got err == %s, want err == nil", client.replicaID, err)
	}
	if !got.GetAcquired() {
		t.Fatalf("TestForwardedLock: got acquired=false, want true (forwarded acquire)")
	}

	// Read from a third node (the leader itself) — the lock is replicated.
	reader := c.nodeByReplica(leaderID)
	st, err := reader.srv.Status(ctx, &zuulv1.StatusRequest{Name: key})
	if err != nil {
		t.Fatalf("TestForwardedLock: Status via leader node: got err == %s, want err == nil", err)
	}
	if !st.GetHeld() || st.GetHolderClientId() != "c1" {
		t.Errorf("TestForwardedLock: Status: got held=%v holder=%q, want true/c1", st.GetHeld(), st.GetHolderClientId())
	}
}

// TestFencingAcrossNodes proves a lock acquired through one node and released
// through another (both forwarded as needed) re-issues a strictly larger fencing
// token on the next acquisition — fencing holds regardless of entry node.
func TestFencingAcrossNodes(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	const key = "/test/fence-key"
	nodeA := c.nodes[0]
	nodeB := c.nodes[1]
	openSession(nodeA, "a")
	openSession(nodeB, "a")
	openSession(nodeB, "b")

	first, err := nodeA.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "a"})
	if err != nil {
		t.Fatalf("TestFencingAcrossNodes: TryLock via A: got err == %s, want err == nil", err)
	}

	if _, err := nodeB.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: "a", FencingToken: first.GetFencingToken()}); err != nil {
		t.Fatalf("TestFencingAcrossNodes: Unlock via B: got err == %s, want err == nil", err)
	}

	second, err := nodeB.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "b"})
	if err != nil {
		t.Fatalf("TestFencingAcrossNodes: re-acquire via B: got err == %s, want err == nil", err)
	}
	if !second.GetAcquired() || second.GetFencingToken() <= first.GetFencingToken() {
		t.Errorf("TestFencingAcrossNodes: got acquired=%v token=%d, want true and > %d", second.GetAcquired(), second.GetFencingToken(), first.GetFencingToken())
	}
}

// TestFailoverKeepsLock proves a held lock survives the loss of its shard's leader:
// after the leader node is stopped and the survivors re-elect, the lock is still
// held by the same client with the same fencing token, and a different client
// cannot steal it.
func TestFailoverKeepsLock(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	const key = "/test/failover-key"
	shardID := c.router.Shard(key)
	leaderID := c.leaderReplica(t, shardID)
	client := c.otherNode(leaderID) // survives the failover; the client talks here
	openSession(client, "holder")
	openSession(client, "thief")

	held, err := client.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "holder"})
	if err != nil {
		t.Fatalf("TestFailoverKeepsLock: acquire: got err == %s, want err == nil", err)
	}
	if !held.GetAcquired() {
		t.Fatalf("TestFailoverKeepsLock: acquire: got acquired=false, want true")
	}

	// Kill the shard leader and wait for the survivors to re-elect.
	c.nodeByReplica(leaderID).host.Close()
	awaitLeaderChange(t, client, shardID, leaderID)

	st := awaitStatus(t, client, key)
	if !st.GetHeld() || st.GetHolderClientId() != "holder" || st.GetFencingToken() != held.GetFencingToken() {
		t.Errorf("TestFailoverKeepsLock: Status: got held=%v holder=%q token=%d, want true/holder/%d", st.GetHeld(), st.GetHolderClientId(), st.GetFencingToken(), held.GetFencingToken())
	}

	thief, err := client.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "thief"})
	if err != nil {
		t.Fatalf("TestFailoverKeepsLock: thief TryLock: got err == %s, want err == nil", err)
	}
	if thief.GetAcquired() {
		t.Errorf("TestFailoverKeepsLock: thief acquired a held lock after failover, want denied")
	}
}
