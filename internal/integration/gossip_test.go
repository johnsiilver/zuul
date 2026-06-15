package integration

import (
	"fmt"
	"testing"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestGossipAddressing proves a cluster addressed by NodeHostID over gossip (which
// requires mutual TLS) works end to end: peers find each other by gossip, a write
// forwards to the shard leader, and every node is recorded in the meta shard with
// its NodeHostID. Gossip resolves only the Raft transport address — the forward
// plane still uses the gRPC address from the meta shard.
func TestGossipAddressing(t *testing.T) {
	certs := genCerts(t)
	c := newGossipCluster(t, 3, 4, certs)
	ctx := t.Context()

	// A forwarded write succeeds (the entry node reaches the leader's gRPC address
	// from the meta shard, while Raft replication uses gossip-resolved addresses).
	const key = "/test/gossip-key"
	leaderID := c.leaderReplica(t, c.router.Shard(key))
	entry := c.otherNode(leaderID)
	openSession(entry, "g1")
	got, err := entry.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "g1"})
	if err != nil {
		t.Fatalf("TestGossipAddressing: forwarded TryLock over gossip: %s", err)
	}
	if !got.GetAcquired() {
		t.Fatalf("TestGossipAddressing: TryLock acquired=false, want true")
	}

	// Every member is in the meta shard, tagged with its NodeHostID.
	resp, err := c.nodes[0].cluster.Members(ctx, &zuulv1.MembersRequest{})
	if err != nil {
		t.Fatalf("TestGossipAddressing: Members: %s", err)
	}
	if len(resp.GetMembers()) != 3 {
		t.Fatalf("TestGossipAddressing: got %d members, want 3", len(resp.GetMembers()))
	}
	for _, m := range resp.GetMembers() {
		want := fmt.Sprintf("nhid-%d", m.GetReplicaId())
		if m.GetNodeHostId() != want {
			t.Errorf("TestGossipAddressing: replica %d NodeHostId = %q, want %q", m.GetReplicaId(), m.GetNodeHostId(), want)
		}
	}
}

// TestGossipInsecure proves gossip works without mutual TLS — security is the
// operator's choice (a warning is logged, but it is not forced).
func TestGossipInsecure(t *testing.T) {
	c := newGossipCluster(t, 3, 4, nil) // no certs: insecure gossip + Raft
	ctx := t.Context()

	const key = "/test/gossip-insecure-key"
	leaderID := c.leaderReplica(t, c.router.Shard(key))
	entry := c.otherNode(leaderID)
	openSession(entry, "g1")
	got, err := entry.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "g1"})
	if err != nil {
		t.Fatalf("TestGossipInsecure: TryLock over insecure gossip: %s", err)
	}
	if !got.GetAcquired() {
		t.Fatalf("TestGossipInsecure: TryLock acquired=false, want true")
	}
}
