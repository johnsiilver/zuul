package integration

import (
	"sort"
	"testing"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestClusterMembers proves the meta shard is populated on boot and the Cluster
// Members API returns every node with its addresses, read from any node.
func TestClusterMembers(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	resp, err := c.nodes[0].cluster.Members(ctx, &zuulv1.MembersRequest{})
	if err != nil {
		t.Fatalf("TestClusterMembers: Members: got err == %s, want err == nil", err)
	}
	if len(resp.GetMembers()) != 3 {
		t.Fatalf("TestClusterMembers: got %d members, want 3", len(resp.GetMembers()))
	}

	ids := []uint64{}
	for _, m := range resp.GetMembers() {
		ids = append(ids, m.GetReplicaId())
		if m.GetZuulGrpcAddress() == "" || m.GetRaftAddress() == "" {
			t.Errorf("TestClusterMembers: replica %d has empty addresses: grpc=%q raft=%q", m.GetReplicaId(), m.GetZuulGrpcAddress(), m.GetRaftAddress())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		if id != uint64(i+1) {
			t.Errorf("TestClusterMembers: member ids = %v, want [1 2 3]", ids)
			break
		}
	}
}

// TestClusterHealthAndShards proves Shards reports a leader for every hosted shard
// (including the meta shard) and Health counts shards led by the node.
func TestClusterHealthAndShards(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	// Wait for every hosted shard to have a leader before reading.
	for _, sh := range append([]uint64{metaShardID}, c.shards...) {
		c.leaderReplica(t, sh)
	}

	shardsResp, err := c.nodes[0].cluster.Shards(ctx, &zuulv1.ShardsRequest{})
	if err != nil {
		t.Fatalf("TestClusterHealthAndShards: Shards: got err == %s, want err == nil", err)
	}
	// 4 lock shards + 1 meta shard.
	if len(shardsResp.GetShards()) != 5 {
		t.Fatalf("TestClusterHealthAndShards: got %d shards, want 5", len(shardsResp.GetShards()))
	}
	for _, s := range shardsResp.GetShards() {
		if s.GetLeaderReplicaId() == 0 {
			t.Errorf("TestClusterHealthAndShards: shard %d has no leader", s.GetShardId())
		}
	}

	// Leadership should be spread: across all 3 nodes, the led-shard counts sum to 5.
	var total uint64
	for _, n := range c.nodes {
		h, err := n.cluster.Health(ctx, &zuulv1.HealthRequest{})
		if err != nil {
			t.Fatalf("TestClusterHealthAndShards: Health node %d: got err == %s, want err == nil", n.replicaID, err)
		}
		if !h.GetHealthy() || h.GetShardCount() != 5 {
			t.Errorf("TestClusterHealthAndShards: node %d: healthy=%v shardCount=%d, want true/5", n.replicaID, h.GetHealthy(), h.GetShardCount())
		}
		total += h.GetLeaderCount()
	}
	if total != 5 {
		t.Errorf("TestClusterHealthAndShards: sum of LeaderCount = %d, want 5 (each shard led once)", total)
	}
}
