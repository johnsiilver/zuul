package integration

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/context"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// membersCount returns how many nodes the meta shard lists, read from n. It retries:
// a linearizable read on a replica that just joined (or right after a membership
// change) can be briefly unavailable while it catches up — a real client would retry
// the same way.
func membersCount(t *testing.T, n *node) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := n.cluster.Members(t.Context(), &zuulv1.MembersRequest{})
		if err == nil {
			return len(resp.GetMembers())
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("membersCount: Members never succeeded: %s", lastErr)
	return 0
}

// TestGrowCluster proves a node added at runtime joins, catches up (a lock taken
// before the grow is visible on it), is recorded in the meta shard, and can serve.
func TestGrowCluster(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	// Hold a lock before growing.
	openSession(c.nodes[0], "pre")
	const key = "/test/pre-grow-key"
	held, err := c.nodes[0].srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "pre"})
	if err != nil || !held.GetAcquired() {
		t.Fatalf("TestGrowCluster: pre-grow TryLock: acquired=%v err=%v, want true/nil", held.GetAcquired(), err)
	}

	newNode := c.addNode(t)

	if got := membersCount(t, newNode); got != 4 {
		t.Errorf("TestGrowCluster: meta members after grow = %d, want 4", got)
	}

	// The pre-grow lock is replicated to and visible on the new node.
	st := awaitStatus(t, newNode, key)
	if !st.GetHeld() || st.GetHolderClientId() != "pre" || st.GetFencingToken() != held.GetFencingToken() {
		t.Errorf("TestGrowCluster: Status on new node: held=%v holder=%q token=%d, want true/pre/%d", st.GetHeld(), st.GetHolderClientId(), st.GetFencingToken(), held.GetFencingToken())
	}

	// The new node can serve writes (forwarded to leaders).
	openSession(newNode, "c4")
	got, err := newNode.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/post-grow-key", ClientId: "c4"})
	if err != nil {
		t.Fatalf("TestGrowCluster: TryLock via new node: %s", err)
	}
	if !got.GetAcquired() {
		t.Errorf("TestGrowCluster: TryLock via new node: acquired=false, want true")
	}
}

// TestShrinkCluster proves a node removed at runtime drops out of the meta shard and
// the cluster keeps serving.
func TestShrinkCluster(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	added := c.addNode(t)
	if got := membersCount(t, c.nodes[0]); got != 4 {
		t.Fatalf("TestShrinkCluster: members after grow = %d, want 4", got)
	}

	c.removeNode(t, added)

	survivor := c.nodes[0]
	if got := membersCount(t, survivor); got != 3 {
		t.Errorf("TestShrinkCluster: members after shrink = %d, want 3", got)
	}

	openSession(survivor, "after")
	got, err := survivor.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/after-shrink-key", ClientId: "after"})
	if err != nil {
		t.Fatalf("TestShrinkCluster: TryLock after shrink: %s", err)
	}
	if !got.GetAcquired() {
		t.Errorf("TestShrinkCluster: TryLock after shrink: acquired=false, want true")
	}
}

// TestAmnesiacReAdd proves a crashed node that restarts with no state is re-admitted
// as a fresh replica (remove the old one, add a new one in join mode), and the
// cluster keeps serving throughout.
func TestAmnesiacReAdd(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	// "Crash" a node: stop it without removing it from the cluster config.
	crashed := c.nodes[2]
	crashed.host.Close()

	// Clean up the dead replica, then admit a fresh one (a new replica id, empty FS).
	c.removeNode(t, crashed)
	rejoined := c.addNode(t)

	if got := membersCount(t, rejoined); got != 3 {
		t.Errorf("TestAmnesiacReAdd: members after re-add = %d, want 3", got)
	}

	openSession(rejoined, "reborn")
	got, err := rejoined.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/amnesiac-key", ClientId: "reborn"})
	if err != nil {
		t.Fatalf("TestAmnesiacReAdd: TryLock via re-added node: %s", err)
	}
	if !got.GetAcquired() {
		t.Errorf("TestAmnesiacReAdd: TryLock via re-added node: acquired=false, want true")
	}
}

// TestGracefulDrain proves Drain transfers leadership of every shard a node leads to
// other members, so after draining it leads nothing and the cluster keeps serving.
func TestGracefulDrain(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()
	allShards := append([]uint64{metaShardID}, c.shards...)

	// Find a node that leads at least one shard.
	var drained *node
	for _, n := range c.nodes {
		for _, sh := range allShards {
			if n.host.IsLeader(sh) {
				drained = n
				break
			}
		}
		if drained != nil {
			break
		}
	}
	if drained == nil {
		t.Fatal("TestGracefulDrain: no shard leader found")
	}

	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	drained.host.Drain(drainCtx)

	for _, sh := range allShards {
		if drained.host.IsLeader(sh) {
			t.Errorf("TestGracefulDrain: drained node %d still leads shard %d", drained.replicaID, sh)
		}
	}

	// The cluster still serves through a survivor.
	survivor := c.otherNode(drained.replicaID)
	openSession(survivor, "after-drain")
	got, err := survivor.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/test/after-drain-key", ClientId: "after-drain"})
	if err != nil {
		t.Fatalf("TestGracefulDrain: TryLock after drain: %s", err)
	}
	if !got.GetAcquired() {
		t.Errorf("TestGracefulDrain: TryLock after drain: acquired=false, want true")
	}
}
