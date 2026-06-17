package integration

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
)

// TestClientFailover proves the client survives the death of the node it is
// connected to: with several endpoints configured, the keepalive stream
// re-establishes the session on another node (recovering its per-shard leases
// there), the held lock is preserved, and the client keeps working — including
// releasing the original lock with its original fencing token.
func TestClientFailover(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	// gRPC's default pick_first policy means the client lands on the first
	// endpoint (node 1) until it dies.
	endpoints := client.Endpoints{c.grpcAddrs[1], c.grpcAddrs[2], c.grpcAddrs[3]}
	cl, err := client.New(ctx, endpoints, client.WithClientID("phoenix"), client.WithTTL(3*time.Second))
	if err != nil {
		t.Fatalf("TestClientFailover: Dial: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	mu := cl.NewMutex("/test/failover/lock")
	ok, err := mu.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TestClientFailover: TryLock: ok=%v err=%v, want true/nil", ok, err)
	}

	// Kill node 1's client-facing front end. Drain its shard leadership first so
	// surviving nodes never need to forward through the dead gRPC endpoint.
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	c.nodes[0].host.Drain(drainCtx)
	cancel()
	c.nodes[0].n.Stop()

	// The lock survives: visible via node 3 with the same holder and token.
	st := awaitStatus(t, c.nodes[2], "/test/failover/lock")
	if !st.GetHeld() || st.GetHolderClientId() != "phoenix" || st.GetFencingToken() != mu.Token() {
		t.Fatalf("TestClientFailover: post-kill status: held=%v holder=%q token=%d, want true/phoenix/%d", st.GetHeld(), st.GetHolderClientId(), st.GetFencingToken(), mu.Token())
	}

	// The client recovers: once its heartbeat re-establishes the session on a
	// surviving node, new acquisitions work again.
	mu2 := cl.NewMutex("/test/failover/second")
	deadline := time.Now().Add(20 * time.Second)
	for {
		ok, err := mu2.TryLock(ctx)
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("TestClientFailover: client never recovered: ok=%v err=%v", ok, err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// And the original lock releases cleanly with its original token.
	if err := mu.Unlock(ctx); err != nil {
		t.Fatalf("TestClientFailover: Unlock after failover: %s", err)
	}
	st2 := awaitStatus(t, c.nodes[2], "/test/failover/lock")
	if st2.GetHeld() {
		t.Errorf("TestClientFailover: lock still held after post-failover unlock")
	}
}
