package integration

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// dialClient connects the Go client library to node n over real gRPC.
func dialClient(t *testing.T, c *cluster, n *node, clientID string) *client.Client {
	t.Helper()
	cl, err := client.New(t.Context(), client.Endpoints{c.grpcAddrs[n.replicaID]}, client.WithClientID(clientID), client.WithTTL(30*time.Second))
	if err != nil {
		t.Fatalf("dialClient(%s): %s", clientID, err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// TestClientMutex drives the Mutex helper over real gRPC: contention, fencing, and
// release, with two clients talking to two different nodes.
func TestClientMutex(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	alice := dialClient(t, c, c.nodes[0], "alice")
	bob := dialClient(t, c, c.nodes[1], "bob")

	aMu := alice.NewMutex("/test/client-mutex")
	bMu := bob.NewMutex("/test/client-mutex")

	ok, err := aMu.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TestClientMutex: alice TryLock: ok=%v err=%v, want true/nil", ok, err)
	}
	if aMu.Token() == 0 {
		t.Errorf("TestClientMutex: alice token = 0, want non-zero")
	}

	ok, err = bMu.TryLock(ctx)
	if err != nil {
		t.Fatalf("TestClientMutex: bob TryLock: %s", err)
	}
	if ok {
		t.Errorf("TestClientMutex: bob acquired a held lock, want false")
	}

	if err := aMu.Unlock(ctx); err != nil {
		t.Fatalf("TestClientMutex: alice Unlock: %s", err)
	}

	ok, err = bMu.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TestClientMutex: bob TryLock after release: ok=%v err=%v, want true/nil", ok, err)
	}
	if bMu.Token() <= aMu.Token() {
		t.Errorf("TestClientMutex: bob token %d not greater than alice token %d", bMu.Token(), aMu.Token())
	}
}

// TestClientBlockingLock proves the client's blocking Lock is woken when the holder
// releases.
func TestClientBlockingLock(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	alice := dialClient(t, c, c.nodes[0], "alice")
	bob := dialClient(t, c, c.nodes[1], "bob")

	aMu := alice.NewMutex("/test/blocking-mutex")
	if ok, err := aMu.TryLock(ctx); err != nil || !ok {
		t.Fatalf("TestClientBlockingLock: alice TryLock: ok=%v err=%v", ok, err)
	}

	done := make(chan error, 1)
	bMu := bob.NewMutex("/test/blocking-mutex")
	context.Pool(ctx).Submit(ctx, func() { done <- bMu.Lock(ctx, 5*time.Second) })

	time.Sleep(200 * time.Millisecond) // let bob enqueue
	if err := aMu.Unlock(ctx); err != nil {
		t.Fatalf("TestClientBlockingLock: alice Unlock: %s", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("TestClientBlockingLock: bob blocking Lock: %s", err)
	}
	if !bMu.Held() {
		t.Errorf("TestClientBlockingLock: bob does not hold the lock after promotion")
	}
}

// TestClientElection drives the Election helper: campaign, observe, leader read,
// resign.
func TestClientElection(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	alice := dialClient(t, c, c.nodes[0], "alice")
	el := alice.NewElection("/test/client-election")

	events, err := el.Observe(ctx)
	if err != nil {
		t.Fatalf("TestClientElection: Observe: %s", err)
	}

	if err := el.Campaign(ctx, []byte("alice-value"), 0); err != nil {
		t.Fatalf("TestClientElection: Campaign: %s", err)
	}

	ld, err := el.Leader(ctx)
	if err != nil {
		t.Fatalf("TestClientElection: Leader: %s", err)
	}
	if !ld.Has || ld.ID != "alice" || string(ld.Value) != "alice-value" {
		t.Errorf("TestClientElection: Leader: %+v, want alice/alice-value", ld)
	}

	// The observe stream should report alice as leader.
	if !awaitLeader(t, events, func(li client.Leader) bool {
		return li.Has && li.ID == "alice" && string(li.Value) == "alice-value"
	}) {
		t.Errorf("TestClientElection: never observed alice as leader")
	}

	if err := el.Resign(ctx); err != nil {
		t.Fatalf("TestClientElection: Resign: %s", err)
	}
	ld2, _ := el.Leader(ctx)
	if ld2.Has {
		t.Errorf("TestClientElection: leader after resign: %+v, want leaderless", ld2)
	}
}

// TestClientFollowMaster drives the master-dial helpers over real gRPC: a leader
// publishes its address as an Endpoint, an observer resolves it via Master and
// FollowMaster, and the follower converges on the new master after a leadership
// handoff.
func TestClientFollowMaster(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()

	alice := dialClient(t, c, c.nodes[0], "alice")
	bob := dialClient(t, c, c.nodes[1], "bob")
	carol := dialClient(t, c, c.nodes[2], "carol")

	const name = "/test/master-election"

	aliceEl := alice.NewElection(name)
	if err := aliceEl.Campaign(ctx, endpointValue(t, "10.0.0.1", 9001), 0); err != nil {
		t.Fatalf("TestClientFollowMaster: alice Campaign: %s", err)
	}

	// carol follows the election's current master.
	carolEl := carol.NewElection(name)
	follower, err := carolEl.FollowMaster(ctx)
	if err != nil {
		t.Fatalf("TestClientFollowMaster: FollowMaster: %s", err)
	}
	defer follower.Close()

	if !awaitMaster(t, follower.Updates(), "10.0.0.1:9001") {
		t.Fatalf("TestClientFollowMaster: never resolved alice as master at 10.0.0.1:9001")
	}

	// The one-shot Master lookup agrees with the follower.
	m, ok, err := carolEl.Master(ctx)
	if err != nil || !ok {
		t.Fatalf("TestClientFollowMaster: Master one-shot: ok=%v err=%v, want true/nil", ok, err)
	}
	if m.Address() != "10.0.0.1:9001" || m.LeaderID != "alice" {
		t.Errorf("TestClientFollowMaster: Master one-shot = %q/%q, want 10.0.0.1:9001/alice", m.Address(), m.LeaderID)
	}

	// bob queues behind alice; when alice resigns, bob is promoted and republishes
	// its own address, and the follower must converge on it.
	bobEl := bob.NewElection(name)
	camp := make(chan error, 1)
	context.Pool(ctx).Submit(ctx, func() { camp <- bobEl.Campaign(ctx, endpointValue(t, "10.0.0.2", 9002), 0) })

	time.Sleep(200 * time.Millisecond) // let bob enqueue
	if err := aliceEl.Resign(ctx); err != nil {
		t.Fatalf("TestClientFollowMaster: alice Resign: %s", err)
	}
	if err := <-camp; err != nil {
		t.Fatalf("TestClientFollowMaster: bob Campaign: %s", err)
	}

	if !awaitMaster(t, follower.Updates(), "10.0.0.2:9002") {
		t.Errorf("TestClientFollowMaster: follower never converged on bob at 10.0.0.2:9002")
	}
}

// endpointValue marshals a dialable Endpoint for use as an election value.
func endpointValue(t *testing.T, ip string, port uint32) []byte {
	t.Helper()
	b, err := client.MarshalEndpoint(&zuulv1.Endpoint{Host: ip, Port: port})
	if err != nil {
		t.Fatalf("endpointValue(%s:%d): %s", ip, port, err)
	}
	return b
}

// awaitMaster drains a follower's update channel until a master's address matches
// wantAddr or it times out.
func awaitMaster(t *testing.T, ch <-chan client.Master, wantAddr string) bool {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return false
			}
			if m.Address() == wantAddr {
				return true
			}
		case <-timeout:
			return false
		}
	}
}

// awaitLeader drains the observe channel until pred matches or it times out.
func awaitLeader(t *testing.T, ch <-chan client.Leader, pred func(client.Leader) bool) bool {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		select {
		case li, ok := <-ch:
			if !ok {
				return false
			}
			if pred(li) {
				return true
			}
		case <-timeout:
			return false
		}
	}
}
