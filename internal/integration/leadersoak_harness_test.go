package integration

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestRealClockLeaseExpiry proves the soak harness's real-clock cluster actually expires
// a lapsed lease: a lock taken under a short TTL that is never kept alive is reaped by
// the leader's expiry sweep and released. (Under newCluster's frozen testNow clock this
// would never happen, which is exactly why the soak needs a real clock.)
func TestRealClockLeaseExpiry(t *testing.T) {
	c := newRealClockCluster(t, 3, 4, 200*time.Millisecond)
	ctx := t.Context()
	n := c.nodes[0]

	const (
		key   = "/test/expire/lock"
		ghost = "ghost"
		ttlMS = int64(1000)
	)
	n.sessions.Open(ghost, ttlMS)
	res, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: ghost})
	switch {
	case err != nil:
		t.Fatalf("TestRealClockLeaseExpiry: TryLock: got err == %s, want err == nil", err)
	case !res.GetAcquired():
		t.Fatalf("TestRealClockLeaseExpiry: TryLock: acquired == false, want true")
	}

	// The ghost's lease is never kept alive; with the real clock the leader's expiry
	// sweep must reap it (within ~ttl + sweep interval), releasing the lock.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st := awaitStatus(t, n, key); !st.GetHeld() {
			return // lease expired, lock released — correct
		}
		if time.Now().After(deadline) {
			t.Fatalf("TestRealClockLeaseExpiry: %s still held after its lease should have expired", key)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRestartNodeReusesPort proves restartNode rebinds a crashed node on its original
// gRPC port, so a client configured with the static three-endpoint list keeps working
// across the restart (the path the soak relies on for stable client endpoints).
func TestRestartNodeReusesPort(t *testing.T) {
	c := newRealClockCluster(t, 3, 4, 0)
	ctx := t.Context()

	eps := client.Endpoints{c.grpcAddrs[1], c.grpcAddrs[2], c.grpcAddrs[3]}
	cl, err := client.New(ctx, eps, client.WithClientID("survivor"), client.WithTTL(3*time.Second))
	if err != nil {
		t.Fatalf("TestRestartNodeReusesPort: client.New: %s", err)
	}
	// Close via defer, not t.Cleanup: a node's graceful Close (GracefulStop) blocks on any
	// open client stream, and restartNode registers its new node's cleanup AFTER this one,
	// so a t.Cleanup here would run (LIFO) after that node's Close and deadlock teardown.
	// A defer runs before every t.Cleanup, guaranteeing the client closes before any node.
	defer func() { _ = cl.Close() }()

	mu := cl.NewMutex("/test/restart/lock")
	if ok, err := mu.TryLock(ctx); err != nil || !ok {
		t.Fatalf("TestRestartNodeReusesPort: TryLock: ok=%v err=%v, want true/nil", ok, err)
	}

	oldPort := c.grpcAddrs[1]
	restarted := c.restartNode(t, c.nodes[0], true) // hard crash + reuse port
	// restartNode does not register a cleanup for its node; this runs after the client's
	// deferred Close (t.Cleanup runs after function defers), so the node closes cleanly.
	t.Cleanup(func() { restarted.n.Close() })
	if got := c.grpcAddrs[restarted.replicaID]; got != oldPort {
		t.Errorf("TestRestartNodeReusesPort: restarted node grpc=%q, want reused %q", got, oldPort)
	}

	// The client keeps working: it acquires a second lock after the restart.
	mu2 := cl.NewMutex("/test/restart/second")
	deadline := time.Now().Add(20 * time.Second)
	for {
		ok, err := mu2.TryLock(ctx)
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("TestRestartNodeReusesPort: client never recovered after restart: ok=%v err=%v", ok, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestGracefulStopBounded proves a node's graceful Close returns within a bounded time even
// while a client holds a long-lived keepalive stream to it: shutdown must force the stream
// closed rather than block on gRPC GracefulStop forever. Before the bounded-stop fix, Close
// hung until an external SIGKILL (so this test would time out).
func TestGracefulStopBounded(t *testing.T) {
	c := newRealClockCluster(t, 3, 4, 0)
	ctx := t.Context()
	target := c.nodes[0]

	// Pin a client to the target's single endpoint so its keepalive stream lands on it, and
	// hold a lock so there is real session activity to wait on.
	cl, err := client.New(ctx, client.Endpoints{c.grpcAddrs[target.replicaID]}, client.WithClientID("holder"), client.WithTTL(30*time.Second))
	if err != nil {
		t.Fatalf("TestGracefulStopBounded: client.New: %s", err)
	}
	defer func() { _ = cl.Close() }()
	if ok, err := cl.NewMutex("/test/graceful/lock").TryLock(ctx); err != nil || !ok {
		t.Fatalf("TestGracefulStopBounded: TryLock: ok=%v err=%v, want true/nil", ok, err)
	}

	closed := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		target.n.Close()
		close(closed)
	})
	select {
	case <-closed:
		// graceful Close returned within the bound
	case <-time.After(20 * time.Second):
		t.Fatalf("TestGracefulStopBounded: node.Close blocked >20s on the open client stream — graceful stop is not bounded")
	}
}

// TestSoakGhostFault deterministically exercises the soak's broken-client (ghost) fault:
// a single-endpoint, short-TTL client grabs leadership, its only node is killed, and the
// fault must observe the session lapse (Done fired) and the orphaned leadership being
// reaped, then restore the node — all with no recorded invariant violation. (The soak
// itself only rolls this fault ~1 in 10, so it is validated directly here.)
func TestSoakGhostFault(t *testing.T) {
	c := newRealClockCluster(t, 3, 4, 500*time.Millisecond)
	cfg := soakConfig{
		duration: time.Minute, electionKeys: 1, contendersPerKey: 2, churnKeys: 1,
		lockKeys: 1, churnWorkers: 1, checkEvery: time.Minute, randomCheckEvery: time.Minute,
		faultEvery: time.Minute, settle: time.Second, expiryInterval: 500 * time.Millisecond,
		livenessTimeout: 10 * time.Second, graceTTL: 30 * time.Second,
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("TestSoakGhostFault: config: %s", err)
	}
	s := newSoaker(t, newInProcBackend(t, c), cfg)
	defer s.teardown()

	s.faultGhost()

	if v := s.count.violations.Load(); v != 0 {
		t.Errorf("TestSoakGhostFault: got %d violations, want 0 (see logged soak errors)", v)
	}
	if got := len(c.nodes); got != 3 {
		t.Errorf("TestSoakGhostFault: cluster has %d nodes after the fault, want 3", got)
	}
}
