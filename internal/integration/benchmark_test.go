package integration

import (
	"testing"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// BenchmarkTryLockUnlock measures one uncontended acquire+release round-trip through
// the full stack (route → session → dispatcher → Raft commit → FSM → result) on a
// single in-memory node. Each iteration is two Raft proposals.
func BenchmarkTryLockUnlock(b *testing.B) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	n := c.nodes[0]
	openSession(n, "bench")
	const key = "/test/bench-key"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "bench"})
		if err != nil {
			b.Fatalf("TryLock: %s", err)
		}
		if !resp.GetAcquired() {
			b.Fatalf("TryLock: not acquired (iteration %d)", i)
		}
		if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: "bench", FencingToken: resp.GetFencingToken()}); err != nil {
			b.Fatalf("Unlock: %s", err)
		}
	}
}

// BenchmarkStatusLinearizable measures linearizable read (ReadIndex) throughput for
// a held lock on a single node.
func BenchmarkStatusLinearizable(b *testing.B) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	n := c.nodes[0]
	openSession(n, "bench")
	const key = "/test/bench-read-key"

	if _, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "bench"}); err != nil {
		b.Fatalf("setup TryLock: %s", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.srv.Status(ctx, &zuulv1.StatusRequest{Name: key}); err != nil {
			b.Fatalf("Status: %s", err)
		}
	}
}
