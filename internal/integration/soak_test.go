package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gostdlib/base/context"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestSoakLockChurn drives a 3-node cluster with many concurrent clients doing
// contended TryLock/Unlock across a small key space for a sustained number of
// operations, spread over all nodes (so writes forward to shard leaders). It asserts
// two things continuously and at the end:
//   - mutual exclusion: a live per-key holder guard never sees two simultaneous
//     holders (a double-grant would be caught the instant it happens);
//   - no leaks/deadlock: the run completes, and every key ends unheld.
//
// It is not marked slow but is bounded by a fixed op count so CI time stays small.
func TestSoakLockChurn(t *testing.T) {
	const (
		workers = 48
		keys    = 16
		opsEach = 150 // total acquire attempts ~= workers*opsEach = 7200
	)
	c := newCluster(t, 3, 8)
	ctx := t.Context()

	// guard[k] holds the worker id (1-based) currently holding key k, or 0. A failed
	// CAS on acquire means the FSM granted a key that was already held — a bug.
	guard := make([]atomic.Int64, keys)
	var violations atomic.Int64
	var acquired, contended atomic.Int64

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := int64(w + 1)
			clientID := fmt.Sprintf("soak-%d", id)
			// Each worker pins to one node (round-robin) and opens its session there;
			// the node forwards proposes to the shard leaders internally.
			n := c.nodes[w%len(c.nodes)]
			openSession(n, clientID)
			rng := rand.New(rand.NewSource(id)) // deterministic per worker

			for i := 0; i < opsEach; i++ {
				k := rng.Intn(keys)
				key := fmt.Sprintf("/test/soak/key-%d", k)
				res, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: clientID})
				if err != nil {
					t.Errorf("TestSoakLockChurn: TryLock %s: %s", key, err)
					return
				}
				if !res.GetAcquired() {
					contended.Add(1)
					continue
				}
				acquired.Add(1)
				// We hold key k. Nobody else may hold it: claim the guard.
				if !guard[k].CompareAndSwap(0, id) {
					violations.Add(1)
					t.Errorf("TestSoakLockChurn: mutual exclusion violated on %s (held by %d, we are %d)", key, guard[k].Load(), id)
				}
				// Release: clear the guard first, then unlock.
				guard[k].CompareAndSwap(id, 0)
				if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: clientID, FencingToken: res.GetFencingToken()}); err != nil {
					t.Errorf("TestSoakLockChurn: Unlock %s: %s", key, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("TestSoakLockChurn: %d mutual-exclusion violations", v)
	}
	t.Logf("TestSoakLockChurn: %d acquired, %d contended (no violations)", acquired.Load(), contended.Load())

	// Every key must end unheld — no leaked locks.
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("/test/soak/key-%d", k)
		st, err := c.nodes[0].srv.Status(context.Background(), &zuulv1.StatusRequest{Name: key})
		if err != nil {
			t.Errorf("TestSoakLockChurn: final Status %s: %s", key, err)
			continue
		}
		if st.GetHeld() {
			t.Errorf("TestSoakLockChurn: %s still held at end by %q", key, st.GetHolderClientId())
		}
	}
}
