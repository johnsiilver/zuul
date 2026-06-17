package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// lockInput / lockOutput are one recorded operation against a single lock key.
type lockInput struct {
	op     string // "lock" or "unlock"
	client string
}

type lockOutput struct {
	ok bool // lock: acquired; unlock: released
}

// lockModel is the sequential specification of a mutual-exclusion lock: the state
// is the current holder ("" when free). A try-lock succeeds iff the lock is free; a
// holder's unlock always succeeds. If the recorded concurrent history cannot be
// explained by any sequential ordering of this model, two clients held the lock at
// once — the property a lock service must never violate.
var lockModel = porcupine.Model{
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		holder := state.(string)
		in := input.(lockInput)
		out := output.(lockOutput)
		switch in.op {
		case "lock":
			if holder == "" {
				return out.ok, in.client // free: must acquire; new holder is the caller
			}
			return !out.ok, holder // held: must fail; holder unchanged
		case "unlock":
			if holder == in.client {
				if out.ok {
					return true, ""
				}
				return false, holder
			}
			return !out.ok, holder // non-holder unlock must fail
		default:
			return false, holder
		}
	},
	Equal: func(a, b any) bool { return a.(string) == b.(string) },
}

// recorder collects operations with their invocation/response timestamps.
type recorder struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (r *recorder) record(clientID int, in lockInput, out lockOutput, call, ret int64) {
	r.mu.Lock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: clientID, Input: in, Call: call, Output: out, Return: ret})
	r.mu.Unlock()
}

// TestLockLinearizable hammers one lock key with several concurrent clients,
// records the history, and verifies with Porcupine that it is linearizable — i.e.
// the lock was never held by two clients at once. Run on a single node so every
// operation has an unambiguous result (no forwarding timeouts to leave an op's
// outcome unknown).
func TestLockLinearizable(t *testing.T) {
	c := newCluster(t, 1, 1)
	ctx := t.Context()
	n := c.nodes[0]

	const (
		clients    = 4
		iterations = 20
		key        = "/test/linz"
	)

	rec := &recorder{}
	g := context.Pool(ctx).Group()
	for i := 0; i < clients; i++ {
		i := i
		clientID := fmt.Sprintf("client-%d", i)
		openSession(n, clientID)
		g.Go(ctx, func(ctx context.Context) error {
			for j := 0; j < iterations; j++ {
				// Try to acquire.
				call := time.Now().UnixNano()
				resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: clientID})
				ret := time.Now().UnixNano()
				if err != nil {
					return fmt.Errorf("%s: TryLock: %w", clientID, err)
				}
				rec.record(i, lockInput{op: "lock", client: clientID}, lockOutput{ok: resp.GetAcquired()}, call, ret)
				if !resp.GetAcquired() {
					continue
				}

				// Hold briefly to force overlap, then release.
				time.Sleep(time.Duration(i+1) * time.Millisecond)

				call = time.Now().UnixNano()
				_, err = n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: clientID, FencingToken: resp.GetFencingToken()})
				ret = time.Now().UnixNano()
				if err != nil {
					return fmt.Errorf("%s: Unlock: %w", clientID, err)
				}
				rec.record(i, lockInput{op: "unlock", client: clientID}, lockOutput{ok: true}, call, ret)
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestLockLinearizable: client error: %s", err)
	}

	result := porcupine.CheckOperationsTimeout(lockModel, rec.ops, 30*time.Second)
	switch result {
	case porcupine.Ok:
		// linearizable
	case porcupine.Illegal:
		t.Errorf("TestLockLinearizable: history is NOT linearizable — mutual exclusion was violated (%d ops)", len(rec.ops))
	case porcupine.Unknown:
		t.Errorf("TestLockLinearizable: linearizability check timed out (inconclusive) over %d ops", len(rec.ops))
	}
}
