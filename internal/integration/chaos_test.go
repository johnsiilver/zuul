package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/gostdlib/base/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestLinearizableUnderFailover hammers one key with several clients while the shard
// leader is killed mid-contention, then verifies with Porcupine that the recorded
// history is still linearizable — i.e. failover never let two clients hold the lock.
//
// In-flight operations are made unambiguous by retrying: AcquireLock is idempotent
// (a re-try by the holder returns the same grant), and an Unlock retried after it
// already applied returns FailedPrecondition, which we treat as "released". Each
// recorded op's interval spans its whole retry sequence, so Porcupine can place its
// linearization point anywhere inside.
func TestLinearizableUnderFailover(t *testing.T) {
	c := newCluster(t, 3, 1) // 3 nodes, one lock shard (plus the meta shard)
	ctx := t.Context()

	const key = "/test/chaos"
	shardID := c.router.Shard(key)
	leaderID := c.leaderReplica(t, shardID)
	entry := c.otherNode(leaderID) // a survivor; all clients enter here

	// Kill the shard leader partway through the run; the survivors re-elect.
	context.Pool(ctx).Submit(ctx, func() {
		time.Sleep(500 * time.Millisecond)
		c.nodeByReplica(leaderID).host.Close()
	})

	rec := &recorder{}
	const clients = 4
	clientDeadline := time.Now().Add(3 * time.Second)

	g := context.Pool(ctx).Group()
	for i := 0; i < clients; i++ {
		i := i
		clientID := fmt.Sprintf("chaos-%d", i)
		openSession(entry, clientID)
		g.Go(ctx, func(ctx context.Context) error {
			for time.Now().Before(clientDeadline) {
				call := time.Now().UnixNano()
				acquired, token, ok := tryLockRetry(ctx, entry, key, clientID, clientDeadline)
				ret := time.Now().UnixNano()
				if !ok {
					continue // never got a definite result before the deadline
				}
				rec.record(i, lockInput{op: "lock", client: clientID}, lockOutput{ok: acquired}, call, ret)
				if !acquired {
					continue
				}

				time.Sleep(time.Duration(i+1) * time.Millisecond) // hold, forcing overlap

				call = time.Now().UnixNano()
				ok = unlockRetry(ctx, entry, key, clientID, token)
				ret = time.Now().UnixNano()
				if ok {
					rec.record(i, lockInput{op: "unlock", client: clientID}, lockOutput{ok: true}, call, ret)
				}
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestLinearizableUnderFailover: %s", err)
	}

	if len(rec.ops) < 10 {
		t.Fatalf("TestLinearizableUnderFailover: only %d ops completed; cluster did not make progress through failover", len(rec.ops))
	}
	switch porcupine.CheckOperationsTimeout(lockModel, rec.ops, 30*time.Second) {
	case porcupine.Ok:
		// linearizable through the failover
	case porcupine.Illegal:
		t.Errorf("TestLinearizableUnderFailover: NOT linearizable — failover allowed two holders (%d ops)", len(rec.ops))
	case porcupine.Unknown:
		t.Errorf("TestLinearizableUnderFailover: check timed out over %d ops", len(rec.ops))
	}
}

// tryLockRetry retries TryLock through transient (transport) failures until it gets a
// definite acquired/not-acquired result, or the deadline passes.
func tryLockRetry(ctx context.Context, n *node, key, clientID string, deadline time.Time) (acquired bool, token uint64, ok bool) {
	for time.Now().Before(deadline) {
		resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: clientID})
		if err == nil {
			return resp.GetAcquired(), resp.GetFencingToken(), true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false, 0, false
}

// unlockRetry retries Unlock through transient failures. A FailedPrecondition means
// the release already applied (the response to an earlier attempt was lost), which is
// success. It runs slightly past the client deadline so a late acquisition is released.
func unlockRetry(ctx context.Context, n *node, key, clientID string, token uint64) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: clientID, FencingToken: token})
		if err == nil {
			return true
		}
		if status.Code(err) == codes.FailedPrecondition {
			return true // already released
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
