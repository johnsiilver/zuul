package forward

import (
	"fmt"
	"time"

	"github.com/gostdlib/base/retry/exponential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/otel/metrics"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
)

// errStopRetry marks a structurally permanent failure (unknown leader address, or
// undecodable bytes) so the retry loop gives up immediately.
var errStopRetry = errors.New("forward: permanent failure")

// errNoLeader is the transient "leadership not settled yet" condition; it is
// retried.
var errNoLeader = errors.New("forward: no leader elected yet")

// Dispatcher routes a write to the shard's leader: proposes locally when this node
// leads the shard, otherwise forwards to the leader node's Forwarder. Transient
// failures (leader churn, peer unavailable) are retried with exponential backoff;
// structural failures stop immediately.
type Dispatcher struct {
	local Local
	topo  Resolver
	pool  *pool
	boff  *exponential.Backoff
}

// NewDispatcher returns a Dispatcher over the local node and a topology resolver.
// dialOpts are applied when dialing peer nodes (e.g. mutual-TLS credentials); with
// none, peers are dialed insecurely.
func NewDispatcher(local Local, topo Resolver, dialOpts ...grpc.DialOption) (*Dispatcher, error) {
	boff, err := exponential.New()
	if err != nil {
		return nil, fmt.Errorf("forward.NewDispatcher: %w", err)
	}
	return &Dispatcher{local: local, topo: topo, pool: newPool(dialOpts), boff: boff}, nil
}

// Close releases pooled peer connections.
func (d *Dispatcher) Close() {
	d.pool.close()
}

// retry runs fn through exponential backoff: transient errors are retried,
// errStopRetry-wrapped errors stop immediately, and the whole thing is bounded by
// the dispatch deadline.
func (d *Dispatcher) retry(ctx context.Context, fn func(ctx context.Context) error) error {
	dctx, cancel := context.EnsureDeadline(ctx, dispatchTimeout)
	if cancel != nil {
		defer cancel()
	}
	return d.boff.Retry(dctx, func(ctx context.Context, _ exponential.Record) error {
		err := fn(ctx)
		if err != nil && errors.Is(err, errStopRetry) {
			return fmt.Errorf("%w: %w", err, exponential.ErrPermanent)
		}
		return err
	})
}

// route decides how to reach shardID's leader: local == true to run on this node,
// otherwise addr is the leader's forwarding address. Unknown-leader and
// unknown-address conditions are both transient: the leader may still be electing,
// and a runtime-added leader's address may not have replicated into the meta shard
// yet — the retry loop re-resolves until the dispatch deadline.
func (d *Dispatcher) route(shardID uint64) (local bool, addr string, err error) {
	if d.local.IsLeader(shardID) {
		return true, "", nil
	}
	leaderID, ok := d.local.LeaderID(shardID)
	if !ok {
		return false, "", errNoLeader
	}
	if leaderID == d.local.ReplicaID() {
		return true, "", nil
	}
	a, ok := d.topo.Addr(leaderID)
	if !ok {
		return false, "", fmt.Errorf("forward: no address yet for leader replica %d", leaderID)
	}
	return false, a, nil
}

// Propose commits the marshalled command cmd to shardID, locally or via the leader
// node, retrying through leader churn until it succeeds or the deadline passes. It
// returns the marshalled result.
func (d *Dispatcher) Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error) {
	start := time.Now()
	var (
		result []byte
		local  bool
	)
	err := d.retry(ctx, func(ctx context.Context) error {
		isLocal, addr, err := d.route(shardID)
		if err != nil {
			return err
		}
		local = isLocal
		if isLocal {
			result, err = d.local.Propose(ctx, shardID, cmd)
			return err
		}
		cli, err := d.pool.client(addr)
		if err != nil {
			return fmt.Errorf("%w: dial %s: %v", errStopRetry, addr, err)
		}
		resp, err := cli.Propose(ctx, &forwardpb.ProposeRequest{ShardId: shardID, Command: cmd})
		if err != nil {
			d.evictIfUnavailable(addr, err)
			return err // gRPC FailedPrecondition/Unavailable are retried by the loop.
		}
		result = resp.GetResult()
		return nil
	})
	if err != nil {
		return nil, d.classify(ctx, fmt.Errorf("forward.Propose: shard %d: %w", shardID, err))
	}
	metrics.Forward(ctx, target(local))
	metrics.ProposeDuration(ctx, time.Since(start).Seconds())
	return result, nil
}

// ListObservers fetches a peer's local watch-hub observers for key by dialing addr
// (its forwarding gRPC address). It is the per-peer leg of the browse UI's cluster-wide
// observer aggregation; the caller fans it out across members and tolerates failures.
func (d *Dispatcher) ListObservers(ctx context.Context, addr, key string) (*forwardpb.ListObserversResponse, error) {
	cli, err := d.pool.client(addr)
	if err != nil {
		return nil, fmt.Errorf("forward.ListObservers: dial %s: %w", addr, err)
	}
	resp, err := cli.ListObservers(ctx, &forwardpb.ListObserversRequest{Key: key})
	if err != nil {
		d.evictIfUnavailable(addr, err)
		return nil, err
	}
	return resp, nil
}

// evictIfUnavailable drops the pooled connection to addr when err reports the peer is
// unreachable, so the next attempt re-dials (lazily) instead of reusing a dead
// connection. A nil err or any other code is a no-op.
func (d *Dispatcher) evictIfUnavailable(addr string, err error) {
	if err != nil && status.Code(err) == codes.Unavailable {
		d.pool.evict(addr)
	}
}

// target labels a propose by where it ran.
func target(local bool) string {
	if local {
		return "local"
	}
	return "remote"
}

// AddReplica adds replicaID (reachable at raftAddr) to shardID, routed to the
// shard's leader and retried through churn.
func (d *Dispatcher) AddReplica(ctx context.Context, shardID, replicaID uint64, raftAddr string) error {
	err := d.retry(ctx, func(ctx context.Context) error {
		local, addr, err := d.route(shardID)
		if err != nil {
			return err
		}
		if local {
			return d.local.AddReplicaShard(ctx, shardID, replicaID, raftAddr)
		}
		cli, err := d.pool.client(addr)
		if err != nil {
			return fmt.Errorf("%w: dial %s: %v", errStopRetry, addr, err)
		}
		_, err = cli.AddReplica(ctx, &forwardpb.AddReplicaRequest{ShardId: shardID, ReplicaId: replicaID, RaftAddress: raftAddr})
		d.evictIfUnavailable(addr, err)
		return err
	})
	if err != nil {
		return d.classify(ctx, fmt.Errorf("forward.AddReplica: shard %d: %w", shardID, err))
	}
	return nil
}

// RemoveReplica removes replicaID from shardID, routed to the shard's leader and
// retried through churn.
func (d *Dispatcher) RemoveReplica(ctx context.Context, shardID, replicaID uint64) error {
	err := d.retry(ctx, func(ctx context.Context) error {
		local, addr, err := d.route(shardID)
		if err != nil {
			return err
		}
		if local {
			return d.local.RemoveReplicaShard(ctx, shardID, replicaID)
		}
		cli, err := d.pool.client(addr)
		if err != nil {
			return fmt.Errorf("%w: dial %s: %v", errStopRetry, addr, err)
		}
		_, err = cli.RemoveReplica(ctx, &forwardpb.RemoveReplicaRequest{ShardId: shardID, ReplicaId: replicaID})
		d.evictIfUnavailable(addr, err)
		return err
	})
	if err != nil {
		return d.classify(ctx, fmt.Errorf("forward.RemoveReplica: shard %d: %w", shardID, err))
	}
	return nil
}

// classify stamps a failed dispatch with a Category for the wire. A structurally
// permanent failure (errStopRetry: an undialable leader address or a closed pool) is
// classified Internal and marked non-retryable, so a caller does not retry a request
// that cannot succeed; the prior blanket Unavailable wrongly signalled it as retryable.
// Any other failure is a retryable consensus/forwarding error classified as Unavailable.
// An already-classified inner error (e.g. a forwarded PermissionDenied) keeps its own
// code via the Error's GRPCStatus.
func (d *Dispatcher) classify(ctx context.Context, err error) error {
	if errors.Is(err, errStopRetry) {
		return errors.E(ctx, errors.CatInternal, errors.TypeConsensus, errors.Permanent(err))
	}
	return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, err)
}
