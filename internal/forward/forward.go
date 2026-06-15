// Package forward proxies a write to whichever node currently leads its shard.
// Raft proposals must originate on the leader, so when a write lands on a node
// that does not lead the target shard, the dispatcher forwards the encoded command
// to the leader node's Forwarder gRPC service, which proposes it locally. Leader
// churn is absorbed with exponential-backoff retries that re-resolve the leader.
package forward

import (
	"time"

	"github.com/gostdlib/base/context"
)

// dispatchTimeout bounds a Propose (across all retries) that arrives without its
// own deadline.
const dispatchTimeout = 10 * time.Second

// Local is the local consensus node the dispatcher and Forwarder server sit on
// top of. *consensus.Host satisfies it. Commands and results are opaque bytes so
// one path serves every shard's state machine.
type Local interface {
	// ReplicaID is this node's replica id.
	ReplicaID() uint64
	// IsLeader reports whether this node currently leads shardID.
	IsLeader(shardID uint64) bool
	// LeaderID returns the replica id leading shardID, and whether one is known.
	LeaderID(shardID uint64) (uint64, bool)
	// Propose commits cmd to shardID locally; valid only when this node leads it.
	Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error)
	// AddReplicaShard adds a replica to shardID locally; valid only on its leader.
	AddReplicaShard(ctx context.Context, shardID, replicaID uint64, raftAddr string) error
	// RemoveReplicaShard removes a replica from shardID locally; valid only on its leader.
	RemoveReplicaShard(ctx context.Context, shardID, replicaID uint64) error
}

// Resolver maps a replica id to its forwarding address. topology.Static satisfies
// it.
type Resolver interface {
	Addr(replicaID uint64) (string, bool)
}
