package forward

import (
	"fmt"

	"github.com/gostdlib/base/context"
	"google.golang.org/grpc"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/forward/forwardpb"
)

// Server is the node-to-node Forwarder gRPC service: it proposes a forwarded
// command locally, but only while this node leads the target shard.
type Server struct {
	forwardpb.UnimplementedForwarderServer
	local Local
}

// NewServer returns a Forwarder server backed by the local node.
func NewServer(local Local) *Server {
	return &Server{local: local}
}

// Register registers the Forwarder service on reg.
func (s *Server) Register(reg grpc.ServiceRegistrar) {
	forwardpb.RegisterForwarderServer(reg, s)
}

// Propose applies a forwarded command locally. If this node is not (or is no
// longer) the shard leader, it returns FailedPrecondition so the caller re-resolves
// the leader and retries.
func (s *Server) Propose(ctx context.Context, req *forwardpb.ProposeRequest) (*forwardpb.ProposeResponse, error) {
	shardID := req.GetShardId()
	if !s.local.IsLeader(shardID) {
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLeader, fmt.Errorf("not leader of shard %d", shardID))
	}
	res, err := s.local.Propose(ctx, shardID, req.GetCommand())
	if err != nil {
		// May have lost leadership mid-flight; FailedPrecondition lets the caller retry.
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeConsensus, fmt.Errorf("propose on shard %d: %v", shardID, err))
	}
	return &forwardpb.ProposeResponse{Result: res}, nil
}

// AddReplica adds a replica to a shard this node must lead.
func (s *Server) AddReplica(ctx context.Context, req *forwardpb.AddReplicaRequest) (*forwardpb.MembershipResponse, error) {
	shardID := req.GetShardId()
	if !s.local.IsLeader(shardID) {
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLeader, fmt.Errorf("not leader of shard %d", shardID))
	}
	if err := s.local.AddReplicaShard(ctx, shardID, req.GetReplicaId(), req.GetRaftAddress()); err != nil {
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeConsensus, fmt.Errorf("add replica on shard %d: %v", shardID, err))
	}
	return &forwardpb.MembershipResponse{}, nil
}

// RemoveReplica removes a replica from a shard this node must lead.
func (s *Server) RemoveReplica(ctx context.Context, req *forwardpb.RemoveReplicaRequest) (*forwardpb.MembershipResponse, error) {
	shardID := req.GetShardId()
	if !s.local.IsLeader(shardID) {
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLeader, fmt.Errorf("not leader of shard %d", shardID))
	}
	if err := s.local.RemoveReplicaShard(ctx, shardID, req.GetReplicaId()); err != nil {
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeConsensus, fmt.Errorf("remove replica on shard %d: %v", shardID, err))
	}
	return &forwardpb.MembershipResponse{}, nil
}
