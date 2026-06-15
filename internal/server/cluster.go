package server

import (
	"fmt"

	"github.com/gostdlib/base/context"
	"google.golang.org/grpc"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/meta/metapb"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// clusterKey is the authorization key for Cluster operations. Reading topology
// requires Read on it; membership changes require the distinct Admin right, which a
// wildcard read-write lock grant does NOT confer (grant with an ACL line like
// "operator * rwa"). Health is exempt so monitors can always probe.
const clusterKey = "cluster/"

// ClusterReader exposes the topology and per-shard leadership this node knows.
// *consensus.Host satisfies it.
type ClusterReader interface {
	// ReplicaID is this node's replica id.
	ReplicaID() uint64
	// Hosted returns every shard id this node hosts.
	Hosted() []uint64
	// LeaderID returns the replica id leading shardID, and whether one is known.
	LeaderID(shardID uint64) (uint64, bool)
	// MetaList returns every cluster member from the meta shard.
	MetaList(ctx context.Context) ([]*metapb.Member, error)
}

// MembershipChanger applies a Raft membership change to a single shard, routing it
// to that shard's leader. *forward.Dispatcher satisfies it.
type MembershipChanger interface {
	AddReplica(ctx context.Context, shardID, replicaID uint64, raftAddr string) error
	RemoveReplica(ctx context.Context, shardID, replicaID uint64) error
}

// ClusterConfig configures a ClusterServer.
type ClusterConfig struct {
	// Host is the local node, for topology and leadership reads. Required.
	Host ClusterReader
	// Members applies per-shard membership changes. When nil, AddNode/RemoveNode are
	// unimplemented and the server is read-only.
	Members MembershipChanger
	// Proposer records membership changes in the meta shard. Required when Members
	// is set.
	Proposer Proposer
	// MetaShardID is the meta shard's id. Required when Members is set.
	MetaShardID uint64
	// Authorizer gates Cluster operations under the "cluster/" namespace by the
	// caller's authenticated identity. Default AllowAll.
	Authorizer authz.Authorizer
}

// ClusterServer implements the Cluster admin API: Members/Shards/Health always, and
// AddNode/RemoveNode when a MembershipChanger is configured.
type ClusterServer struct {
	zuulv1.UnimplementedClusterServer
	cfg ClusterConfig
}

// NewClusterServer returns a Cluster admin server. With cfg.Members nil it serves
// only the read APIs.
func NewClusterServer(cfg ClusterConfig) *ClusterServer {
	if cfg.Authorizer == nil {
		cfg.Authorizer = authz.AllowAll()
	}
	return &ClusterServer{cfg: cfg}
}

// authorize checks the caller's identity against the cluster admin namespace.
func (c *ClusterServer) authorize(ctx context.Context, op authz.Op) error {
	identity, _ := authz.IdentityFromContext(ctx)
	if err := c.cfg.Authorizer.Authorize(identity, clusterKey, op); err != nil {
		return errors.E(ctx, errors.CatPermission, errors.TypeUnauthorizedCluster, fmt.Errorf("client %q is not authorized for cluster administration", identity))
	}
	return nil
}

// Register registers the Cluster service on reg.
func (c *ClusterServer) Register(reg grpc.ServiceRegistrar) {
	zuulv1.RegisterClusterServer(reg, c)
}

// Members lists the cluster's nodes and their addresses from the meta shard.
func (c *ClusterServer) Members(ctx context.Context, _ *zuulv1.MembersRequest) (*zuulv1.MembersResponse, error) {
	if err := c.authorize(ctx, authz.Read); err != nil {
		return nil, err
	}
	members, err := c.cfg.Host.MetaList(ctx)
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	out := &zuulv1.MembersResponse{}
	for _, m := range members {
		out.Members = append(out.Members, &zuulv1.Member{
			ReplicaId:       m.GetReplicaId(),
			NodeHostId:      m.GetNodeHostId(),
			RaftAddress:     m.GetRaftAddress(),
			ZuulGrpcAddress: m.GetZuulGrpcAddress(),
		})
	}
	return out, nil
}

// Shards reports, per hosted shard, which replica this node sees as leader.
func (c *ClusterServer) Shards(ctx context.Context, _ *zuulv1.ShardsRequest) (*zuulv1.ShardsResponse, error) {
	if err := c.authorize(ctx, authz.Read); err != nil {
		return nil, err
	}
	out := &zuulv1.ShardsResponse{}
	for _, shardID := range c.cfg.Host.Hosted() {
		info := &zuulv1.ShardInfo{ShardId: shardID}
		if id, ok := c.cfg.Host.LeaderID(shardID); ok {
			info.LeaderReplicaId = id
		}
		out.Shards = append(out.Shards, info)
	}
	return out, nil
}

// Health reports this node's shard counts and how many it currently leads.
func (c *ClusterServer) Health(_ context.Context, _ *zuulv1.HealthRequest) (*zuulv1.HealthResponse, error) {
	hosted := c.cfg.Host.Hosted()
	var leaders uint64
	for _, shardID := range hosted {
		if id, ok := c.cfg.Host.LeaderID(shardID); ok && id == c.cfg.Host.ReplicaID() {
			leaders++
		}
	}
	return &zuulv1.HealthResponse{Healthy: true, ShardCount: uint64(len(hosted)), LeaderCount: leaders}, nil
}

// AddNode adds a replica to every shard (each change routed to that shard's leader)
// and records the new node in the meta shard. The new node must then boot in join
// mode. It is not implemented when no MembershipChanger is configured.
func (c *ClusterServer) AddNode(ctx context.Context, req *zuulv1.AddNodeRequest) (*zuulv1.AddNodeResponse, error) {
	if err := c.authorize(ctx, authz.Admin); err != nil {
		return nil, err
	}
	if c.cfg.Members == nil {
		return nil, errors.E(ctx, errors.CatUnimplemented, errors.TypeMembershipDisabled, errors.New("membership changes are not enabled on this server"))
	}
	if req.GetReplicaId() == 0 || req.GetRaftAddress() == "" {
		return nil, errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, errors.New("replica_id and raft_address are required"))
	}
	g := context.Pool(ctx).Group()
	for _, shardID := range c.cfg.Host.Hosted() {
		shardID := shardID
		g.Go(ctx, func(ctx context.Context) error {
			return c.cfg.Members.AddReplica(ctx, shardID, req.GetReplicaId(), req.GetRaftAddress())
		})
	}
	if err := g.Wait(ctx); err != nil {
		return nil, grpcErr(ctx, err)
	}
	if err := c.putMember(ctx, req); err != nil {
		return nil, grpcErr(ctx, err)
	}
	return &zuulv1.AddNodeResponse{}, nil
}

// RemoveNode removes a replica from every shard and from the meta shard.
func (c *ClusterServer) RemoveNode(ctx context.Context, req *zuulv1.RemoveNodeRequest) (*zuulv1.RemoveNodeResponse, error) {
	if err := c.authorize(ctx, authz.Admin); err != nil {
		return nil, err
	}
	if c.cfg.Members == nil {
		return nil, errors.E(ctx, errors.CatUnimplemented, errors.TypeMembershipDisabled, errors.New("membership changes are not enabled on this server"))
	}
	if req.GetReplicaId() == 0 {
		return nil, errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, errors.New("replica_id is required"))
	}
	g := context.Pool(ctx).Group()
	for _, shardID := range c.cfg.Host.Hosted() {
		shardID := shardID
		g.Go(ctx, func(ctx context.Context) error {
			return c.cfg.Members.RemoveReplica(ctx, shardID, req.GetReplicaId())
		})
	}
	if err := g.Wait(ctx); err != nil {
		return nil, grpcErr(ctx, err)
	}
	if err := c.deleteMember(ctx, req.GetReplicaId()); err != nil {
		return nil, grpcErr(ctx, err)
	}
	return &zuulv1.RemoveNodeResponse{}, nil
}

// putMember records the joining node's addresses in the meta shard.
func (c *ClusterServer) putMember(ctx context.Context, req *zuulv1.AddNodeRequest) error {
	member := &metapb.Member{
		ReplicaId:       req.GetReplicaId(),
		RaftAddress:     req.GetRaftAddress(),
		ZuulGrpcAddress: req.GetZuulGrpcAddress(),
		NodeHostId:      req.GetNodeHostId(),
	}
	b, err := (&metapb.MetaCommand{Cmd: &metapb.MetaCommand_Put{Put: member}}).MarshalVT()
	if err != nil {
		return fmt.Errorf("cluster.AddNode: marshal member: %w", err)
	}
	_, err = c.cfg.Proposer.Propose(ctx, c.cfg.MetaShardID, b)
	return err
}

// deleteMember removes a node from the meta shard.
func (c *ClusterServer) deleteMember(ctx context.Context, replicaID uint64) error {
	b, err := (&metapb.MetaCommand{Cmd: &metapb.MetaCommand_Delete{Delete: replicaID}}).MarshalVT()
	if err != nil {
		return fmt.Errorf("cluster.RemoveNode: marshal delete: %w", err)
	}
	_, err = c.cfg.Proposer.Propose(ctx, c.cfg.MetaShardID, b)
	return err
}
