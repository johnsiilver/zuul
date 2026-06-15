package consensus

import (
	"fmt"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/cmd"
	"github.com/johnsiilver/zuul/internal/deadline"
	dragonboat "github.com/johnsiilver/zuul/internal/dragonboat"
	"github.com/johnsiilver/zuul/internal/dragonboat/client"
	"github.com/johnsiilver/zuul/internal/dragonboat/config"
	"github.com/johnsiilver/zuul/internal/fsm"
	"github.com/johnsiilver/zuul/internal/meta"
	"github.com/johnsiilver/zuul/internal/meta/metapb"
	"github.com/johnsiilver/zuul/internal/metrics"

	gvfs "github.com/lni/goutils/vfs"
)

// proposeTimeout bounds a propose/read that arrives without its own deadline.
const proposeTimeout = 5 * time.Second

// Config configures one in-memory NodeHost participating in a set of shards.
type Config struct {
	// ReplicaID is this node's replica id within every shard (dragonboat NodeID).
	ReplicaID uint64
	// RaftAddr is the dragonboat Raft transport address (host:port).
	RaftAddr string
	// GRPCAddr is this node's client-facing / forwarding gRPC address, recorded in
	// the meta shard so other nodes can forward writes here.
	GRPCAddr string
	// DataDir names the NodeHost directory; it lives only in the in-memory FS.
	DataDir string
	// Shards are the lock Raft group ids this node hosts.
	Shards []uint64
	// MetaShardID is the topology (meta) shard's id, also hosted. 0 disables it.
	MetaShardID uint64
	// Members maps every initial replica id to its Raft address. Required unless
	// Join is set (a joining node learns members from the cluster). With full
	// replication, the same member set bootstraps every shard.
	Members map[uint64]string
	// Join starts this node's replicas in join mode (empty initial members): the
	// node must already have been added to each shard via AddNode on an existing
	// member. Used to grow the cluster, or to re-admit an amnesiac restart.
	Join bool
	// MutualTLS enables mutual TLS on the Raft transport plane. When set, CAFile,
	// CertFile, and KeyFile are required.
	MutualTLS bool
	// CAFile, CertFile, KeyFile are paths to the CA, node certificate, and node key
	// (PEM) for the Raft transport, used only when MutualTLS is set.
	CAFile, CertFile, KeyFile string
	// Gossip enables NodeHostID addressing and the internal gossip (memberlist)
	// service — for dynamic-IP deployments without stable DNS. Peers are addressed
	// by NodeHostID and gossip resolves it to the current RaftAddress. In this mode
	// the Members targets must be NodeHostID strings (see GossipTarget). Gossip is
	// unauthenticated/unencrypted, so MutualTLS is strongly recommended with it (New
	// logs a warning otherwise), but not required — security is the operator's call.
	Gossip bool
	// NodeHostID is this node's stable gossip identity; its dragonboat NodeHostID is
	// "nhid-<NodeHostID>". It must survive restarts. Required when Gossip.
	NodeHostID uint64
	// GossipBind is this node's gossip bind/advertise address (IP:port literal).
	// Required when Gossip.
	GossipBind string
	// GossipSeeds are peer gossip addresses to join. Required (non-empty) when Gossip.
	GossipSeeds []string
	// SnapshotEntries is how many Raft log entries trigger a snapshot for log
	// compaction (in the in-memory FS); 0 means the default (10000).
	SnapshotEntries uint64
	// CompactionOverhead is how many entries are retained after compaction;
	// 0 means the default (100).
	CompactionOverhead uint64
	// Notifier receives FSM ownership-change events (the watch hub). May be nil.
	Notifier fsm.Notifier
}

// GossipTarget returns the dragonboat member target for a node addressed by its
// gossip NodeHostID. In gossip mode the Members map values are these strings.
func GossipTarget(nodeHostID uint64) string {
	return fmt.Sprintf("nhid-%d", nodeHostID)
}

// nodeHostIDString is the node's dragonboat NodeHostID recorded in the meta shard:
// "nhid-<id>" in gossip mode, empty otherwise.
func nodeHostIDString(c Config) string {
	if c.Gossip {
		return GossipTarget(c.NodeHostID)
	}
	return ""
}

func (c Config) validate() error {
	switch {
	case c.ReplicaID == 0:
		return fmt.Errorf("consensus.Config: ReplicaID must be non-zero")
	case c.RaftAddr == "":
		return fmt.Errorf("consensus.Config: RaftAddr is required")
	case c.GRPCAddr == "":
		return fmt.Errorf("consensus.Config: GRPCAddr is required (it is recorded in the meta shard so peers can forward writes here)")
	case c.DataDir == "":
		return fmt.Errorf("consensus.Config: DataDir is required")
	case len(c.Shards) == 0:
		return fmt.Errorf("consensus.Config: at least one shard is required")
	}
	for _, shardID := range c.Shards {
		if c.MetaShardID != 0 && shardID == c.MetaShardID {
			return fmt.Errorf("consensus.Config: MetaShardID %d collides with a lock shard", c.MetaShardID)
		}
	}
	if c.MutualTLS && (c.CAFile == "" || c.CertFile == "" || c.KeyFile == "") {
		return fmt.Errorf("consensus.Config: MutualTLS requires CAFile, CertFile, and KeyFile")
	}
	if c.Gossip {
		switch {
		case c.NodeHostID == 0:
			return fmt.Errorf("consensus.Config: Gossip requires a non-zero NodeHostID")
		case c.GossipBind == "":
			return fmt.Errorf("consensus.Config: Gossip requires GossipBind")
		case len(c.GossipSeeds) == 0:
			return fmt.Errorf("consensus.Config: Gossip requires at least one GossipSeed")
		}
	}
	if c.Join {
		return nil
	}
	if len(c.Members) == 0 {
		return fmt.Errorf("consensus.Config: Members is required when not joining")
	}
	if _, ok := c.Members[c.ReplicaID]; !ok {
		return fmt.Errorf("consensus.Config: Members must include this node's ReplicaID %d", c.ReplicaID)
	}
	return nil
}

// Host is one in-memory NodeHost serving several shards. Lock state lives only in
// RAM (the NodeHost uses an in-memory VFS), so a full restart is intentionally
// amnesiac.
type Host struct {
	nh          *dragonboat.NodeHost
	sessions    map[uint64]*client.Session
	shards      []uint64 // lock shards (expiry sweeps these)
	hosted      []uint64 // every hosted shard (lock shards + meta shard)
	metaShardID uint64
	replicaID   uint64
	raftAddr    string
	grpcAddr    string
	nodeHostID  string
	closeOnce   sync.Once
}

// New boots a NodeHost on an in-memory VFS and starts every configured shard (lock
// shards plus the meta shard). For an initial member it bootstraps each shard with
// the full member set; for a joining node (Config.Join) it starts replicas in join
// mode. It returns once every hosted shard has a leader.
func New(ctx context.Context, cfg Config) (*Host, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	nhc := config.NodeHostConfig{
		NodeHostDir:    cfg.DataDir,
		RTTMillisecond: 5,
		RaftAddress:    cfg.RaftAddr,
		MutualTLS:      cfg.MutualTLS,
		CAFile:         cfg.CAFile,
		CertFile:       cfg.CertFile,
		KeyFile:        cfg.KeyFile,
	}
	nhc.Expert.FS = gvfs.NewMem() // Raft log + snapshots in RAM, never on disk.
	if cfg.Gossip {
		nhc.AddressByNodeHostID = true
		nhc.Gossip = config.GossipConfig{BindAddress: cfg.GossipBind, AdvertiseAddress: cfg.GossipBind, Seed: cfg.GossipSeeds}
		nhc.Expert.TestNodeHostID = cfg.NodeHostID // stable identity (in-mem FS won't persist one)
		if !cfg.MutualTLS {
			context.Log(ctx).Warn("gossip enabled without MutualTLS: gossip and Raft traffic is unauthenticated and unencrypted", "replica", cfg.ReplicaID)
		}
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("consensus.New: NewNodeHost: %w", err))
	}

	snapEntries := cfg.SnapshotEntries
	if snapEntries == 0 {
		snapEntries = 10_000
	}
	compaction := cfg.CompactionOverhead
	if compaction == 0 {
		compaction = 100
	}

	hosted := append([]uint64(nil), cfg.Shards...)
	if cfg.MetaShardID != 0 {
		hosted = append(hosted, cfg.MetaShardID)
	}

	create := newFactory(cfg.Notifier, cfg.MetaShardID)
	h := &Host{
		nh:          nh,
		sessions:    make(map[uint64]*client.Session, len(hosted)),
		shards:      append([]uint64(nil), cfg.Shards...),
		hosted:      hosted,
		metaShardID: cfg.MetaShardID,
		replicaID:   cfg.ReplicaID,
		raftAddr:    cfg.RaftAddr,
		grpcAddr:    cfg.GRPCAddr,
		nodeHostID:  nodeHostIDString(cfg),
	}

	initialMembers := cfg.Members
	if cfg.Join {
		initialMembers = map[uint64]string{}
	}
	for _, shardID := range hosted {
		rc := config.Config{
			NodeID:             cfg.ReplicaID,
			ClusterID:          shardID,
			CheckQuorum:        true,
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			SnapshotEntries:    snapEntries,
			CompactionOverhead: compaction,
		}
		if err := nh.StartConcurrentCluster(initialMembers, cfg.Join, create, rc); err != nil {
			nh.Stop()
			return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("consensus.New: StartConcurrentCluster shard %d: %w", shardID, err))
		}
		h.sessions[shardID] = nh.GetNoOPSession(shardID)
	}

	if err := h.waitReady(ctx); err != nil {
		nh.Stop()
		return nil, err
	}
	return h, nil
}

// Shards returns the lock shard ids this node hosts.
func (h *Host) Shards() []uint64 {
	return append([]uint64(nil), h.shards...)
}

// Hosted returns every shard id this node hosts (lock shards plus the meta shard).
func (h *Host) Hosted() []uint64 {
	return append([]uint64(nil), h.hosted...)
}

// ReplicaID returns this node's replica id.
func (h *Host) ReplicaID() uint64 {
	return h.replicaID
}

// waitReady blocks until every shard has a leader (so the cluster can serve) or
// ctx ends.
func (h *Host) waitReady(ctx context.Context) error {
	wait, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.allShardsHaveLeader() {
			return nil
		}
		select {
		case <-wait.Done():
			return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.New: timed out waiting for shard leadership: %w", wait.Err()))
		case <-tick.C:
		}
	}
}

// Ready reports whether this node can serve: every hosted shard has a leader. It
// backs the gRPC health service's readiness signal.
func (h *Host) Ready() bool {
	return h.allShardsHaveLeader()
}

// allShardsHaveLeader reports whether every hosted shard currently has a leader.
func (h *Host) allShardsHaveLeader() bool {
	for _, shardID := range h.hosted {
		if _, ok, err := h.nh.GetLeaderID(shardID); err != nil || !ok {
			return false
		}
	}
	return true
}

// Propose commits an opaque command (the marshalled command for whichever state
// machine owns shardID) and returns the marshalled result. Keeping it byte-oriented
// lets one propose/forward path serve both the lock shards and the meta shard. It
// must run on the shard's leader; on a follower it returns an error.
func (h *Host) Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error) {
	session, ok := h.sessions[shardID]
	if !ok {
		return nil, errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("consensus.Propose: shard %d not hosted here", shardID))
	}
	pctx, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := h.nh.SyncPropose(pctx, session, cmd)
	if err != nil {
		return nil, errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.Propose: %w", err))
	}
	return res.Data, nil
}

// Read performs a linearizable read on shardID; it works on any replica.
func (h *Host) Read(ctx context.Context, shardID uint64, query any) (any, error) {
	rctx, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	res, err := h.nh.SyncRead(rctx, shardID, query)
	if err != nil {
		return nil, errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.Read: %w", err))
	}
	return res, nil
}

// StaleRead performs a non-linearizable local read on shardID.
func (h *Host) StaleRead(shardID uint64, query any) (any, error) {
	return h.nh.StaleRead(shardID, query)
}

// IsLeader reports whether this node currently leads shardID.
func (h *Host) IsLeader(shardID uint64) bool {
	leader, ok, err := h.nh.GetLeaderID(shardID)
	return err == nil && ok && leader == h.replicaID
}

// LeaderID returns the replica id leading shardID, and whether a leader is known.
func (h *Host) LeaderID(shardID uint64) (uint64, bool) {
	leader, ok, err := h.nh.GetLeaderID(shardID)
	if err != nil {
		return 0, false
	}
	return leader, ok
}

// Close stops the NodeHost and frees its resources. It is safe to call more than
// once (the second call is a no-op), so a node can be deliberately stopped in a
// test and still have its cleanup run.
func (h *Host) Close() {
	h.closeOnce.Do(h.nh.Stop)
}

// Proposer commits a marshalled command to a shard (the forward dispatcher). It is
// declared here so WriteSelf can forward to the meta-shard leader without consensus
// importing the forward package.
type Proposer interface {
	Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error)
}

// WriteSelf records this node's member entry in the meta shard, forwarding through
// p to the meta-shard leader. It is idempotent (a Put), so it is safe to call on
// every boot. A no-op if there is no meta shard.
func (h *Host) WriteSelf(ctx context.Context, p Proposer) error {
	if h.metaShardID == 0 {
		return nil
	}
	member := &metapb.Member{
		ReplicaId:       h.replicaID,
		RaftAddress:     h.raftAddr,
		ZuulGrpcAddress: h.grpcAddr,
		NodeHostId:      h.nodeHostID,
	}
	b, err := (&metapb.MetaCommand{Cmd: &metapb.MetaCommand_Put{Put: member}}).MarshalVT()
	if err != nil {
		return errors.E(ctx, errors.CatInternal, errors.TypeMarshal, fmt.Errorf("consensus.WriteSelf: marshal: %w", err))
	}
	if _, err := p.Propose(ctx, h.metaShardID, b); err != nil {
		return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.WriteSelf: %w", err))
	}
	return nil
}

// AddReplicaShard adds a replica to a single shard. It must run on that shard's
// leader (the config change is dropped on a follower); the forward layer routes it
// there. It is idempotent: if the replica is already a member (e.g. a retry after a
// lost response), it is a no-op.
func (h *Host) AddReplicaShard(ctx context.Context, shardID, replicaID uint64, raftAddr string) error {
	rctx, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	if m, err := h.nh.SyncGetClusterMembership(rctx, shardID); err == nil {
		if _, ok := m.Nodes[replicaID]; ok {
			return nil // already a member
		}
	}
	if err := h.nh.SyncRequestAddNode(rctx, shardID, replicaID, raftAddr, 0); err != nil {
		return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.AddReplicaShard: shard %d: %w", shardID, err))
	}
	return nil
}

// RemoveReplicaShard removes a replica from a single shard. It must run on that
// shard's leader; the forward layer routes it there. Idempotent: if the replica is
// already gone, it is a no-op.
func (h *Host) RemoveReplicaShard(ctx context.Context, shardID, replicaID uint64) error {
	rctx, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	if m, err := h.nh.SyncGetClusterMembership(rctx, shardID); err == nil {
		if _, ok := m.Nodes[replicaID]; !ok {
			return nil // already not a member
		}
	}
	if err := h.nh.SyncRequestDeleteNode(rctx, shardID, replicaID, 0); err != nil {
		return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.RemoveReplicaShard: shard %d: %w", shardID, err))
	}
	return nil
}

// Drain transfers leadership of every shard this node currently leads to another
// member, so a graceful shutdown does not strand those shards in an election. It is
// best effort: it waits only for the shards a transfer was actually requested for (a
// shard with no other member has nowhere to go), and returns once those transfers
// land or ctx is done.
func (h *Host) Drain(ctx context.Context) {
	requested := map[uint64]bool{}
	for _, shardID := range h.hosted {
		if !h.IsLeader(shardID) {
			continue
		}
		if target := h.transferTarget(ctx, shardID); target != 0 {
			_ = h.nh.RequestLeaderTransfer(shardID, target) // async; verified by the poll below
			requested[shardID] = true
		}
	}
	if len(requested) == 0 {
		return
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for h.leadsAnyOf(requested) {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// leadsAnyOf reports whether this node still leads any of the given shards.
func (h *Host) leadsAnyOf(shards map[uint64]bool) bool {
	for shardID := range shards {
		if h.IsLeader(shardID) {
			return true
		}
	}
	return false
}

// transferTarget picks another member of shardID to receive leadership, or 0 if none.
func (h *Host) transferTarget(ctx context.Context, shardID uint64) uint64 {
	rctx, cancel := deadline.Ensure(ctx, proposeTimeout)
	if cancel != nil {
		defer cancel()
	}
	m, err := h.nh.SyncGetClusterMembership(rctx, shardID)
	if err != nil {
		return 0
	}
	for nodeID := range m.Nodes {
		if nodeID != h.replicaID {
			return nodeID
		}
	}
	return 0
}

// leadsAny reports whether this node currently leads any hosted shard.
func (h *Host) leadsAny() bool {
	for _, shardID := range h.hosted {
		if h.IsLeader(shardID) {
			return true
		}
	}
	return false
}

// MetaMember returns the topology entry for replicaID via a local stale read of the
// meta shard, and whether it is known.
func (h *Host) MetaMember(replicaID uint64) (*metapb.Member, bool) {
	if h.metaShardID == 0 {
		return nil, false
	}
	v, err := h.StaleRead(h.metaShardID, meta.MemberQuery{ReplicaID: replicaID})
	if err != nil {
		return nil, false
	}
	m, _ := v.(*metapb.Member)
	if m == nil {
		return nil, false
	}
	return m, true
}

// MetaList returns every topology entry via a linearizable read of the meta shard.
func (h *Host) MetaList(ctx context.Context) ([]*metapb.Member, error) {
	if h.metaShardID == 0 {
		return nil, nil
	}
	v, err := h.Read(ctx, h.metaShardID, meta.ListQuery{})
	if err != nil {
		return nil, errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.MetaList: %w", err))
	}
	out, _ := v.([]*metapb.Member)
	return out, nil
}

// ExpireDue reads the leases due at now on shardID and proposes a LeaseExpire for
// each. The FSM re-checks each deadline on apply, so it is safe against a keepalive
// that races in after the read.
func (h *Host) ExpireDue(ctx context.Context, shardID uint64, now int64) error {
	res, err := h.Read(ctx, shardID, fsm.DueLeasesQuery{NowUnixNano: now})
	if err != nil {
		return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.ExpireDue: read shard %d: %w", shardID, err))
	}
	due, _ := res.([]string)
	for _, clientID := range due {
		b, err := cmd.Expire(clientID, now).MarshalVT()
		if err != nil {
			return errors.E(ctx, errors.CatInternal, errors.TypeMarshal, fmt.Errorf("consensus.ExpireDue: marshal expire: %w", err))
		}
		if _, err := h.Propose(ctx, shardID, b); err != nil {
			return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("consensus.ExpireDue: propose expire for %q on shard %d: %w", clientID, shardID, err))
		}
	}
	metrics.LeasesExpired(ctx, len(due))
	return nil
}

// RunExpiry starts the leader-driven expiry sweep as a background task: every
// interval, for each shard this node leads, it expires due leases. now supplies the
// leader's clock. The task stops when ctx is cancelled.
func (h *Host) RunExpiry(ctx context.Context, interval time.Duration, now func() int64) error {
	boff, err := exponential.New()
	if err != nil {
		return errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("consensus.RunExpiry: backoff: %w", err))
	}
	task := func(ctx context.Context) error {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
				for _, shardID := range h.shards {
					if !h.IsLeader(shardID) {
						continue
					}
					if err := h.ExpireDue(ctx, shardID, now()); err != nil {
						context.Log(ctx).Warn("lease expiry sweep failed", "shard", shardID, "err", err.Error())
					}
				}
			}
		}
	}
	return context.Tasks(ctx).Run(ctx, "lease-expiry", task, boff)
}

// ensureDeadline returns ctx unchanged if it already has a deadline, otherwise a
// derived context with the default propose/read timeout. The returned cancel is
// nil when ctx was returned unchanged.
