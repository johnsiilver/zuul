// Package integration wires the full Zuul stack (consensus host with lock + meta
// shards, watch hub, forward dispatcher + server, session manager, gRPC server)
// into an in-process N-node cluster and exercises multi-master behavior end to end:
// forwarded writes, cross-node fencing, leader failover, meta-shard topology, and
// dynamic membership (grow / shrink / amnesiac re-add).
package integration

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/consensus"
	zuulnode "github.com/johnsiilver/zuul/internal/node"
	"github.com/johnsiilver/zuul/internal/router"
	"github.com/johnsiilver/zuul/internal/server"
	"github.com/johnsiilver/zuul/internal/session"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

const (
	testNow     = int64(1_000_000_000)
	testTTLms   = int64(30_000)
	metaShardID = uint64(100_000)
)

// node is one cluster member, built through the production node.New path. It keeps
// the real *zuulnode.Node for lifecycle (Serve/Start/Close) and surfaces the
// in-process service handles the tests drive directly.
type node struct {
	replicaID uint64
	n         *zuulnode.Node
	host      *consensus.Host
	sessions  *session.Manager
	srv       *server.Server
	cluster   *server.ClusterServer
}

// tlsPaths are the PEM file paths securing all three planes (Raft, forward, client).
type tlsPaths struct {
	ca, cert, key string
}

// cluster is an in-process Zuul cluster that can grow and shrink at runtime.
type cluster struct {
	nodes      []*node
	shards     []uint64
	router     *router.Router
	members    map[uint64]string // replica id -> raft address
	grpcAddrs  map[uint64]string // replica id -> forwarding/grpc address
	gossip     map[uint64]string // replica id -> gossip address (gossip mode only)
	useGossip  bool
	authorizer authz.Authorizer
	nextID     uint64
	now        func() int64
	tls        *tlsPaths // nil => insecure
}

// clusterOpts configures a test cluster.
type clusterOpts struct {
	tls        *tlsPaths
	gossip     bool
	authorizer authz.Authorizer
}

// newCluster boots an insecure in-process cluster.
func newCluster(t testing.TB, numNodes, numShards int) *cluster {
	return buildCluster(t, numNodes, numShards, clusterOpts{})
}

// newSecureCluster boots a cluster with mutual TLS on all three planes.
func newSecureCluster(t testing.TB, numNodes, numShards int, tls *tlsPaths) *cluster {
	return buildCluster(t, numNodes, numShards, clusterOpts{tls: tls})
}

// newAuthzCluster boots a mutual-TLS cluster enforcing the given authorizer.
func newAuthzCluster(t testing.TB, numNodes, numShards int, tls *tlsPaths, a authz.Authorizer) *cluster {
	return buildCluster(t, numNodes, numShards, clusterOpts{tls: tls, authorizer: a})
}

// newGossipCluster boots a cluster addressed by NodeHostID over gossip (which
// requires mutual TLS).
func newGossipCluster(t testing.TB, numNodes, numShards int, tls *tlsPaths) *cluster {
	return buildCluster(t, numNodes, numShards, clusterOpts{tls: tls, gossip: true})
}

// buildCluster boots numNodes fully-replicated members over numShards lock shards (plus
// the meta shard), each through the production node.New path, and waits for the meta
// shard to record every node. opts.tls runs mutual TLS on every plane; opts.gossip
// addresses nodes by NodeHostID over gossip.
func buildCluster(t testing.TB, numNodes, numShards int, opts clusterOpts) *cluster {
	t.Helper()
	ctx := t.Context()

	shards := make([]uint64, numShards)
	for i := range shards {
		shards[i] = uint64(i + 1)
	}
	r, err := router.New(shards)
	if err != nil {
		t.Fatalf("buildCluster: router.New: %s", err)
	}

	c := &cluster{
		shards:     shards,
		router:     r,
		members:    map[uint64]string{},
		grpcAddrs:  map[uint64]string{},
		gossip:     map[uint64]string{},
		useGossip:  opts.gossip,
		nextID:     uint64(numNodes) + 1,
		now:        func() int64 { return testNow },
		tls:        opts.tls,
		authorizer: opts.authorizer,
	}

	// Boot the cluster, retrying on a transient port collision. freePort hands out a
	// port via a closed TCP listener, which does not reserve the UDP side, so a gossip
	// bind can lose a race to a port some other allocation (often a prior test's
	// not-yet-released socket) still holds. A retry with fresh ports clears it; failed
	// attempts are torn down immediately so their cleanups never leak past the test.
	const bootAttempts = 4
	var nodes []*node
	for attempt := 1; ; attempt++ {
		c.allocPorts(t, numNodes, opts.gossip)
		dbMembers, gossipSeeds := c.dragonboatMembers(opts.gossip)
		built, err := c.bootNodes(ctx, numNodes, dbMembers, opts.gossip, gossipSeeds)
		if err == nil {
			nodes = built
			break
		}
		closeNodes(built)
		if attempt >= bootAttempts || !isTransientBind(err) {
			t.Fatalf("buildCluster: booting nodes: %s", err)
		}
	}

	for _, n := range nodes {
		t.Cleanup(n.n.Close)
	}
	c.nodes = nodes
	c.awaitMembership(t, numNodes)
	return c
}

// allocPorts assigns each member a fresh Raft, gRPC, and (in gossip mode) gossip
// loopback address, replacing any from a prior boot attempt.
func (c *cluster) allocPorts(t testing.TB, numNodes int, gossip bool) {
	t.Helper()
	c.members = map[uint64]string{}
	c.grpcAddrs = map[uint64]string{}
	c.gossip = map[uint64]string{}
	for i := 0; i < numNodes; i++ {
		id := uint64(i + 1)
		c.members[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		c.grpcAddrs[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		if gossip {
			c.gossip[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		}
	}
}

// dragonboatMembers returns the bootstrap member targets and gossip seeds for the
// current ports. In gossip mode peers are addressed by NodeHostID (== replicaID
// here) and every node's gossip address is a seed.
func (c *cluster) dragonboatMembers(gossip bool) (map[uint64]string, []string) {
	if !gossip {
		return c.members, nil
	}
	dbMembers := map[uint64]string{}
	var seeds []string
	for id := range c.members {
		dbMembers[id] = consensus.GossipTarget(id)
		seeds = append(seeds, c.gossip[id])
	}
	return dbMembers, seeds
}

// bootNodes boots all members concurrently through node.New: each blocks until its
// shards elect leaders, which only happens once a quorum is up. Building each node the
// way the binary does — same interceptor chain, dial credentials, expiry sweep, health
// watcher, and graceful shutdown — keeps the harness from masking bugs that live in
// that assembly. The returned slice holds the booted nodes (nil where one failed) so a
// failed batch can be torn down; ctx is the durable test context so the nodes'
// background tasks outlive the boot group.
func (c *cluster) bootNodes(ctx context.Context, numNodes int, dbMembers map[uint64]string, gossip bool, gossipSeeds []string) ([]*node, error) {
	nodes := make([]*node, numNodes)
	g := context.Pool(ctx).Group()
	for i := 0; i < numNodes; i++ {
		i := i
		id := uint64(i + 1)
		g.Go(ctx, func(_ context.Context) error {
			cfg := c.baseConfig(id)
			cfg.Members = dbMembers
			if gossip {
				cfg.Gossip = true
				cfg.NodeHostID = id
				cfg.GossipBind = c.gossip[id]
				cfg.GossipSeeds = gossipSeeds
			}
			n, err := c.startNode(ctx, cfg)
			if err != nil {
				return fmt.Errorf("node %d: %w", id, err)
			}
			nodes[i] = n
			return nil
		})
	}
	return nodes, g.Wait(ctx)
}

// baseConfig is the node.Config shared by fresh and joining members (no Members,
// gossip, or Join set — callers add those).
func (c *cluster) baseConfig(id uint64) zuulnode.Config {
	mtls, caFile, certFile, keyFile := c.tlsFields()
	return zuulnode.Config{
		ReplicaID:   id,
		RaftAddr:    c.members[id],
		GRPCAddr:    c.grpcAddrs[id],
		DataDir:     fmt.Sprintf("zuul-node-%d", id),
		Shards:      c.shards,
		MetaShardID: metaShardID,
		Seed:        c.grpcAddrs,
		MutualTLS:   mtls,
		CAFile:      caFile,
		CertFile:    certFile,
		KeyFile:     keyFile,
		Now:         c.now,
		Authorizer:  c.authorizer,
	}
}

// startNode builds a node via node.New, serves its gRPC stack, and starts its
// background tasks, returning the test wrapper. ctx must be the durable test context
// so the node's tasks live for the whole test. It registers no cleanup — the caller
// owns the node's lifetime (Close on success, or on teardown of a failed boot batch) —
// and returns errors rather than calling t.Fatal, so it is safe in a boot goroutine.
func (c *cluster) startNode(ctx context.Context, cfg zuulnode.Config) (*node, error) {
	zn, err := zuulnode.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		zn.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}
	context.Pool(ctx).Submit(ctx, func() { _ = zn.Serve(lis) })
	if err := zn.Start(ctx); err != nil {
		zn.Close()
		return nil, fmt.Errorf("start: %w", err)
	}
	return &node{
		replicaID: cfg.ReplicaID,
		n:         zn,
		host:      zn.Host(),
		sessions:  zn.Sessions(),
		srv:       zn.Server(),
		cluster:   zn.ClusterServer(),
	}, nil
}

// closeNodes shuts down every non-nil node (tearing down a failed boot batch).
func closeNodes(nodes []*node) {
	for _, n := range nodes {
		if n != nil {
			n.n.Close()
		}
	}
}

// isTransientBind reports whether err is a retryable address-in-use bind failure.
func isTransientBind(err error) bool {
	return err != nil && strings.Contains(err.Error(), "address already in use")
}

// awaitMembership waits until the meta shard records at least want members — the
// node.Start announce is a background, retried write, so topology converges
// asynchronously just as it does in a real deployment.
func (c *cluster) awaitMembership(t testing.TB, want int) {
	t.Helper()
	ctx := t.Context()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		members, err := c.nodes[0].host.MetaList(ctx)
		if err == nil && len(members) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("awaitMembership: meta shard never recorded %d members", want)
}

// addNode grows the cluster by one: it admits a new replica through an existing
// node's Cluster API, boots it in join mode, and assembles its stack.
func (c *cluster) addNode(t *testing.T) *node {
	t.Helper()
	ctx := t.Context()

	id := c.nextID
	c.nextID++
	raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	c.members[id] = raftAddr
	c.grpcAddrs[id] = grpcAddr

	admit := c.nodes[0]
	if _, err := admit.cluster.AddNode(ctx, &zuulv1.AddNodeRequest{ReplicaId: id, RaftAddress: raftAddr, ZuulGrpcAddress: grpcAddr}); err != nil {
		t.Fatalf("addNode: AddNode(%d): %s", id, err)
	}

	cfg := c.baseConfig(id)
	cfg.Join = true
	n, err := c.startNode(ctx, cfg)
	if err != nil {
		t.Fatalf("addNode: boot join node %d: %s", id, err)
	}
	t.Cleanup(n.n.Close)
	c.nodes = append(c.nodes, n)
	c.awaitMembership(t, len(c.nodes))
	return n
}

// removeNode shrinks the cluster by removing target through a surviving node's
// Cluster API, then stops it.
func (c *cluster) removeNode(t *testing.T, target *node) {
	t.Helper()
	ctx := t.Context()

	admit := c.otherNode(target.replicaID)
	if admit == nil {
		t.Fatalf("removeNode: no surviving node to issue removal of %d", target.replicaID)
	}
	if _, err := admit.cluster.RemoveNode(ctx, &zuulv1.RemoveNodeRequest{ReplicaId: target.replicaID}); err != nil {
		t.Fatalf("removeNode: RemoveNode(%d): %s", target.replicaID, err)
	}
	target.n.Close()

	live := c.nodes[:0]
	for _, n := range c.nodes {
		if n.replicaID != target.replicaID {
			live = append(live, n)
		}
	}
	c.nodes = live
}

// leaderReplica returns the replica id leading shardID (waiting briefly for one).
func (c *cluster) leaderReplica(t testing.TB, shardID uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if id, ok := c.nodes[0].host.LeaderID(shardID); ok && id != 0 {
			return id
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("leaderReplica: shard %d never elected a leader", shardID)
	return 0
}

// nodeByReplica returns the live node with the given replica id, or nil.
func (c *cluster) nodeByReplica(id uint64) *node {
	for _, n := range c.nodes {
		if n != nil && n.replicaID == id {
			return n
		}
	}
	return nil
}

// otherNode returns any live node whose replica id is not excludeID.
func (c *cluster) otherNode(excludeID uint64) *node {
	for _, n := range c.nodes {
		if n != nil && n.replicaID != excludeID {
			return n
		}
	}
	return nil
}

// freePort returns a currently-free loopback TCP port.
func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %s", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// tlsFields returns the MutualTLS flag and PEM file paths for consensus.Config.
func (c *cluster) tlsFields() (bool, string, string, string) {
	if c.tls == nil {
		return false, "", "", ""
	}
	return true, c.tls.ca, c.tls.cert, c.tls.key
}

// openSession registers a client session on n so its EnsureLease/Lock calls work.
func openSession(n *node, clientID string) {
	n.sessions.Open(clientID, testTTLms)
}

// awaitStatus reads a key's status from n, retrying through the transient window
// right after a leadership or membership change (a linearizable read can briefly
// time out while the shard re-stabilizes — a real client retries the same way).
func awaitStatus(t *testing.T, n *node, key string) *zuulv1.StatusResponse {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		st, err := n.srv.Status(t.Context(), &zuulv1.StatusRequest{Name: key})
		if err == nil {
			return st
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("awaitStatus(%s): never succeeded: %s", key, lastErr)
	return nil
}

// awaitLeaderChange waits until shardID has a leader other than oldID.
func awaitLeaderChange(t *testing.T, n *node, shardID, oldID uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if id, ok := n.host.LeaderID(shardID); ok && id != 0 && id != oldID {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("awaitLeaderChange: shard %d never moved off replica %d", shardID, oldID)
	return 0
}
