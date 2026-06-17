package integration

import (
	"fmt"
	"math/rand"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// soakBackend abstracts the cluster the leader soak drives, so the same workload, invariant
// checker, and chaos loop run against either an in-process cluster (fast; for CI and
// correctness) or a set of real zuuld processes (production-faithful; for long memory
// soaks, where a node restart is a real process restart so dead memory is reclaimed).
//
// Mutating methods (restart/growShrink/ghostVictim) and liveNodes reads run only on the
// test goroutine — the chaos loop and the quiesce checker are serialized there — so the
// backends need no internal locking and may use the t.Fatalf-based helpers.
type soakBackend interface {
	// name identifies the backend in logs.
	name() string
	// endpoints is the stable client endpoint set (restarts reuse gRPC ports).
	endpoints() client.Endpoints
	// liveNodes is the current set of live members.
	liveNodes() []soakNode
	// awaitReady blocks until the cluster is healthy enough to check (all shards led).
	awaitReady(timeout time.Duration)
	// restart kills and restarts one live node (quorum-safe), hard (crash) or graceful.
	restart(rng *rand.Rand, hard bool)
	// growShrink adds a node then removes it (membership churn).
	growShrink(rng *rand.Rand)
	// ghostVictim selects a node to strand a broken client on: its single endpoint, a
	// survivor to read the reap from, and kill/restore funcs (kill keeps it down).
	ghostVictim(rng *rand.Rand) (endpoint string, survivor soakNode, kill, restore func())
	// teardown shuts the cluster down. It runs after the soaker has closed every client
	// (a node's graceful Close blocks on open client streams), before the framework's own
	// cleanups.
	teardown()
}

// soakNode is one cluster member as the checker and leak sampler see it.
type soakNode interface {
	id() uint64
	leader(ctx context.Context, key string) (*zuulv1.LeaderResponse, error)
	status(ctx context.Context, key string) (*zuulv1.StatusResponse, error)
	rssKiB() int // resident set size in KiB; 0 for an in-process node
}

// ----- in-process backend -----

type inProcBackend struct {
	t   *testing.T
	c   *cluster
	eps client.Endpoints
}

func newInProcBackend(t *testing.T, c *cluster) *inProcBackend {
	return &inProcBackend{t: t, c: c, eps: client.Endpoints{c.grpcAddrs[1], c.grpcAddrs[2], c.grpcAddrs[3]}}
}

func (b *inProcBackend) name() string                { return "in-process" }
func (b *inProcBackend) endpoints() client.Endpoints { return b.eps }

func (b *inProcBackend) liveNodes() []soakNode {
	out := make([]soakNode, 0, len(b.c.nodes))
	for _, n := range b.c.nodes {
		out = append(out, inProcNode{n})
	}
	return out
}

func (b *inProcBackend) awaitReady(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	shards := append([]uint64{metaShardID}, b.c.shards...)
	for time.Now().Before(deadline) {
		all := true
		for _, sh := range shards {
			if id, ok := b.c.nodes[0].host.LeaderID(sh); !ok || id == 0 {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.t.Logf("soak: not every shard had a leader within %s", timeout)
}

func (b *inProcBackend) restart(rng *rand.Rand, hard bool) {
	target := b.c.nodes[rng.Intn(len(b.c.nodes))]
	b.c.restartNode(b.t, target, hard)
}

func (b *inProcBackend) growShrink(rng *rand.Rand) {
	added := b.c.addNode(b.t)
	b.awaitReady(10 * time.Second)
	b.c.removeNode(b.t, added)
}

func (b *inProcBackend) ghostVictim(rng *rand.Rand) (string, soakNode, func(), func()) {
	target := b.c.nodes[rng.Intn(len(b.c.nodes))]
	ep := b.c.grpcAddrs[target.replicaID]
	survivor := inProcNode{b.c.otherNode(target.replicaID)}
	kill := func() { target.n.Stop() }
	restore := func() { b.c.restartNode(b.t, target, true) }
	return ep, survivor, kill, restore
}

// teardown closes the in-process nodes (after clients have closed, to avoid a node's
// graceful Close blocking on an open client stream). node.Close is idempotent, so the
// initial nodes' buildCluster t.Cleanups closing them again is harmless.
func (b *inProcBackend) teardown() {
	for _, n := range b.c.nodes {
		n.n.Close()
	}
}

type inProcNode struct{ n *node }

func (n inProcNode) id() uint64 { return n.n.replicaID }
func (n inProcNode) leader(ctx context.Context, key string) (*zuulv1.LeaderResponse, error) {
	return n.n.srv.Leader(ctx, &zuulv1.LeaderRequest{Name: key})
}
func (n inProcNode) status(ctx context.Context, key string) (*zuulv1.StatusResponse, error) {
	return n.n.srv.Status(ctx, &zuulv1.StatusRequest{Name: key})
}
func (n inProcNode) rssKiB() int { return 0 }

// ----- process backend (real zuuld) -----

type procBackend struct {
	t      *testing.T
	zuuld  string
	ids    []uint64 // current live replica ids
	raft   map[uint64]string
	grpc   map[uint64]string
	nodes  map[uint64]*procNode
	conns  map[uint64]*grpc.ClientConn
	eps    client.Endpoints
	nextID uint64
	uiBind string // read-only UI HTTP address, "" if disabled
	uiID   uint64 // replica id serving the UI; chaos never restarts it (keeps the UI stable)
}

// newProcBackend builds zuuld, boots numNodes processes, and waits for the cluster to form.
// When uiBind is non-empty the first node serves the read-only web UI there; that node is
// then exempt from chaos so the UI stays up on one address (quorum is still held — it plus
// at least one other node is always alive).
func newProcBackend(t *testing.T, numNodes int, uiBind string) *procBackend {
	t.Helper()
	b := &procBackend{
		t:      t,
		zuuld:  buildZuuld(t),
		raft:   map[uint64]string{},
		grpc:   map[uint64]string{},
		nodes:  map[uint64]*procNode{},
		conns:  map[uint64]*grpc.ClientConn{},
		uiBind: uiBind,
	}
	for i := 0; i < numNodes; i++ {
		id := uint64(i + 1)
		b.ids = append(b.ids, id)
		b.raft[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		b.grpc[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	b.nextID = uint64(numNodes) + 1
	if uiBind != "" {
		b.uiID = b.ids[0]
	}
	peers := peerList(b.ids, b.raft, b.grpc)
	for _, id := range b.ids {
		var extra []string
		if id == b.uiID {
			extra = []string{"-ui-enable", "-ui-bind", uiBind}
		}
		b.nodes[id] = launchNode(t, b.zuuld, id, b.raft[id], b.grpc[id], peers, false, extra...)
		b.conns[id] = dialAdmin(t, b.grpc[id])
	}
	b.eps = client.Endpoints{b.grpc[b.ids[0]], b.grpc[b.ids[1]], b.grpc[b.ids[2]]}
	t.Cleanup(b.shutdown)
	for _, id := range b.ids {
		awaitMembers(t, b.grpc[id], numNodes, 40*time.Second)
	}
	if uiBind != "" {
		t.Logf("soak: read-only UI serving at http://%s (replica %d, chaos-exempt)", uiBind, b.uiID)
	}
	return b
}

// pickVictim returns a random live replica id, never the UI node (so the UI stays up).
func (b *procBackend) pickVictim(rng *rand.Rand) uint64 {
	cands := make([]uint64, 0, len(b.ids))
	for _, id := range b.ids {
		if id != b.uiID {
			cands = append(cands, id)
		}
	}
	if len(cands) == 0 {
		return b.ids[rng.Intn(len(b.ids))]
	}
	return cands[rng.Intn(len(cands))]
}

func (b *procBackend) shutdown() {
	for _, c := range b.conns {
		_ = c.Close()
	}
	for _, n := range b.nodes {
		killNode(n)
	}
}

func (b *procBackend) name() string                { return "process" }
func (b *procBackend) endpoints() client.Endpoints { return b.eps }

// teardown is a no-op: the nodes are separate processes killed by SIGKILL via the
// t.Cleanup(shutdown) registered at construction (no graceful-stop stream-block to order
// against the clients), so there is nothing client-ordering-sensitive to do here.
func (b *procBackend) teardown() {}

func (b *procBackend) liveNodes() []soakNode {
	out := make([]soakNode, 0, len(b.ids))
	for _, id := range b.ids {
		out = append(out, procSoakNode{rid: id, conn: b.conns[id], pid: b.nodes[id].cmd.Process.Pid})
	}
	return out
}

func (b *procBackend) anyOther(exclude uint64) uint64 {
	for _, id := range b.ids {
		if id != exclude {
			return id
		}
	}
	return 0
}

func (b *procBackend) awaitReady(timeout time.Duration) {
	awaitMembers(b.t, b.grpc[b.ids[0]], len(b.ids), timeout)
}

// kill kills replica victimID (hard = SIGKILL crash, else SIGTERM graceful drain), removes
// it from membership through a survivor, and closes its admin conn.
func (b *procBackend) kill(victimID uint64, hard bool) (gport string, survivorGRPC string) {
	victim := b.nodes[victimID]
	gport = b.grpc[victimID]
	switch {
	case hard:
		killNode(victim)
	default:
		killNodeGraceful(victim)
	}
	_ = b.conns[victimID].Close()
	delete(b.conns, victimID)
	delete(b.nodes, victimID)
	b.ids = removeID(b.ids, victimID)
	survivorGRPC = b.grpc[b.anyOtherLive()]
	removeMember(b.t, survivorGRPC, victimID)
	return gport, survivorGRPC
}

func (b *procBackend) anyOtherLive() uint64 {
	return b.ids[0]
}

// join admits and launches a fresh replica reusing gport (fresh Raft port).
func (b *procBackend) join(gport, survivorGRPC string) uint64 {
	id := b.nextID
	b.nextID++
	newRaft := fmt.Sprintf("127.0.0.1:%d", freePort(b.t))
	addMember(b.t, survivorGRPC, id, newRaft, gport)
	b.raft[id] = newRaft
	b.grpc[id] = gport
	b.nodes[id] = launchNode(b.t, b.zuuld, id, newRaft, gport, "", true)
	b.conns[id] = dialAdmin(b.t, gport)
	b.ids = append(b.ids, id)
	awaitMembers(b.t, survivorGRPC, len(b.ids), 40*time.Second)
	return id
}

func (b *procBackend) restart(rng *rand.Rand, hard bool) {
	victimID := b.pickVictim(rng)
	gport, survivor := b.kill(victimID, hard)
	b.join(gport, survivor)
}

func (b *procBackend) growShrink(rng *rand.Rand) {
	// Add a fourth node on fresh ports, then remove it.
	id := b.nextID
	b.nextID++
	newRaft := fmt.Sprintf("127.0.0.1:%d", freePort(b.t))
	newGRPC := fmt.Sprintf("127.0.0.1:%d", freePort(b.t))
	survivor := b.grpc[b.ids[0]]
	addMember(b.t, survivor, id, newRaft, newGRPC)
	b.raft[id] = newRaft
	b.grpc[id] = newGRPC
	b.nodes[id] = launchNode(b.t, b.zuuld, id, newRaft, newGRPC, "", true)
	b.conns[id] = dialAdmin(b.t, newGRPC)
	b.ids = append(b.ids, id)
	awaitMembers(b.t, survivor, len(b.ids), 40*time.Second)

	// Shrink back.
	killNode(b.nodes[id])
	_ = b.conns[id].Close()
	delete(b.conns, id)
	delete(b.nodes, id)
	b.ids = removeID(b.ids, id)
	removeMember(b.t, b.grpc[b.ids[0]], id)
}

func (b *procBackend) ghostVictim(rng *rand.Rand) (string, soakNode, func(), func()) {
	victimID := b.pickVictim(rng)
	ep := b.grpc[victimID]
	survID := b.anyOther(victimID)
	survivor := procSoakNode{rid: survID, conn: b.conns[survID], pid: b.nodes[survID].cmd.Process.Pid}
	var gport, survGRPC string
	kill := func() { gport, survGRPC = b.kill(victimID, true) }
	restore := func() { b.join(gport, survGRPC) }
	return ep, survivor, kill, restore
}

type procSoakNode struct {
	rid  uint64
	conn *grpc.ClientConn
	pid  int
}

func (n procSoakNode) id() uint64 { return n.rid }
func (n procSoakNode) leader(ctx context.Context, key string) (*zuulv1.LeaderResponse, error) {
	return zuulv1.NewElectionClient(n.conn).Leader(ctx, &zuulv1.LeaderRequest{Name: key})
}
func (n procSoakNode) status(ctx context.Context, key string) (*zuulv1.StatusResponse, error) {
	return zuulv1.NewLockerClient(n.conn).Status(ctx, &zuulv1.StatusRequest{Name: key})
}
func (n procSoakNode) rssKiB() int { return rssKiB(n.pid) }

// gracePeriod is how long a SIGTERM'd node is given to drain (transfer Raft leadership)
// before it is SIGKILL'd — modelling a real orchestrator's termination grace period.
const gracePeriod = 5 * time.Second

// killNodeGraceful models a real graceful shutdown: SIGTERM (zuuld drains shard
// leadership), a grace period, then SIGKILL. The SIGKILL is required, not just a fallback:
// zuuld's SIGTERM handler drains and then calls Close, whose gRPC GracefulStop waits for
// in-flight streams to end — but the workload's long-lived client KeepAlive/Observe
// streams never end, so the process would otherwise hang forever (and cmd.Wait with it).
// This is exactly how Kubernetes behaves (terminationGracePeriodSeconds then SIGKILL); the
// drain still happens, only the lingering process is forced down. (It also surfaces a real
// zuuld observation: its graceful shutdown has no bounded self-fallback to a hard stop.)
func killNodeGraceful(n *procNode) {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
		time.Sleep(gracePeriod)
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait() // returns promptly once SIGKILL has reaped the process
	}
	if n.log != nil {
		_ = n.log.Close()
	}
}

// removeID returns ids with the first occurrence of v removed.
func removeID(ids []uint64, v uint64) []uint64 {
	out := ids[:0]
	for _, id := range ids {
		if id != v {
			out = append(out, id)
		}
	}
	return out
}
