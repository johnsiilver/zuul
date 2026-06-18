package integration

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// This is the prototype validator for the out-of-process soak harness. It boots three
// REAL zuuld processes (insecure, in-memory store), drives a light client workload, then
// repeatedly hard-kills and restarts one node — a true process restart, as in production
// — while sampling each node process's RSS and the test driver's own RSS. The premise it
// proves: with real process restarts, the dead node's memory is reclaimed by the OS, so
// total live memory stays bounded across many restarts (unlike the in-process harness,
// where killed dragonboat NodeHosts accumulate ~50-65 MB each in one address space).
//
// Run: go test ./internal/integration -run TestProcSoakProto -zuul.procproto=2m -timeout 5m -v

var procProtoDur = flag.Duration("zuul.procproto", 0, "process-soak prototype run time; 0 (default) skips")

const procShards = 8

// procNode is one zuuld child process.
type procNode struct {
	id   uint64
	raft string
	grpc string
	cmd  *exec.Cmd
	log  *os.File
}

func TestProcSoakProto(t *testing.T) {
	if testing.Short() || *procProtoDur <= 0 {
		t.Skip("process-soak prototype: set -zuul.procproto=<dur> to run")
	}
	ctx := t.Context()
	zuuld := buildZuuld(t)

	// Three nodes; gRPC ports stay stable across restarts so client endpoints stay valid.
	ids := []uint64{1, 2, 3}
	raft := map[uint64]string{}
	grpc := map[uint64]string{}
	for _, id := range ids {
		raft[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		grpc[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	peers := peerList(ids, raft, grpc)

	nodes := map[uint64]*procNode{}
	for _, id := range ids {
		nodes[id] = launchNode(t, zuuld, id, raft[id], grpc[id], peers, false)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			killNode(n)
		}
	})
	endpoints := client.Endpoints{grpc[1], grpc[2], grpc[3]}

	// Wait for the cluster to form (every node sees all three members).
	for _, id := range ids {
		awaitMembers(t, grpc[id], 3, 30*time.Second)
	}
	t.Logf("procproto: 3 zuuld up; endpoints=%v", endpoints)

	// Light workload: a few real clients churning locks + elections so each shard takes
	// Raft writes (memory would grow if restarts leaked).
	runCtx, cancel := context.WithCancel(ctx)
	g := context.Pool(runCtx).Group()
	for w := 0; w < 4; w++ {
		w := w
		g.Go(runCtx, func(c context.Context) error { return procWorker(c, endpoints, w) })
	}

	// Drive restarts + RSS sampling on the test goroutine.
	type sample struct {
		t        time.Duration
		restarts int
		liveRSS  int // sum of live node RSS (KiB)
		driver   int // driver process RSS (KiB)
	}
	var samples []sample
	start := time.Now()
	nextID := uint64(4)
	restarts := 0
	deadline := time.Now().Add(*procProtoDur)
	for time.Now().Before(deadline) {
		time.Sleep(3 * time.Second)

		// Hard-kill a random live node and restart it reusing its gRPC port.
		victimID := ids[restarts%len(ids)]
		victim := nodes[victimID]
		gport := victim.grpc
		killNode(victim)
		delete(nodes, victimID)
		survivor := anyLive(nodes)
		removeMember(t, survivor.grpc, victimID)

		newID := nextID
		nextID++
		newRaft := fmt.Sprintf("127.0.0.1:%d", freePort(t))
		addMember(t, survivor.grpc, newID, newRaft, gport)
		n := launchNode(t, zuuld, newID, newRaft, gport, "", true) // join: no peers
		nodes[newID] = n
		// Replace the victim slot in ids with the new id so round-robin keeps working.
		for i := range ids {
			if ids[i] == victimID {
				ids[i] = newID
			}
		}
		raft[newID] = newRaft
		grpc[newID] = gport
		awaitMembers(t, survivor.grpc, 3, 30*time.Second)
		restarts++

		// Sample memory.
		live := 0
		for _, n := range nodes {
			live += rssKiB(n.cmd.Process.Pid)
		}
		s := sample{t: time.Since(start).Round(time.Second), restarts: restarts, liveRSS: live, driver: rssKiB(os.Getpid())}
		samples = append(samples, s)
		t.Logf("procproto: t=%s restarts=%d liveNodeRSS=%dMiB driverRSS=%dMiB", s.t, s.restarts, s.liveRSS>>10, s.driver>>10)
	}

	cancel()
	_ = g.Wait(runCtx)

	// Verdict: with real restarts, total live-node RSS must NOT grow ~linearly with
	// restart count (the in-process harness grew ~60 MiB/restart). Compare the last
	// sample to the first; allow generous headroom for warm-up/GC, but a true leak would
	// blow far past it.
	if len(samples) < 4 {
		t.Fatalf("procproto: only %d samples; run longer", len(samples))
	}
	first, last := samples[1], samples[len(samples)-1] // skip sample[0] warm-up
	t.Logf("procproto: SUMMARY restarts=%d liveNodeRSS %dMiB -> %dMiB ; driverRSS %dMiB -> %dMiB over %d restarts",
		last.restarts, first.liveRSS>>10, last.liveRSS>>10, first.driver>>10, last.driver>>10, last.restarts-first.restarts)
	if last.liveRSS > first.liveRSS*2 {
		t.Errorf("procproto: live node RSS grew >2x across %d restarts (%dMiB -> %dMiB) — restarts may be leaking",
			last.restarts-first.restarts, first.liveRSS>>10, last.liveRSS>>10)
	}
	if last.driver > first.driver*2 && (last.driver-first.driver)>>10 > 100 {
		t.Errorf("procproto: driver RSS grew >2x and +%dMiB — driver-side leak", (last.driver-first.driver)>>10)
	}
}

// procWorker churns locks and elections against the cluster via a real client.
func procWorker(ctx context.Context, eps client.Endpoints, w int) error {
	cl, err := client.New(ctx, eps, client.WithClientID(fmt.Sprintf("pw-%d", w)), client.WithTTL(5*time.Second))
	if err != nil {
		return nil
	}
	defer func() { _ = cl.Close() }()
	for ctx.Err() == nil {
		mu := cl.NewMutex(fmt.Sprintf("/soak/proc/lock-%d", w%3))
		if ok, err := mu.TryLock(ctx); err == nil && ok {
			sleepCtx(ctx, 100*time.Millisecond)
			_ = mu.Unlock(ctx)
		}
		el := cl.NewElection(fmt.Sprintf("/soak/proc/elect-%d", w%2))
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		err := el.Campaign(cctx, []byte("v"))
		cancel()
		if err == nil {
			sleepCtx(ctx, 100*time.Millisecond)
			_ = el.Resign(ctx)
		}
		sleepCtx(ctx, 50*time.Millisecond)
	}
	return nil
}

// ----- process + gRPC helpers -----

// buildZuuld compiles the zuuld binary to a temp path.
func buildZuuld(t *testing.T) string {
	t.Helper()
	out := t.TempDir() + "/zuuld"
	cmd := exec.Command("go", "build", "-o", out, "github.com/johnsiilver/zuul/cmd/zuuld")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("buildZuuld: %s\n%s", err, b)
	}
	return out
}

// peerList renders the -peers value (id=raftAddr=grpcAddr,...).
func peerList(ids []uint64, raft, grpc map[uint64]string) string {
	var parts []string
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d=%s=%s", id, raft[id], grpc[id]))
	}
	return strings.Join(parts, ",")
}

// launchNode starts a zuuld process. With join set it omits -peers (the cluster AddNode'd
// it already); otherwise -peers carries the initial members. extraArgs are appended as-is
// (e.g. the UI flags for the designated UI node).
func launchNode(t *testing.T, zuuld string, id uint64, raft, grpc, peers string, join bool, extraArgs ...string) *procNode {
	t.Helper()
	args := []string{"-id", strconv.FormatUint(id, 10), "-raft", raft, "-grpc", grpc, "-shards", strconv.Itoa(procShards)}
	switch {
	case join:
		args = append(args, "-join")
	default:
		args = append(args, "-peers", peers)
	}
	args = append(args, extraArgs...)
	logf, err := os.Create(fmt.Sprintf("%s/zuuld-%d.log", t.TempDir(), id))
	if err != nil {
		t.Fatalf("launchNode(%d): log: %s", id, err)
	}
	cmd := exec.Command(zuuld, args...)
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("launchNode(%d): start: %s", id, err)
	}
	return &procNode{id: id, raft: raft, grpc: grpc, cmd: cmd, log: logf}
}

// killNode hard-kills a node process and reaps it.
func killNode(n *procNode) {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
	if n.log != nil {
		_ = n.log.Close()
	}
}

// anyLive returns any node from the map.
func anyLive(nodes map[uint64]*procNode) *procNode {
	for _, n := range nodes {
		return n
	}
	return nil
}

// dialAdmin opens an insecure gRPC connection to a node for admin/read RPCs.
func dialAdmin(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialAdmin(%s): %s", addr, err)
	}
	return conn
}

// awaitMembers waits until the node at addr reports at least want members.
func awaitMembers(t *testing.T, addr string, want int, timeout time.Duration) {
	t.Helper()
	conn := dialAdmin(t, addr)
	defer conn.Close()
	cl := zuulv1.NewClusterClient(conn)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := cl.Members(t.Context(), &zuulv1.MembersRequest{})
		if err == nil && len(resp.GetMembers()) >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("awaitMembers(%s): never reached %d members", addr, want)
}

// addMember admits a new replica through a live node.
func addMember(t *testing.T, addr string, id uint64, raft, grpc string) {
	t.Helper()
	conn := dialAdmin(t, addr)
	defer conn.Close()
	if _, err := zuulv1.NewClusterClient(conn).AddNode(t.Context(), &zuulv1.AddNodeRequest{ReplicaId: id, RaftAddress: raft, ZuulGrpcAddress: grpc}); err != nil {
		t.Fatalf("addMember(%d): %s", id, err)
	}
}

// removeMember removes a replica through a live node.
func removeMember(t *testing.T, addr string, id uint64) {
	t.Helper()
	conn := dialAdmin(t, addr)
	defer conn.Close()
	if _, err := zuulv1.NewClusterClient(conn).RemoveNode(t.Context(), &zuulv1.RemoveNodeRequest{ReplicaId: id}); err != nil {
		t.Fatalf("removeMember(%d): %s", id, err)
	}
}

// rssKiB returns the resident set size of a pid in KiB (via ps), or 0.
func rssKiB(pid int) int {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0
	}
	return n
}

// sleepCtx is a ctx-aware sleep.
func sleepCtx(ctx context.Context, d time.Duration) {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
	case <-tm.C:
	}
}
