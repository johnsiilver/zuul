package integration

// Scaling benchmarks. These answer three capacity questions on a single in-memory
// node, and are meant to be run pinned to ONE processor so the numbers reflect a
// single core doing all the work (Raft commit, FSM apply, gRPC, scheduling):
//
//	GOMAXPROCS=1 go test -run '^$' -bench 'Capacity|LatencyVsConnections|ConnectionMemory' \
//	  -benchmem -benchtime 2s ./internal/integration/
//
// (equivalently, add -cpu 1). What each one tells you:
//
//   - BenchmarkLockCapacity / BenchmarkLeaderCapacity: ns/op is acquire latency, the
//     locks/sec (leaders/sec) custom metric is throughput, and heapB/lock
//     (heapB/leader) is the retained heap per held lock/leader — divide your memory
//     budget by it to estimate how many you can hold. B/op (from -benchmem) is the
//     allocation churn per acquire.
//   - BenchmarkLatencyVsConnections/conns=N: ns/op is the mean lock+unlock round-trip
//     latency with N real gRPC connections driving the node concurrently. Watch how it
//     grows as N rises on one core; ops/sec is aggregate throughput.
//   - BenchmarkConnectionMemory: heapB/conn is the retained heap per live client
//     connection+session (client and server side, same process).

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// retainedPerOp reports the heap bytes retained per iteration as a custom metric,
// measured as the live-heap delta from baseline after a forced GC. It must be called
// with the timer stopped.
func retainedPerOp(b *testing.B, baseline uint64, n int, unit string) {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	delta := float64(0)
	if m.HeapAlloc > baseline {
		delta = float64(m.HeapAlloc - baseline)
	}
	b.ReportMetric(delta/float64(n), unit)
}

// heapBaseline forces a GC and returns the current live heap, for use as the baseline
// in retainedPerOp.
func heapBaseline() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// BenchmarkLockCapacity acquires b.N distinct locks and holds them all, so ns/op is the
// per-lock acquire latency, locks/sec is throughput, and heapB/lock is the retained
// memory of one held lock (key + lease + FSM/Raft state).
func BenchmarkLockCapacity(b *testing.B) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	n := c.nodes[0]
	openSession(n, "bench")

	base := heapBaseline()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: "/bench/lock/" + strconv.Itoa(i), ClientId: "bench"})
		if err != nil {
			b.Fatalf("BenchmarkLockCapacity: TryLock: %s", err)
		}
		if !resp.GetAcquired() {
			b.Fatalf("BenchmarkLockCapacity: lock %d not acquired", i)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "locks/sec")
	retainedPerOp(b, base, b.N, "heapB/lock")
}

// BenchmarkLeaderCapacity wins b.N distinct (uncontended) elections and holds the
// leadership of each, so ns/op is per-leader campaign latency, leaders/sec is
// throughput, and heapB/leader is the retained memory of one held leadership.
func BenchmarkLeaderCapacity(b *testing.B) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	n := c.nodes[0]
	openSession(n, "bench")

	base := heapBaseline()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := n.srv.Campaign(ctx, &zuulv1.CampaignRequest{Name: "/bench/elect/" + strconv.Itoa(i), ClientId: "bench"})
		if err != nil {
			b.Fatalf("BenchmarkLeaderCapacity: Campaign: %s", err)
		}
		if !resp.GetLeadership() {
			b.Fatalf("BenchmarkLeaderCapacity: election %d not won", i)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "leaders/sec")
	retainedPerOp(b, base, b.N, "heapB/leader")
}

// BenchmarkLatencyVsConnections drives one node with a growing number of real gRPC
// client connections, each on its own key, and reports how operation latency and
// throughput move as concurrency rises on one core. One op == one Lock + one Unlock
// (two Raft proposals). Two latency views are reported, and they tell opposite halves
// of the story:
//
//   - ns/op (and ops/sec) is the AGGREGATE: total wall time / total ops. It improves
//     with connections because Raft batches concurrent in-flight proposals.
//   - p50-ns / p99-ns is the OBSERVED per-request latency a single caller sees. It
//     rises with connections because each request waits behind more in-flight work.
func BenchmarkLatencyVsConnections(b *testing.B) {
	for _, conns := range []int{1, 2, 4, 8, 16, 32, 64} {
		b.Run(fmt.Sprintf("conns=%d", conns), func(b *testing.B) {
			benchLatencyConns(b, conns)
		})
	}
}

func benchLatencyConns(b *testing.B, conns int) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	addr := c.grpcAddrs[c.nodes[0].replicaID]

	clients := make([]*client.Client, conns)
	for i := range clients {
		cl, err := client.New(ctx, client.Endpoints{addr}, client.WithClientID(fmt.Sprintf("bench-%d", i)), client.WithTTL(30*time.Second))
		if err != nil {
			b.Fatalf("benchLatencyConns: client.New: %s", err)
		}
		clients[i] = cl
		b.Cleanup(func() { _ = cl.Close() })
	}

	// Spread exactly b.N ops across the connections, each writing its observed op
	// latencies into a disjoint slice region (no locking).
	lat := make([]time.Duration, b.N)
	per := b.N / conns
	rem := b.N % conns

	b.ResetTimer()
	var g sync.Group
	offset := 0
	for i := 0; i < conns; i++ {
		ops := per
		if i < rem {
			ops++
		}
		i, start := i, offset
		offset += ops
		g.Go(ctx, func(ctx context.Context) error {
			m := clients[i].NewMutex(fmt.Sprintf("/bench/conn-%d", i))
			for j := 0; j < ops; j++ {
				t0 := time.Now()
				if err := m.Lock(ctx); err != nil {
					return fmt.Errorf("conn %d lock: %w", i, err)
				}
				if err := m.Unlock(ctx); err != nil {
					return fmt.Errorf("conn %d unlock: %w", i, err)
				}
				lat[start+j] = time.Since(t0)
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		b.Fatalf("benchLatencyConns(%d): %s", conns, err)
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds()), "p50-ns")
		b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds()), "p99-ns")
	}
}

// BenchmarkConnectionMemory opens b.N real client connections (each with its own live
// session) and holds them, so heapB/conn is the retained heap of one connection+session
// across the client and server in this process.
func BenchmarkConnectionMemory(b *testing.B) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	addr := c.grpcAddrs[c.nodes[0].replicaID]

	clients := make([]*client.Client, 0, b.N)
	base := heapBaseline()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cl, err := client.New(ctx, client.Endpoints{addr}, client.WithClientID(fmt.Sprintf("bench-%d", i)), client.WithTTL(30*time.Second))
		if err != nil {
			b.Fatalf("BenchmarkConnectionMemory: client.New: %s", err)
		}
		clients = append(clients, cl)
	}
	b.StopTimer()

	retainedPerOp(b, base, b.N, "heapB/conn")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "conns/sec")

	for _, cl := range clients {
		_ = cl.Close()
	}
}

// BenchmarkForwardedWrite measures the leader-forwarding latency tax in a 3-node
// cluster: the same lock+unlock cycle driven on the shard LEADER (local Raft propose)
// versus on a FOLLOWER (the follower's server forwards each write to the leader over the
// node-to-node forward plane, then the leader proposes). Both use in-process server
// calls so the only difference is the forward hop. The "forwarded" ns/op minus the
// "local" ns/op is the tax.
func BenchmarkForwardedWrite(b *testing.B) {
	c := newCluster(b, 3, 4)
	const key = "/bench/forward-key"
	shardID := c.router.Shard(key)
	leaderID := c.leaderReplica(b, shardID)

	b.Run("local", func(b *testing.B) {
		benchWriteVia(b, c.nodeByReplica(leaderID), key, "fwd-local")
	})
	b.Run("forwarded", func(b *testing.B) {
		benchWriteVia(b, c.otherNode(leaderID), key, "fwd-remote")
	})
}

func benchWriteVia(b *testing.B, n *node, key, clientID string) {
	ctx := b.Context()
	openSession(n, clientID)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: clientID})
		if err != nil {
			b.Fatalf("benchWriteVia(replica %d): TryLock: %s", n.replicaID, err)
		}
		if !resp.GetAcquired() {
			b.Fatalf("benchWriteVia(replica %d): not acquired", n.replicaID)
		}
		if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: clientID, FencingToken: resp.GetFencingToken()}); err != nil {
			b.Fatalf("benchWriteVia(replica %d): Unlock: %s", n.replicaID, err)
		}
	}
}

// BenchmarkContendedKey measures the cost of contention on a SINGLE lock: N real client
// connections all loop Lock+Unlock on the same key, so only one holds at a time and the
// rest queue on the server until promoted. handoffs/sec is the lock's serial throughput
// (bounded by one Raft commit per handoff, not by N), and p50wait/p99wait-ns is the
// queue-wait a caller observes — it grows with N as each waiter sits behind more of the
// queue. One node, one shard, so this isolates queueing from forwarding.
func BenchmarkContendedKey(b *testing.B) {
	for _, waiters := range []int{2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			benchContended(b, waiters)
		})
	}
}

func benchContended(b *testing.B, waiters int) {
	c := newCluster(b, 1, 1)
	ctx := b.Context()
	addr := c.grpcAddrs[c.nodes[0].replicaID]
	const key = "/bench/contended"

	clients := make([]*client.Client, waiters)
	for i := range clients {
		cl, err := client.New(ctx, client.Endpoints{addr}, client.WithClientID(fmt.Sprintf("waiter-%d", i)), client.WithTTL(30*time.Second))
		if err != nil {
			b.Fatalf("benchContended: client.New: %s", err)
		}
		clients[i] = cl
		b.Cleanup(func() { _ = cl.Close() })
	}

	// Each waiter records the wait it observed on Lock into a disjoint slice region.
	lat := make([]time.Duration, b.N)
	per := b.N / waiters
	rem := b.N % waiters

	b.ResetTimer()
	var g sync.Group
	offset := 0
	for i := 0; i < waiters; i++ {
		ops := per
		if i < rem {
			ops++
		}
		i, start := i, offset
		offset += ops
		g.Go(ctx, func(ctx context.Context) error {
			m := clients[i].NewMutex(key)
			for j := 0; j < ops; j++ {
				t0 := time.Now()
				ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				if err := m.Lock(ctx); err != nil {
					return fmt.Errorf("waiter %d lock: %w", i, err)
				}
				lat[start+j] = time.Since(t0)
				if err := m.Unlock(ctx); err != nil {
					return fmt.Errorf("waiter %d unlock: %w", i, err)
				}
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		b.Fatalf("benchContended(%d): %s", waiters, err)
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "handoffs/sec")
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds()), "p50wait-ns")
		b.ReportMetric(float64(lat[len(lat)*99/100].Nanoseconds()), "p99wait-ns")
	}
}

// BenchmarkShardedThroughput shows how aggregate write throughput scales with shard
// count. A single node hosts N independent Raft groups; a fixed pool of concurrent
// writers drives distinct keys that the router spreads across all N shards, so as N
// grows the in-flight writes fan out to more independent commit pipelines. Each op is
// one TryLock + one Unlock (two Raft proposals) on a fresh key. Run it across processor
// counts to see the effect: GOMAXPROCS=1 is roughly flat (one core commits serially),
// while more cores let independent shards commit in parallel —
//
//	for p in 1 2 4 8; do GOMAXPROCS=$p go test -run '^$' -bench BenchmarkShardedThroughput \
//	  -benchtime 2s ./internal/integration/ 2>/dev/null | grep -E 'shards=|ops/sec'; done
func BenchmarkShardedThroughput(b *testing.B) {
	for _, shards := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			benchSharded(b, shards, 32)
		})
	}
}

// BenchmarkShardSaturation is the capacity-saturation companion to
// BenchmarkShardedThroughput. Instead of a fixed writer pool, it keeps PER-SHARD
// concurrency constant (perShard writers per shard) and scales the total offered load
// with shard count, so each added shard brings its own proportional work. On fixed
// hardware aggregate ops/sec climbs as shards (and load) grow, then plateaus once the
// box's cores saturate — the plateau is this machine's write ceiling. Sweep cores to
// see the ceiling move:
//
//	for p in 1 2 4 8; do GOMAXPROCS=$p go test -run '^$' -bench BenchmarkShardSaturation \
//	  -benchtime 2s ./internal/integration/ 2>/dev/null | grep -oE 'shards=[0-9]+|[0-9.]+ ops/sec' | paste - -; done
func BenchmarkShardSaturation(b *testing.B) {
	const perShard = 8 // constant per-shard writer concurrency
	for _, shards := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			benchSharded(b, shards, perShard*shards)
		})
	}
}

func benchSharded(b *testing.B, shards, writers int) {
	c := newCluster(b, 1, shards)
	ctx := b.Context()
	n := c.nodes[0]
	for w := 0; w < writers; w++ {
		openSession(n, fmt.Sprintf("bench-%d", w))
	}

	per := b.N / writers
	rem := b.N % writers

	b.ReportAllocs()
	b.ResetTimer()
	var g sync.Group
	for w := 0; w < writers; w++ {
		ops := per
		if w < rem {
			ops++
		}
		w := w
		g.Go(ctx, func(ctx context.Context) error {
			cid := fmt.Sprintf("bench-%d", w)
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("/bench/sh/%d-%d", w, j)
				resp, err := n.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: cid})
				if err != nil {
					return fmt.Errorf("writer %d TryLock: %w", w, err)
				}
				if !resp.GetAcquired() {
					return fmt.Errorf("writer %d key %s not acquired", w, key)
				}
				if _, err := n.srv.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: key, ClientId: cid, FencingToken: resp.GetFencingToken()}); err != nil {
					return fmt.Errorf("writer %d Unlock: %w", w, err)
				}
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		b.Fatalf("benchSharded(shards=%d,writers=%d): %s", shards, writers, err)
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	b.ReportMetric(float64(writers), "writers")
}
