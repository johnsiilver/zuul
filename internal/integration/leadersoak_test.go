package integration

import (
	"bytes"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// This file holds the long-running leader-election correctness soak. It drives a 3-node
// cluster (real wall clock, so leases actually expire) with two workload tiers and a
// quorum-safe chaos engine, and at a quiescent barrier — on a 5-minute timer and at
// random pauses — asserts four invariants and samples for goroutine/heap leaks.
//
// Design notes:
//   - Chaos and the checker run on the TEST goroutine, one at a time. That makes the
//     t.Fatalf-based cluster helpers (addNode/removeNode/restartNode) safe to call (they
//     must not run off the test goroutine) and serializes all c.nodes mutation vs reads
//     without extra locking — workers never touch c.nodes.
//   - Workers run in a pool Group and only ever call t.Errorf/Logf (concurrency-safe);
//     invariant violations they spot go through soaker.errorf.
//   - Tier-1 contenders use a BLOCKING Campaign so non-leaders stay enqueued server-side:
//     a leader's death promotes a waiter automatically, which is what makes the liveness
//     guarantee ("a contested key always has a leader") clean. During a check the current
//     leader holds (does not resign) so leadership is stable while we read it.
//   - Clients are closed via the soaker's own teardown (before the framework's node
//     cleanups), because a node's graceful Close blocks on any open client stream.

var (
	soakDuration         = flag.Duration("zuul.soak", 0, "leader-election soak run time; 0 (default) skips the test. Use e.g. -zuul.soak=2h")
	soakSeed             = flag.Int64("zuul.seed", 0, "RNG seed; 0 derives one from the clock (the chosen seed is logged)")
	soakElectionKeys     = flag.Int("zuul.electionKeys", 4, "strongly-contended election keys (the liveness/single-leader core)")
	soakContenders       = flag.Int("zuul.contenders", 3, "contenders per strongly-contended election key")
	soakChurnKeys        = flag.Int("zuul.churnKeys", 4, "churn election keys (bounded campaigns; may be leaderless)")
	soakLockKeys         = flag.Int("zuul.lockKeys", 6, "lock keys driven by the churn workers")
	soakChurnWorkers     = flag.Int("zuul.churnWorkers", 8, "churn workers driving the mixed op stream")
	soakCheckEvery       = flag.Duration("zuul.checkEvery", 5*time.Minute, "periodic full-correctness check interval")
	soakRandomCheckEvery = flag.Duration("zuul.randomCheckEvery", 2*time.Minute, "mean interval between random-pause single-leader checks")
	soakFaultEvery       = flag.Duration("zuul.faultEvery", 45*time.Second, "mean interval between injected faults")
	soakHeapDump         = flag.String("zuul.heapdump", "", "if set, write a heap profile to this path at every check (overwritten; diagnostics)")
	soakProc             = flag.Bool("zuul.proc", false, "drive real zuuld processes instead of an in-process cluster (production-faithful; restarts are real process restarts, so memory is measured by node RSS)")
	soakUIBind           = flag.String("zuul.uiBind", "", "with -zuul.proc, serve the read-only web UI at this address (e.g. 127.0.0.1:9999); that node is exempt from chaos so the UI stays up")
	soakSnapEntries      = flag.Uint64("zuul.snapEntries", 0, "Raft SnapshotEntries for the in-process backend (0 => node default 10000); lower truncates the in-RAM log sooner (memory mitigation test)")
)

// soakConfig is the validated configuration for one soak run.
type soakConfig struct {
	duration         time.Duration
	seed             int64
	shards           int
	electionKeys     int
	contendersPerKey int
	churnKeys        int
	lockKeys         int
	churnWorkers     int
	checkEvery       time.Duration
	randomCheckEvery time.Duration
	faultEvery       time.Duration
	minHold          time.Duration // min leader hold before resign / lock hold
	maxHold          time.Duration
	settle           time.Duration // quiesce settle window (> expiry sweep interval)
	expiryInterval   time.Duration // leader lease-expiry sweep cadence
	snapEntries      uint64        // Raft SnapshotEntries (in-process backend); 0 => default 10000
	livenessTimeout  time.Duration // how long a contested key may take to (re)elect at a check
	graceTTL         time.Duration // post-kill window during which a lapsing lease may still hold
	warmupSamples    int           // leak samples to discard before trend analysis
}

// validate reports invalid configuration.
func (c soakConfig) validate() error {
	switch {
	case c.duration <= 0:
		return fmt.Errorf("duration must be > 0")
	case c.electionKeys < 1:
		return fmt.Errorf("electionKeys must be >= 1")
	case c.contendersPerKey < 2:
		return fmt.Errorf("contendersPerKey must be >= 2 (need contention to test promotion)")
	case c.lockKeys < 1:
		return fmt.Errorf("lockKeys must be >= 1")
	case c.churnWorkers < 1:
		return fmt.Errorf("churnWorkers must be >= 1")
	case c.checkEvery <= 0 || c.randomCheckEvery <= 0 || c.faultEvery <= 0:
		return fmt.Errorf("check/fault intervals must be > 0")
	}
	return nil
}

// soakCounters are run-wide tallies, reported at each check and at the end.
type soakCounters struct {
	won         atomic.Int64
	resigned    atomic.Int64
	proclaimed  atomic.Int64
	campaignErr atomic.Int64
	resignErr   atomic.Int64
	locks       atomic.Int64
	lockErr     atomic.Int64
	reads       atomic.Int64
	clientLost  atomic.Int64 // sessions that permanently died (broken clients)
	clientNew   atomic.Int64 // client (re)creations
	checks      atomic.Int64
	faults      atomic.Int64
	violations  atomic.Int64
}

// soaker owns one soak run.
type soaker struct {
	t       *testing.T
	cfg     soakConfig
	be      soakBackend
	eps     client.Endpoints // the stable three-port endpoint set (restarts reuse ports)
	mainRng *rand.Rand       // used only on the test goroutine (chaos/checker scheduling)

	reg   *intentReg
	leaks *leakSampler
	count soakCounters

	electionKeys []string
	churnKeys    []string
	lockKeys     []string

	paused atomic.Bool  // set during a check: churn workers park, contenders hold steady
	parked atomic.Int64 // count of churn workers currently parked

	clientsMu sync.Mutex
	clients   map[string]*client.Client

	ghostN int // monotonic counter for unique ghost names/keys
}

// TestLeaderElectionSoak is the long-running correctness soak. It is skipped unless
// -zuul.soak is set (and under -short), so a normal `go test ./...` does not run it. See
// the flags above; a typical overnight run is:
//
//	go test ./internal/integration -run TestLeaderElectionSoak -zuul.soak=2h -timeout 3h -v
func TestLeaderElectionSoak(t *testing.T) {
	if testing.Short() || *soakDuration <= 0 {
		t.Skip("leader-election soak: set -zuul.soak=<dur> (e.g. 2h) to run; skipped by default and under -short")
	}

	seed := *soakSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	cfg := soakConfig{
		duration:         *soakDuration,
		seed:             seed,
		shards:           8,
		electionKeys:     *soakElectionKeys,
		contendersPerKey: *soakContenders,
		churnKeys:        *soakChurnKeys,
		lockKeys:         *soakLockKeys,
		churnWorkers:     *soakChurnWorkers,
		checkEvery:       *soakCheckEvery,
		randomCheckEvery: *soakRandomCheckEvery,
		faultEvery:       *soakFaultEvery,
		minHold:          200 * time.Millisecond,
		maxHold:          2 * time.Second,
		settle:           2 * time.Second,
		expiryInterval:   500 * time.Millisecond,
		snapEntries:      *soakSnapEntries,
		livenessTimeout:  20 * time.Second,
		graceTTL:         40 * time.Second,
		warmupSamples:    2,
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("TestLeaderElectionSoak: invalid config: %s", err)
	}
	var be soakBackend
	switch {
	case *soakProc:
		be = newProcBackend(t, 3, *soakUIBind)
	default:
		be = newInProcBackend(t, newSoakCluster(t, 3, cfg.shards, cfg.expiryInterval, cfg.snapEntries))
	}
	t.Logf("TestLeaderElectionSoak: backend=%s duration=%s seed=%d electionKeys=%d contenders=%d churnKeys=%d lockKeys=%d churnWorkers=%d checkEvery=%s",
		be.name(), cfg.duration, cfg.seed, cfg.electionKeys, cfg.contendersPerKey, cfg.churnKeys, cfg.lockKeys, cfg.churnWorkers, cfg.checkEvery)

	s := newSoaker(t, be, cfg)
	s.run()
}

// newSoaker wires a soaker over a ready backend.
func newSoaker(t *testing.T, be soakBackend, cfg soakConfig) *soaker {
	s := &soaker{
		t:       t,
		cfg:     cfg,
		be:      be,
		eps:     be.endpoints(),
		mainRng: rand.New(rand.NewSource(cfg.seed)),
		reg:     newIntentReg(),
		leaks:   &leakSampler{},
		clients: map[string]*client.Client{},
	}
	for i := 0; i < cfg.electionKeys; i++ {
		s.electionKeys = append(s.electionKeys, fmt.Sprintf("/soak/elect/%d", i))
	}
	for i := 0; i < cfg.churnKeys; i++ {
		s.churnKeys = append(s.churnKeys, fmt.Sprintf("/soak/churn/%d", i))
	}
	for i := 0; i < cfg.lockKeys; i++ {
		s.lockKeys = append(s.lockKeys, fmt.Sprintf("/soak/lock/%d", i))
	}
	return s
}

// run starts the workload, drives chaos + checks for the configured duration, then stops
// the workers and closes every client (before the framework closes the nodes), and does a
// final check + report.
func (s *soaker) run() {
	t := s.t
	runCtx, cancel := context.WithCancel(t.Context())
	defer s.teardown() // closes clients then nodes (clients first: a node's Close blocks on open streams)
	defer cancel()

	g := context.Pool(runCtx).Group()

	// Tier 1: per-key contenders (blocking campaign; the liveness/single-leader core).
	for ki := range s.electionKeys {
		ki := ki
		for ci := 0; ci < s.cfg.contendersPerKey; ci++ {
			ci := ci
			g.Go(runCtx, func(ctx context.Context) error { return s.contender(ctx, ki, ci) })
		}
	}
	// Tier 2: churn workers (mixed op stream over lock + churn keys).
	for wi := 0; wi < s.cfg.churnWorkers; wi++ {
		wi := wi
		g.Go(runCtx, func(ctx context.Context) error { return s.churnWorker(ctx, wi) })
	}

	// Let the contenders warm up so every Tier-1 key has a leader before the first check.
	s.awaitWarmup()

	deadline := time.Now().Add(s.cfg.duration)
	periodic := time.NewTicker(s.cfg.checkEvery)
	defer periodic.Stop()
	randomCh := s.afterRandom(s.cfg.randomCheckEvery)
	faultCh := s.afterRandom(s.cfg.faultEvery)
	end := time.After(s.cfg.duration)

loop:
	for {
		select {
		case <-end:
			break loop
		case <-periodic.C:
			s.runCheck("periodic", false)
		case <-randomCh:
			s.runCheck("random-pause", false)
			randomCh = s.afterRandom(s.cfg.randomCheckEvery)
		case <-faultCh:
			s.runChaos()
			faultCh = s.afterRandom(s.cfg.faultEvery)
		}
		if time.Now().After(deadline) {
			break
		}
	}

	t.Logf("soak: run complete; stopping workers")
	cancel()
	if err := g.Wait(runCtx); err != nil {
		t.Logf("soak: worker group returned: %s", err)
	}
	s.runFinalCheck()
	s.report()
}

// ----- workload: Tier 1 contenders -----

// contender continuously campaigns for electionKeys[ki] as one of that key's candidates.
// It blocks in Campaign (so as a non-leader it stays enqueued, making promotion on a
// leader's death automatic), and on winning holds leadership for a while — proclaiming
// new values — before resigning. While a check is in progress it freezes (holds
// leadership, no proclaim/resign) so the checker reads a stable election. If its session
// dies (a broken client) it rebuilds its client and rejoins.
func (s *soaker) contender(ctx context.Context, ki, ci int) error {
	key := s.electionKeys[ki]
	clientID := fmt.Sprintf("ct-%d-%d", ki, ci)
	rng := rand.New(rand.NewSource(s.cfg.seed + int64(hashStr(key)) + int64(ci) + 1))

	cl := s.dial(ctx, clientID, s.randTTL(rng))
	if cl == nil {
		return nil
	}
	s.reg.enter(key, clientID) // a contender is always a candidate for its key
	el := cl.NewElection(key)

	for ctx.Err() == nil {
		if s.sessionDead(cl) {
			s.count.clientLost.Add(1)
			s.replace(clientID, cl)
			cl = s.dial(ctx, clientID, s.randTTL(rng))
			if cl == nil {
				return nil
			}
			el = cl.NewElection(key)
			continue
		}

		err := el.Campaign(ctx, s.electionValue(rng, clientID)) // block until we lead
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			s.count.campaignErr.Add(1)
			s.sleep(ctx, jitter(rng, 50*time.Millisecond, 300*time.Millisecond))
			continue
		}
		s.count.won.Add(1)

		// Hold leadership for a while, occasionally proclaiming. Freeze on pause.
		holdUntil := time.Now().Add(jitter(rng, s.cfg.minHold, s.cfg.maxHold))
		for ctx.Err() == nil {
			if s.paused.Load() {
				s.sleep(ctx, 25*time.Millisecond)
				continue
			}
			if time.Now().After(holdUntil) {
				break
			}
			if rng.Intn(3) == 0 {
				if err := el.Proclaim(ctx, s.electionValue(rng, clientID)); err == nil {
					s.count.proclaimed.Add(1)
				}
			}
			s.sleep(ctx, jitter(rng, 50*time.Millisecond, 250*time.Millisecond))
		}
		// Do not resign while a check is running: keep leadership steady for the read.
		for s.paused.Load() && ctx.Err() == nil {
			s.sleep(ctx, 25*time.Millisecond)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := el.Resign(ctx); err != nil {
			s.count.resignErr.Add(1)
		} else {
			s.count.resigned.Add(1)
		}
	}
	return nil
}

// ----- workload: Tier 2 churn workers -----

// churnWorker drives a mixed, bounded op stream over the lock and churn-election keys:
// TryLock/Lock/Unlock (with an instant mutual-exclusion guard), bounded
// Campaign/Proclaim/Resign, Leader reads, and short-lived Observe/FollowMaster sessions
// (which exercise — and leak-test — the watch and follower goroutines). It parks at the
// top of its loop while a check is in progress, holding nothing.
func (s *soaker) churnWorker(ctx context.Context, wi int) error {
	clientID := fmt.Sprintf("cw-%d", wi)
	rng := rand.New(rand.NewSource(s.cfg.seed + int64(wi)*7919 + 3))
	cl := s.dial(ctx, clientID, s.randTTL(rng))
	if cl == nil {
		return nil
	}

	for ctx.Err() == nil {
		if s.paused.Load() {
			s.parked.Add(1)
			for s.paused.Load() && ctx.Err() == nil {
				s.sleep(ctx, 25*time.Millisecond)
			}
			s.parked.Add(-1)
			continue
		}
		if s.sessionDead(cl) {
			s.count.clientLost.Add(1)
			s.replace(clientID, cl)
			cl = s.dial(ctx, clientID, s.randTTL(rng))
			if cl == nil {
				return nil
			}
			continue
		}

		switch rng.Intn(10) {
		case 0, 1, 2, 3:
			s.doLock(ctx, cl, rng)
		case 4, 5:
			s.doChurnElection(ctx, cl, clientID, rng)
		case 6, 7:
			s.doRead(ctx, cl, rng)
		case 8:
			s.doObserve(ctx, cl, rng)
		default:
			s.doFollow(ctx, cl, rng)
		}
	}
	return nil
}

// doLock takes a random lock key, guards mutual exclusion, holds briefly, and releases.
func (s *soaker) doLock(ctx context.Context, cl *client.Client, rng *rand.Rand) {
	idx := rng.Intn(s.cfg.lockKeys)
	mu := cl.NewMutex(s.lockKeys[idx])

	var ok bool
	var err error
	if rng.Intn(2) == 0 {
		ok, err = mu.TryLock(ctx)
	} else {
		ctx, cancel := context.WithTimeout(ctx, jitter(rng, 200*time.Millisecond, 2*time.Second))
		defer cancel()
		err = mu.Lock(ctx)
		ok = err == nil
		if err == client.ErrNotAcquired {
			err = nil // a bounded-wait timeout is a normal outcome, not an error
		}
	}
	switch {
	case ctx.Err() != nil:
		return
	case err != nil:
		s.count.lockErr.Add(1)
		return
	case !ok:
		return
	}
	s.count.locks.Add(1)
	// Mutual exclusion is asserted authoritatively at the quiesce barrier (checkLock reads
	// the holder from every node). A continuous in-worker guard was removed: under chaos a
	// worker can die mid-hold without clearing it, leaving stale state that false-flags the
	// next legitimate holder. (Instant mutual-exclusion-under-failover is already proven by
	// the Porcupine linearizability test.)
	s.sleep(ctx, jitter(rng, 50*time.Millisecond, 400*time.Millisecond))
	if err := mu.Unlock(ctx); err != nil {
		s.count.lockErr.Add(1)
	}
}

// doChurnElection runs a bounded campaign on a random churn key, optionally proclaims,
// and resigns. These keys may legitimately be leaderless at a check.
func (s *soaker) doChurnElection(ctx context.Context, cl *client.Client, clientID string, rng *rand.Rand) {
	key := s.churnKeys[rng.Intn(s.cfg.churnKeys)]
	el := cl.NewElection(key)
	cctx, cancel := context.WithTimeout(ctx, jitter(rng, 250*time.Millisecond, 2*time.Second))
	err := el.Campaign(cctx, s.electionValue(rng, clientID))
	cancel()
	switch {
	case ctx.Err() != nil:
		return
	case err == client.ErrNotAcquired:
		return
	case err != nil:
		s.count.campaignErr.Add(1)
		return
	}
	s.count.won.Add(1)
	if rng.Intn(2) == 0 {
		if err := el.Proclaim(ctx, s.electionValue(rng, clientID)); err == nil {
			s.count.proclaimed.Add(1)
		}
	}
	s.sleep(ctx, jitter(rng, 50*time.Millisecond, 500*time.Millisecond))
	if err := el.Resign(ctx); err != nil {
		s.count.resignErr.Add(1)
	} else {
		s.count.resigned.Add(1)
	}
}

// doRead does a one-shot leader read on a random election/churn key (exercising the read
// path under load). Fencing monotonicity is asserted only at the quiesce barrier, not from
// these concurrent reads: tokens are a shard-global sequence, so a slow goroutine reporting
// an older-but-valid token after a faster one reported a newer one would false-flag a
// regression that never happened in the FSM.
func (s *soaker) doRead(ctx context.Context, cl *client.Client, rng *rand.Rand) {
	keys := append(append([]string{}, s.electionKeys...), s.churnKeys...)
	key := keys[rng.Intn(len(keys))]
	if _, err := cl.NewElection(key).Leader(ctx); err != nil {
		return
	}
	s.count.reads.Add(1)
}

// doObserve opens an Observe stream on a random election key for a short window, draining
// events, then cancels it — exercising the watch hub and verifying the observe goroutines
// are released (leak-tested at the next quiesce).
func (s *soaker) doObserve(ctx context.Context, cl *client.Client, rng *rand.Rand) {
	key := s.electionKeys[rng.Intn(s.cfg.electionKeys)]
	octx, cancel := context.WithTimeout(ctx, jitter(rng, 200*time.Millisecond, 1500*time.Millisecond))
	defer cancel()
	ch, err := cl.NewElection(key).Observe(octx)
	if err != nil {
		return
	}
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
			s.count.reads.Add(1)
		case <-octx.Done():
			return
		}
	}
}

// doFollow opens a FollowMaster on a random election key briefly, then closes it.
func (s *soaker) doFollow(ctx context.Context, cl *client.Client, rng *rand.Rand) {
	key := s.electionKeys[rng.Intn(s.cfg.electionKeys)]
	octx, cancel := context.WithTimeout(ctx, jitter(rng, 200*time.Millisecond, 1500*time.Millisecond))
	defer cancel()
	f, err := cl.NewElection(key).FollowMaster(octx)
	if err != nil {
		return
	}
	defer f.Close()
	for {
		select {
		case _, open := <-f.Updates():
			if !open {
				return
			}
			s.count.reads.Add(1)
		case <-octx.Done():
			return
		}
	}
}

// ----- chaos -----

// runChaos injects one fault and fully recovers before returning. It runs on the test
// goroutine, so it safely uses the t.Fatalf-based cluster helpers and is serialized
// against the checker. Faults keep the cluster quorate (never more than one node down).
func (s *soaker) runChaos() {
	t := s.t
	if s.paused.Load() {
		return // a check is imminent/underway; skip this fault tick
	}
	s.count.faults.Add(1)
	switch s.mainRng.Intn(10) {
	case 0, 1, 2, 3, 4:
		s.faultRestart(true) // hard crash + restart (most common)
	case 5, 6:
		s.faultRestart(false) // graceful drain + restart
	case 7:
		s.faultMembership() // grow then shrink
	default:
		s.faultGhost() // strand a single-endpoint client by killing its only node
	}
	t.Logf("soak: fault #%d done; cluster has %d nodes", s.count.faults.Load(), len(s.be.liveNodes()))
}

// faultRestart kills a random node and restarts it on the same gRPC port.
func (s *soaker) faultRestart(hard bool) {
	s.be.restart(s.mainRng, hard)
}

// faultMembership grows the cluster by one node, then shrinks back to three.
func (s *soaker) faultMembership() {
	s.be.growShrink(s.mainRng)
}

// faultGhost strands a broken client: it dials a single endpoint with a short TTL, grabs
// leadership on a ghost key, then kills that endpoint's node and keeps it down past the
// ghost's TTL (the cluster stays quorate at two of three). With no other endpoint the
// ghost cannot keep its lease alive, so its session lapses — its Done() must fire and the
// expiry sweep must reap its orphaned leadership — which this verifies in place before
// restoring the node. (Keeping the node down past the TTL is what makes this reliable: a
// fast rebind on the reused port would otherwise let the ghost reconnect before lapsing.)
func (s *soaker) faultGhost() {
	t := s.t
	ctx := t.Context()
	const ghostTTL = 2 * time.Second
	ep, survivor, kill, restore := s.be.ghostVictim(s.mainRng)

	s.ghostN++
	id := fmt.Sprintf("ghost-%d", s.ghostN)
	key := fmt.Sprintf("/soak/ghost/%d", s.ghostN)
	cl, err := client.New(ctx, client.Endpoints{ep}, client.WithClientID(id), client.WithTTL(ghostTTL))
	if err != nil {
		t.Logf("soak: ghost %s dial failed (skipping fault): %s", id, err)
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = cl.NewElection(key).Campaign(cctx, []byte(id))
	cancel()
	if err != nil {
		t.Logf("soak: ghost %s campaign failed (skipping fault): %s", id, err)
		_ = cl.Close()
		return
	}
	s.reg.killClient(id, time.Now().Add(s.cfg.graceTTL).UnixNano())

	// Kill the ghost's only node and keep it down past the TTL so the ghost truly lapses.
	kill()

	select {
	case <-cl.Done():
		// good: the stranded client noticed its permanent session loss
	case <-time.After(ghostTTL + 8*time.Second):
		s.errorf("BROKEN CLIENT: ghost %s never reported a lost session after its only node died", id)
	}
	// The surviving leader's expiry sweep must reap the orphaned leadership.
	if !s.awaitGhostReaped(ctx, survivor, key, 10*time.Second) {
		s.errorf("LEAKED LOCK %s: ghost %s leadership not reaped after its lease lapsed", key, id)
	}
	_ = cl.Close()

	restore() // bring the node back (re-add on the same gRPC port)
}

// awaitGhostReaped polls survivor until key is leaderless (the expiry sweep reaped it).
func (s *soaker) awaitGhostReaped(ctx context.Context, survivor soakNode, key string, timeout time.Duration) bool {
	if survivor == nil {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := survivor.leader(ctx, key); err == nil && !resp.GetHasLeader() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// ----- checks -----

// runCheck pauses the churn workers, heals/settles the cluster, then asserts the
// invariants across all nodes and samples for leaks. It runs on the test goroutine. When
// final is set the workers have already stopped, so it does not wait for them to park and
// does not require contested keys to have a leader (there are no contenders left).
//
// Election-key (Tier-1) checks are always safe: contenders freeze on pause, so those keys
// hold still while we read them. Churn- and lock-key checks need the churn workers parked
// (else state could change between the per-node reads and read as a false split view), so
// they run only when fully quiesced.
func (s *soaker) runCheck(label string, final bool) {
	t := s.t
	ctx := t.Context()

	quiesced := final
	if !final {
		s.paused.Store(true)
		defer s.paused.Store(false)
		quiesced = s.awaitParked(s.cfg.churnWorkers, 30*time.Second)
		if !quiesced {
			t.Logf("soak[%s]: not all churn workers quiesced (parked=%d/%d); checking election keys only", label, s.parked.Load(), s.cfg.churnWorkers)
		}
	}
	s.be.awaitReady(15 * time.Second)
	time.Sleep(s.cfg.settle) // let pending expiry/promotion apply

	// Gate the correctness assertions on the cluster being fully readable: every soak key
	// must answer a linearizable read, which proves its shard has a leader and quorum.
	// Without this a check can land mid-re-election (e.g. just after a fault) and false-flag
	// a contested key as leaderless. If the cluster is not readable in time, skip the
	// assertions this round (not a violation) but still sample for leaks.
	if s.awaitKeysReadable(ctx, 20*time.Second) {
		for _, key := range s.electionKeys {
			s.checkElection(ctx, key, !final)
		}
		if quiesced {
			for _, key := range s.churnKeys {
				s.checkElection(ctx, key, false)
			}
			for _, key := range s.lockKeys {
				s.checkLock(ctx, key)
			}
		}
	} else {
		t.Logf("soak[%s]: cluster not fully readable after heal; skipping correctness assertions this round (not a violation)", label)
	}

	smp := s.leaks.sample(s.liveClientCount(), s.nodeRSSkiB())
	if *soakHeapDump != "" {
		s.dumpHeapTo(*soakHeapDump)
	}
	t.Logf("soak[%s]: goroutines=%d driverHeap=%dMiB nodeRSS=%dMiB liveClients=%d | won=%d resigned=%d proclaimed=%d locks=%d reads=%d brokenClients=%d faults=%d violations=%d",
		label, smp.goroutines, smp.heapAlloc>>20, smp.nodeRSSkiB>>10, smp.liveClients,
		s.count.won.Load(), s.count.resigned.Load(), s.count.proclaimed.Load(), s.count.locks.Load(),
		s.count.reads.Load(), s.count.clientLost.Load(), s.count.faults.Load(), s.count.violations.Load())
	s.count.checks.Add(1)
	s.checkLeakTrend()
}

// nodeRSSkiB sums the resident set size (KiB) of the live nodes; 0 for the in-process
// backend (where the nodes share this process and the driver heap is the memory signal).
func (s *soaker) nodeRSSkiB() int {
	total := 0
	for _, n := range s.be.liveNodes() {
		total += n.rssKiB()
	}
	return total
}

// awaitKeysReadable waits until every soak key answers a linearizable Leader read from a
// live node — success proves the key's shard has a leader and quorum. It gates the
// correctness assertions so a check never lands mid-re-election and false-flags liveness.
func (s *soaker) awaitKeysReadable(ctx context.Context, timeout time.Duration) bool {
	nodes := s.be.liveNodes()
	if len(nodes) == 0 {
		return false
	}
	n := nodes[0]
	keys := make([]string, 0, len(s.electionKeys)+len(s.churnKeys)+len(s.lockKeys))
	keys = append(keys, s.electionKeys...)
	keys = append(keys, s.churnKeys...)
	keys = append(keys, s.lockKeys...)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, key := range keys {
			if _, err := n.leader(ctx, key); err != nil {
				allOK = false
				break
			}
		}
		if allOK {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// checkElection verifies, for one election key, that all nodes agree on a single leader,
// fencing is monotonic, and (when requireLeader) a contested key actually has a leader
// whose id is one of its live candidates.
func (s *soaker) checkElection(ctx context.Context, key string, requireLeader bool) {
	if requireLeader && !s.awaitElectionLeader(ctx, key, s.cfg.livenessTimeout) {
		s.errorf("LIVENESS: election %s has live contenders but no leader after %s", key, s.cfg.livenessTimeout)
		return
	}

	views := s.readLeaderAllNodes(ctx, key)
	if len(views) == 0 {
		s.t.Logf("soak: election %s: no node answered the leader read", key)
		return
	}
	base := views[0]
	for i := 1; i < len(views); i++ {
		if views[i] != base {
			s.errorf("SPLIT VIEW on election %s: node0=%+v node%d=%+v", key, base, i, views[i])
			return
		}
	}
	if base.has {
		if regressed, prev := s.reg.observeToken(key, base.token); regressed {
			s.errorf("FENCING regression on election %s: token %d < prior max %d", key, base.token, prev)
		}
		if requireLeader && !s.legitLeader(key, base.id) {
			s.errorf("LIVENESS: election %s led by %q which is not a current candidate", key, base.id)
		}
	}
}

// legitLeader reports whether id may legitimately lead key: a current candidate, or a
// client still within its post-kill grace window.
func (s *soaker) legitLeader(key, id string) bool {
	for _, c := range s.reg.candidates(key) {
		if c == id {
			return true
		}
	}
	return s.reg.inGrace(id, time.Now().UnixNano())
}

// checkLock verifies all nodes agree on a lock key's holder and that it is not orphaned.
// At a quiesce barrier every churn worker is parked holding nothing, so a lock key should
// be unheld; a holder that is not a live or recently-killed client is a leak.
func (s *soaker) checkLock(ctx context.Context, key string) {
	views := s.readStatusAllNodes(ctx, key)
	if len(views) == 0 {
		return
	}
	base := views[0]
	for i := 1; i < len(views); i++ {
		if views[i] != base {
			s.errorf("SPLIT VIEW on lock %s: node0=%+v node%d=%+v", key, base, i, views[i])
			return
		}
	}
	if base.held && !s.isLiveClient(base.id) && !s.reg.inGrace(base.id, time.Now().UnixNano()) {
		s.errorf("LEAKED LOCK %s: held by %q which is neither a live client nor recently killed", key, base.id)
	}
}

// checkLeakTrend flags a sustained upward memory trend (after warmup). It always checks
// goroutines. For the memory signal it uses node RSS on the process backend (where the
// real per-node memory lives) and the driver heap on the in-process backend (where the
// nodes share this process). Conservative thresholds + the full logged series (the primary
// diagnostic) keep it from false-alarming on pool/GC noise.
func (s *soaker) checkLeakTrend() {
	gor := s.leaks.goroutineSeries()
	if len(gor) <= s.cfg.warmupSamples+5 {
		return
	}
	gor = gor[s.cfg.warmupSamples:]
	if risingTrend(gor, 0.5, 200) {
		s.errorf("LEAK SUSPECTED (goroutines trending up): %v", trimSeries(gor))
		s.dumpGoroutines()
	}

	rss := s.leaks.rssSeries()
	switch {
	case rss[len(rss)-1] > 0: // process backend: node RSS is the memory signal
		rss = rss[s.cfg.warmupSamples:]
		if risingTrend(rss, 0.75, 512*1024) { // +75% and +512 MiB (KiB units)
			s.errorf("LEAK SUSPECTED (node RSS trending up, MiB): %v", kibToMiB(trimSeries(rss)))
		}
	default: // in-process backend: driver heap is the memory signal
		hep := s.leaks.heapSeries()[s.cfg.warmupSamples:]
		if risingTrend(hep, 1.0, 256<<20) {
			s.errorf("LEAK SUSPECTED (driver heap trending up, MiB): %v", mibSeries(trimSeries(hep)))
			s.dumpGoroutines()
			s.dumpHeapTo("")
		}
	}
}

// runFinalCheck does one last correctness check after the workers have stopped.
func (s *soaker) runFinalCheck() {
	s.t.Logf("soak: final check")
	s.runCheck("final", true)
}

// report logs the end-of-run tallies and the full per-check goroutine/heap series, so a
// subtle leak shows as a trend in the output even if the conservative auto-detector did
// not fire.
func (s *soaker) report() {
	s.t.Logf("soak: DONE checks=%d faults=%d won=%d resigned=%d proclaimed=%d locks=%d reads=%d brokenClients=%d clientNew=%d violations=%d",
		s.count.checks.Load(), s.count.faults.Load(), s.count.won.Load(), s.count.resigned.Load(),
		s.count.proclaimed.Load(), s.count.locks.Load(), s.count.reads.Load(), s.count.clientLost.Load(), s.count.clientNew.Load(), s.count.violations.Load())
	s.t.Logf("soak: goroutines per check: %v", intSeries(s.leaks.goroutineSeries()))
	s.t.Logf("soak: driverHeap MiB per check: %v", mibSeries(s.leaks.heapSeries()))
	if rss := s.leaks.rssSeries(); len(rss) > 0 && rss[len(rss)-1] > 0 {
		s.t.Logf("soak: nodeRSS MiB per check: %v", kibToMiB(rss))
	}
	if v := s.count.violations.Load(); v > 0 {
		s.t.Errorf("soak: %d invariant violation(s) over the run", v)
	}
}

// ----- per-node reads (linearizable, retried through the post-failover window) -----

// leaderView is the comparable subset of a leader read used for cross-node agreement.
type leaderView struct {
	has   bool
	id    string
	token uint64
	value string
}

// statusView is the comparable subset of a lock status read.
type statusView struct {
	held  bool
	id    string
	token uint64
}

// readLeaderAllNodes reads the leader of key from every node, retrying transient errors.
func (s *soaker) readLeaderAllNodes(ctx context.Context, key string) []leaderView {
	nodes := s.be.liveNodes()
	out := make([]leaderView, 0, len(nodes))
	for _, n := range nodes {
		resp := s.readLeader(ctx, n, key)
		if resp == nil {
			continue
		}
		out = append(out, leaderView{has: resp.GetHasLeader(), id: resp.GetLeaderClientId(), token: resp.GetFencingToken(), value: string(resp.GetValue())})
	}
	return out
}

// readStatusAllNodes reads the lock status of key from every node, retrying transient errors.
func (s *soaker) readStatusAllNodes(ctx context.Context, key string) []statusView {
	nodes := s.be.liveNodes()
	out := make([]statusView, 0, len(nodes))
	for _, n := range nodes {
		resp := s.readStatus(ctx, n, key)
		if resp == nil {
			continue
		}
		out = append(out, statusView{held: resp.GetHeld(), id: resp.GetHolderClientId(), token: resp.GetFencingToken()})
	}
	return out
}

func (s *soaker) readLeader(ctx context.Context, n soakNode, key string) *zuulv1.LeaderResponse {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := n.leader(ctx, key); err == nil {
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (s *soaker) readStatus(ctx context.Context, n soakNode, key string) *zuulv1.StatusResponse {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := n.status(ctx, key); err == nil {
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// awaitElectionLeader polls (any node) until key has a leader, or timeout.
func (s *soaker) awaitElectionLeader(ctx context.Context, key string, timeout time.Duration) bool {
	nodes := s.be.liveNodes()
	if len(nodes) == 0 {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := nodes[0].leader(ctx, key); err == nil && resp.GetHasLeader() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// ----- client lifecycle -----

// dial creates a tracked client (all three endpoints) with the given TTL, retrying
// transient dial failures until it succeeds or ctx ends (returns nil on ctx end).
func (s *soaker) dial(ctx context.Context, clientID string, ttl time.Duration) *client.Client {
	for ctx.Err() == nil {
		cl, err := client.New(ctx, s.eps, client.WithClientID(clientID), client.WithTTL(ttl))
		if err == nil {
			s.clientsMu.Lock()
			s.clients[clientID] = cl
			s.clientsMu.Unlock()
			s.count.clientNew.Add(1)
			return cl
		}
		s.sleep(ctx, 200*time.Millisecond)
	}
	return nil
}

// replace untracks and closes a dead client before its id is redialed.
func (s *soaker) replace(clientID string, cl *client.Client) {
	s.clientsMu.Lock()
	if s.clients[clientID] == cl {
		delete(s.clients, clientID)
	}
	s.clientsMu.Unlock()
	_ = cl.Close()
}

// sessionDead reports whether cl's session is permanently lost.
func (s *soaker) sessionDead(cl *client.Client) bool {
	select {
	case <-cl.Done():
		return true
	default:
		return false
	}
}

// isLiveClient reports whether clientID is a currently-tracked live client.
func (s *soaker) isLiveClient(clientID string) bool {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	_, ok := s.clients[clientID]
	return ok
}

// liveClientCount returns the number of tracked live clients.
func (s *soaker) liveClientCount() int {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	return len(s.clients)
}

// teardown closes every tracked client and then the cluster's current nodes. Clients must
// close first — a node's graceful Close blocks on any open client stream. It runs as the
// soaker's own deferred teardown, before the framework's per-node t.Cleanups (which cover
// only the initial nodes; restartNode-created nodes are owned by the cluster and closed
// here). node.Close is idempotent, so the initial nodes being closed twice is harmless.
func (s *soaker) teardown() {
	s.clientsMu.Lock()
	cls := make([]*client.Client, 0, len(s.clients))
	for id, cl := range s.clients {
		cls = append(cls, cl)
		delete(s.clients, id)
	}
	s.clientsMu.Unlock()
	for _, cl := range cls {
		_ = cl.Close()
	}
	s.be.teardown()
}

// ----- small helpers -----

// errorf records an invariant violation: it bumps the counter and reports via t.Errorf
// (safe from any goroutine; only Fatal/FailNow must stay on the test goroutine).
func (s *soaker) errorf(format string, args ...any) {
	s.count.violations.Add(1)
	s.t.Errorf("soak: "+format, args...)
}

// dumpGoroutines writes a goroutine profile to a temp file for offline diagnosis.
func (s *soaker) dumpGoroutines() {
	f, err := os.CreateTemp("", "zuul-soak-goroutine-*.txt")
	if err != nil {
		return
	}
	defer f.Close()
	_ = pprof.Lookup("goroutine").WriteTo(f, 1)
	s.t.Logf("soak: wrote goroutine profile to %s", f.Name())
}

// dumpHeapTo writes a heap profile (for `go tool pprof`) to path, or to a temp file when
// path is empty. It GCs first so the profile reflects live (leaked) memory.
func (s *soaker) dumpHeapTo(path string) {
	runtime.GC()
	var f *os.File
	var err error
	switch path {
	case "":
		f, err = os.CreateTemp("", "zuul-soak-heap-*.pprof")
	default:
		f, err = os.Create(path)
	}
	if err != nil {
		return
	}
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
	s.t.Logf("soak: wrote heap profile to %s", f.Name())
}

// awaitWarmup waits until every Tier-1 election key has a leader, so the first check is
// not racing initial elections.
func (s *soaker) awaitWarmup() {
	ctx := s.t.Context()
	for _, key := range s.electionKeys {
		if !s.awaitElectionLeader(ctx, key, 30*time.Second) {
			s.t.Logf("soak: warmup: %s had no leader after 30s", key)
		}
	}
	s.leaks.sample(s.liveClientCount(), s.nodeRSSkiB()) // baseline sample
}

// awaitParked waits until at least want churn workers are parked, or timeout.
func (s *soaker) awaitParked(want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if int(s.parked.Load()) >= want {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return int(s.parked.Load()) >= want
}

// sleep is a ctx-aware sleep.
func (s *soaker) sleep(ctx context.Context, d time.Duration) {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
	case <-tm.C:
	}
}

// randTTL picks a per-client lease TTL spanning the heartbeat-cadence space.
func (s *soaker) randTTL(rng *rand.Rand) time.Duration {
	return []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}[rng.Intn(4)]
}

// electionValue builds a published value: usually a small label, sometimes a marshaled
// Endpoint (exercising the master-dial path), occasionally a larger blob.
func (s *soaker) electionValue(rng *rand.Rand, clientID string) []byte {
	switch rng.Intn(10) {
	case 0, 1:
		ep := &zuulv1.Endpoint{Host: fmt.Sprintf("10.0.%d.%d", rng.Intn(256), rng.Intn(254)+1), Port: uint32(rng.Intn(60000) + 1024)}
		if b, err := client.MarshalEndpoint(ep); err == nil {
			return b
		}
		return []byte(clientID)
	case 2:
		return bytes.Repeat([]byte("z"), 4096)
	default:
		return []byte(clientID + "-v" + strconv.Itoa(rng.Intn(1000)))
	}
}

// afterRandom returns a channel firing once after a random duration in [mean/2, 3*mean/2].
func (s *soaker) afterRandom(mean time.Duration) <-chan time.Time {
	lo := mean / 2
	return time.After(lo + time.Duration(s.mainRng.Int63n(int64(mean))))
}

// jitter returns a random duration in [min, max].
func jitter(rng *rand.Rand, min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rng.Int63n(int64(max-min)))
}

// hashStr is a small string hash used to diversify per-goroutine RNG seeds.
func hashStr(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// trimSeries renders the last few values of a float series for a log line.
func trimSeries(xs []float64) []float64 {
	if len(xs) <= 8 {
		return xs
	}
	return xs[len(xs)-8:]
}

// intSeries renders a float series as ints for a log line.
func intSeries(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x)
	}
	return out
}

// mibSeries renders a byte-count series as whole MiB for a log line.
func mibSeries(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x) >> 20
	}
	return out
}

// kibToMiB renders a KiB series as whole MiB for a log line.
func kibToMiB(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x) >> 10
	}
	return out
}
