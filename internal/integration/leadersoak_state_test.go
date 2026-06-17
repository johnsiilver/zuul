package integration

import (
	"runtime"
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
)

// intentReg is the model the soak checker consults to decide, at a quiescent barrier,
// whether an election key SHOULD have a leader and which client ids may legitimately be
// it. It tracks, per key, the set of clients currently intending to lead, the largest
// fencing token ever observed (to catch regressions), and clients recently killed (whose
// lease may briefly still hold leadership before the expiry sweep reaps it). Safe for
// concurrent use.
type intentReg struct {
	mu       sync.Mutex
	intend   map[string]map[string]bool // key -> set of candidate client ids
	maxToken map[string]uint64          // key -> largest fencing token observed
	killed   map[string]int64           // client id -> unix-nano grace deadline
}

// newIntentReg returns an empty registry.
func newIntentReg() *intentReg {
	return &intentReg{
		intend:   map[string]map[string]bool{},
		maxToken: map[string]uint64{},
		killed:   map[string]int64{},
	}
}

// enter records that clientID intends to lead key (called just before a Campaign).
func (r *intentReg) enter(key, clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.intend[key]
	if s == nil {
		s = map[string]bool{}
		r.intend[key] = s
	}
	s[clientID] = true
}

// leave records that clientID no longer intends to lead key (after a clean Resign/exit).
func (r *intentReg) leave(key, clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.intend[key]; s != nil {
		delete(s, clientID)
	}
}

// candidates returns a snapshot of the client ids currently intending to lead key.
func (r *intentReg) candidates(key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.intend[key]
	out := make([]string, 0, len(s))
	for id := range s {
		out = append(out, id)
	}
	return out
}

// killClient marks clientID dead through graceUntil (unix-nano): until then its lease may
// still hold leadership before the expiry sweep reaps it, so the checker tolerates it as
// a leader.
func (r *intentReg) killClient(clientID string, graceUntil int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.killed[clientID] = graceUntil
}

// inGrace reports whether clientID is within its post-kill grace window at now.
func (r *intentReg) inGrace(clientID string, now int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.killed[clientID]
	return ok && now < g
}

// observeToken records a (non-zero) fencing token seen for key and reports whether it
// regressed below a previously observed token — a fencing-monotonicity violation.
func (r *intentReg) observeToken(key string, token uint64) (regressed bool, prev uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev = r.maxToken[key]
	switch {
	case token < prev:
		return true, prev
	case token > prev:
		r.maxToken[key] = token
	}
	return false, prev
}

// leakSample is one quiescent-point resource reading.
type leakSample struct {
	at          time.Time
	goroutines  int
	heapAlloc   uint64
	heapInuse   uint64
	heapObjs    uint64
	liveClients int
	nodeRSSkiB  int // sum of live node RSS; 0 for the in-process backend
}

// leakSampler accumulates quiescent samples and detects a sustained upward trend in
// goroutines or heap — the signature of a long-term leak. Absolute counts are noisy (the
// shared worker pool grows and shrinks), so detection compares same-condition samples
// over time rather than against a fixed cap.
type leakSampler struct {
	mu      sync.Mutex
	samples []leakSample
}

// sample reads current goroutine and heap usage (after a GC so the heap reflects live
// data, not allocator float) and appends it. liveClients is the caller's count of live
// client objects; nodeRSSkiB is the summed RSS of out-of-process nodes (0 in-process).
func (s *leakSampler) sample(liveClients, nodeRSSkiB int) leakSample {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	smp := leakSample{
		at:          time.Now(),
		goroutines:  runtime.NumGoroutine(),
		heapAlloc:   ms.HeapAlloc,
		heapInuse:   ms.HeapInuse,
		heapObjs:    ms.HeapObjects,
		liveClients: liveClients,
		nodeRSSkiB:  nodeRSSkiB,
	}
	s.mu.Lock()
	s.samples = append(s.samples, smp)
	s.mu.Unlock()
	return smp
}

// rssSeries returns the per-sample node-RSS (KiB) as a float series for trend analysis.
func (s *leakSampler) rssSeries() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]float64, len(s.samples))
	for i, smp := range s.samples {
		out[i] = float64(smp.nodeRSSkiB)
	}
	return out
}

// goroutineSeries / heapSeries return the recorded metric as a float series for trend
// analysis.
func (s *leakSampler) goroutineSeries() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]float64, len(s.samples))
	for i, smp := range s.samples {
		out[i] = float64(smp.goroutines)
	}
	return out
}

func (s *leakSampler) heapSeries() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]float64, len(s.samples))
	for i, smp := range s.samples {
		out[i] = float64(smp.heapAlloc)
	}
	return out
}

// risingTrend reports whether the tail of xs sits meaningfully above its head: the mean
// of the last third exceeds the mean of the first third by more than BOTH a relative
// (relFrac) and an absolute (absFloor) margin. It needs at least 6 samples; with fewer it
// returns false (not enough signal). Requiring both margins makes it tolerant of
// per-sample noise around a plateau while still catching a steady climb.
func risingTrend(xs []float64, relFrac, absFloor float64) bool {
	n := len(xs)
	if n < 6 {
		return false
	}
	k := n / 3
	head := mean(xs[:k])
	tail := mean(xs[n-k:])
	return tail-head > absFloor && tail > head*(1+relFrac)
}

// mean returns the arithmetic mean of xs (0 for an empty slice).
func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// TestIntentReg covers the registry's candidate set, kill grace window, and fencing-token
// regression detection.
func TestIntentReg(t *testing.T) {
	r := newIntentReg()

	r.enter("/k", "a")
	r.enter("/k", "b")
	if got := len(r.candidates("/k")); got != 2 {
		t.Errorf("TestIntentReg: candidates after two enters = %d, want 2", got)
	}
	r.leave("/k", "a")
	if got := len(r.candidates("/k")); got != 1 {
		t.Errorf("TestIntentReg: candidates after one leave = %d, want 1", got)
	}

	r.killClient("b", 100)
	switch {
	case !r.inGrace("b", 50):
		t.Errorf("TestIntentReg: inGrace(b,50) = false, want true (before grace deadline)")
	case r.inGrace("b", 150):
		t.Errorf("TestIntentReg: inGrace(b,150) = true, want false (after grace deadline)")
	case r.inGrace("c", 50):
		t.Errorf("TestIntentReg: inGrace(c,50) = true, want false (never killed)")
	}

	tests := []struct {
		name        string
		token       uint64
		wantRegress bool
	}{
		{name: "Success: first token sets the max", token: 5, wantRegress: false},
		{name: "Success: re-reading the same token is not a regression", token: 5, wantRegress: false},
		{name: "Success: a larger token advances the max", token: 9, wantRegress: false},
		{name: "Error: a smaller token is a fencing regression", token: 7, wantRegress: true},
	}
	for _, test := range tests {
		regressed, _ := r.observeToken("/tok", test.token)
		if regressed != test.wantRegress {
			t.Errorf("TestIntentReg(%s): observeToken(%d) regressed = %v, want %v", test.name, test.token, regressed, test.wantRegress)
		}
	}
}

// TestLeakTrend proves the trend detector flags a steady climb but not a noisy plateau or
// a short series.
func TestLeakTrend(t *testing.T) {
	tests := []struct {
		name string
		xs   []float64
		want bool
	}{
		{name: "Success: noisy plateau does not trip", xs: []float64{100, 110, 95, 105, 98, 102, 100, 108, 96}, want: false},
		{name: "Success: too few samples do not trip", xs: []float64{100, 300, 500}, want: false},
		{name: "Error: a steady climb trips", xs: []float64{100, 150, 200, 260, 320, 380, 450, 520, 600}, want: true},
	}
	for _, test := range tests {
		// relFrac 0.5 (+50%) and absFloor 50 — the soak's goroutine thresholds.
		if got := risingTrend(test.xs, 0.5, 50); got != test.want {
			t.Errorf("TestLeakTrend(%s): risingTrend = %v, want %v", test.name, got, test.want)
		}
	}
}
