// Package watch is a per-node notification hub. The shard FSM applies every
// committed command on every replica, calling Notify with each ownership change;
// the hub fans those events out to locally-waiting callers — a blocked Lock that
// is waiting to be promoted, or an Election Observe stream. It is the bridge from
// "the state changed on this node" to "wake the client connected to this node".
package watch

import (
	"fmt"
	"hash/maphash"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/raft/fsm"
)

// numStripes is the lock striping factor. Notify runs on the Raft apply path, so a
// single hub-wide mutex would serialize every ownership change against every
// subscribe/close; striping by key keeps unrelated keys from contending.
const numStripes = 16

// Hub fans ownership-change events out to per-key subscribers. It implements
// fsm.Notifier, so it can be handed straight to the FSM.
type Hub struct {
	seed    maphash.Seed
	stripes [numStripes]stripe
}

// stripe is one lock-striped slice of the subscription map.
type stripe struct {
	mu   sync.Mutex
	subs map[string]map[*Sub]struct{}
}

// New returns an empty Hub.
func New() *Hub {
	h := &Hub{seed: maphash.MakeSeed()}
	for i := range h.stripes {
		h.stripes[i].subs = map[string]map[*Sub]struct{}{}
	}
	return h
}

// stripe returns the stripe owning key.
func (h *Hub) stripe(key string) *stripe {
	return &h.stripes[maphash.String(h.seed, key)%numStripes]
}

// Notify delivers e to every current subscriber of e.Key. It is called from the
// Raft apply loop and never blocks: each subscriber keeps only the latest event
// and a one-slot wakeup signal.
func (h *Hub) Notify(e fsm.Event) {
	st := h.stripe(e.Key)
	st.mu.Lock()
	for s := range st.subs[e.Key] {
		s.signal(e)
	}
	st.mu.Unlock()
}

// Sub is a subscription to one key's events. It coalesces: a caller always sees
// the most recent event, never a stale one, even if several arrive between reads.
type Sub struct {
	hub      *Hub
	key      string
	identity string // caller identity (Observe streams); "" if unauthenticated
	track    bool   // true for Observe streams, so Observers reports them
	ch       chan struct{}

	mu   sync.Mutex
	last fsm.Event
}

// SubArgs are the inputs to Subscribe.
type SubArgs struct {
	// Key is the lock/election key to watch. Required.
	Key string
	// Identity is the caller's authenticated identity (from
	// context.IdentityFromContext); "" when unauthenticated. Only meaningful with Track.
	Identity string
	// Track marks the subscription as an observer (an Election Observe stream), so it is
	// reported by Observers. Blocked Lock/Campaign contenders set it false: they are
	// reported from the replicated FSM wait-queue, not the hub.
	Track bool
}

func (a SubArgs) validate() error {
	if a.Key == "" {
		return fmt.Errorf("watch.SubArgs: Key is required")
	}
	return nil
}

// Observation is one tracked observer of a key: its identity and the key it watches.
type Observation struct {
	// Key is the watched lock/election key.
	Key string
	// Identity is the observer's identity; "" if unauthenticated.
	Identity string
}

// Subscribe registers interest in args.Key and returns the subscription. The caller
// must Close it when done. Subscribe before proposing the acquire so no promotion
// event can be missed. It panics on an invalid SubArgs (a programming error).
func (h *Hub) Subscribe(args SubArgs) *Sub {
	if err := args.validate(); err != nil {
		panic(err)
	}
	s := &Sub{hub: h, key: args.Key, identity: args.Identity, track: args.Track, ch: make(chan struct{}, 1)}
	st := h.stripe(args.Key)
	st.mu.Lock()
	m := st.subs[args.Key]
	if m == nil {
		m = map[*Sub]struct{}{}
		st.subs[args.Key] = m
	}
	m[s] = struct{}{}
	st.mu.Unlock()
	return s
}

// Observers returns the tracked observers (Election Observe streams) of key, or of every
// key when key is "". Untracked subscriptions (blocked Lock/Campaign contenders) are
// omitted — they are reported from the FSM wait-queue. Order is unspecified.
func (h *Hub) Observers(key string) []Observation {
	var out []Observation
	collect := func(st *stripe) {
		st.mu.Lock()
		defer st.mu.Unlock()
		for k, subs := range st.subs {
			if key != "" && k != key {
				continue
			}
			for s := range subs {
				if s.track {
					out = append(out, Observation{Key: k, Identity: s.identity})
				}
			}
		}
	}
	if key != "" {
		collect(h.stripe(key))
		return out
	}
	for i := range h.stripes {
		collect(&h.stripes[i])
	}
	return out
}

// signal records e as the latest event and wakes a waiter without blocking.
func (s *Sub) signal(e fsm.Event) {
	s.mu.Lock()
	s.last = e
	s.mu.Unlock()
	select {
	case s.ch <- struct{}{}:
	default:
	}
}

// Next blocks until the next event for the key, returning the latest one, or until
// ctx is done.
func (s *Sub) Next(ctx context.Context) (fsm.Event, error) {
	select {
	case <-s.ch:
		s.mu.Lock()
		e := s.last
		s.mu.Unlock()
		return e, nil
	case <-ctx.Done():
		return fsm.Event{}, ctx.Err()
	}
}

// Close removes the subscription from the hub.
func (s *Sub) Close() {
	st := s.hub.stripe(s.key)
	st.mu.Lock()
	if m := st.subs[s.key]; m != nil {
		delete(m, s)
		if len(m) == 0 {
			delete(st.subs, s.key)
		}
	}
	st.mu.Unlock()
}
