package watch

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/raft/fsm"
)

// TestHubDeliversLatest proves a subscriber receives the most recent event for its
// key, coalescing intermediate ones.
func TestHubDeliversLatest(t *testing.T) {
	h := New()
	sub := h.Subscribe(SubArgs{Key: "k"})
	defer sub.Close()

	h.Notify(fsm.Event{Key: "k", Holder: "a", Token: 1, Revision: 1})
	h.Notify(fsm.Event{Key: "k", Holder: "b", Token: 2, Revision: 2})

	got, err := sub.Next(t.Context())
	if err != nil {
		t.Fatalf("TestHubDeliversLatest: Next: got err == %s, want err == nil", err)
	}
	want := fsm.Event{Key: "k", Holder: "b", Token: 2, Revision: 2}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestHubDeliversLatest: -want +got:\n%s", diff)
	}
}

// TestHubWakesBlockedWaiter proves Next blocks until an event arrives, then returns
// it — the path a blocked Lock relies on.
func TestHubWakesBlockedWaiter(t *testing.T) {
	h := New()
	sub := h.Subscribe(SubArgs{Key: "k"})
	defer sub.Close()

	ctx := t.Context()
	context.Pool(ctx).Submit(ctx, func() {
		time.Sleep(20 * time.Millisecond)
		h.Notify(fsm.Event{Key: "k", Holder: "a", Token: 7, Revision: 9})
	})

	got, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("TestHubWakesBlockedWaiter: Next: got err == %s, want err == nil", err)
	}
	want := fsm.Event{Key: "k", Holder: "a", Token: 7, Revision: 9}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestHubWakesBlockedWaiter: -want +got:\n%s", diff)
	}
}

// TestHubKeyIsolation proves an event on one key never wakes a subscriber to a
// different key.
func TestHubKeyIsolation(t *testing.T) {
	h := New()
	sub := h.Subscribe(SubArgs{Key: "k"})
	defer sub.Close()

	h.Notify(fsm.Event{Key: "other", Holder: "x", Token: 1})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); err == nil {
		t.Errorf("TestHubKeyIsolation: Next: got err == nil, want a timeout error")
	}
}

// TestHubContextCancel proves Next returns when its context is cancelled.
func TestHubContextCancel(t *testing.T) {
	h := New()
	sub := h.Subscribe(SubArgs{Key: "k"})
	defer sub.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := sub.Next(ctx); err == nil {
		t.Errorf("TestHubContextCancel: Next: got err == nil, want a cancellation error")
	}
}

// TestHubCloseUnsubscribes proves a closed subscription no longer receives events.
func TestHubCloseUnsubscribes(t *testing.T) {
	h := New()
	sub := h.Subscribe(SubArgs{Key: "k"})
	sub.Close()

	h.Notify(fsm.Event{Key: "k", Holder: "a", Token: 1})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); err == nil {
		t.Errorf("TestHubCloseUnsubscribes: Next: got err == nil, want a timeout error")
	}
}

// TestHubObservers proves Observers reports only tracked subscriptions (Observe
// streams) with their identity, scoped to a key or across all keys, and that Close
// removes a subscription from the listing.
func TestHubObservers(t *testing.T) {
	h := New()
	// A tracked observer, an untracked contender on the same key, and a tracked
	// observer on another key.
	obs := h.Subscribe(SubArgs{Key: "/a/lock", Identity: "alice", Track: true})
	contender := h.Subscribe(SubArgs{Key: "/a/lock", Identity: "bob"}) // Track false
	other := h.Subscribe(SubArgs{Key: "/b/lock", Identity: "carol", Track: true})
	defer contender.Close()
	defer other.Close()

	// Key-scoped: only the tracked observer of /a/lock, not the contender.
	got := h.Observers("/a/lock")
	want := []Observation{{Key: "/a/lock", Identity: "alice"}}
	if diff := pretty.Compare(want, got); diff != "" {
		t.Errorf("TestHubObservers: key-scoped: -want +got:\n%s", diff)
	}

	// All keys: both tracked observers, sorted here for a stable compare.
	all := h.Observers("")
	slices.SortFunc(all, func(a, b Observation) int { return strings.Compare(a.Key, b.Key) })
	wantAll := []Observation{{Key: "/a/lock", Identity: "alice"}, {Key: "/b/lock", Identity: "carol"}}
	if diff := pretty.Compare(wantAll, all); diff != "" {
		t.Errorf("TestHubObservers: all keys: -want +got:\n%s", diff)
	}

	// After Close, the observer is gone.
	obs.Close()
	if got := h.Observers("/a/lock"); len(got) != 0 {
		t.Errorf("TestHubObservers: after Close: got %v, want none", got)
	}
}
