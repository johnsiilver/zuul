package watch

import (
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"github.com/kylelemons/godebug/pretty"

	"github.com/johnsiilver/zuul/internal/fsm"
)

// TestHubDeliversLatest proves a subscriber receives the most recent event for its
// key, coalescing intermediate ones.
func TestHubDeliversLatest(t *testing.T) {
	h := New()
	sub := h.Subscribe("k")
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
	sub := h.Subscribe("k")
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
	sub := h.Subscribe("k")
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
	sub := h.Subscribe("k")
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
	sub := h.Subscribe("k")
	sub.Close()

	h.Notify(fsm.Event{Key: "k", Holder: "a", Token: 1})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := sub.Next(ctx); err == nil {
		t.Errorf("TestHubCloseUnsubscribes: Next: got err == nil, want a timeout error")
	}
}
