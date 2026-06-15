package integration

import (
	"testing"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"google.golang.org/grpc"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// TestElectionBasic covers a single-candidate lifecycle: campaign wins leadership,
// Leader reflects it, Proclaim updates the value, and Resign vacates the post.
func TestElectionBasic(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()
	n := c.nodes[0]
	openSession(n, "a")

	camp, err := n.srv.Campaign(ctx, &zuulv1.CampaignRequest{Name: "/test/E", ClientId: "a", Value: []byte("a-v1")})
	if err != nil {
		t.Fatalf("TestElectionBasic: Campaign: got err == %s, want err == nil", err)
	}
	if !camp.GetLeadership() || camp.GetFencingToken() != 1 {
		t.Fatalf("TestElectionBasic: Campaign: leadership=%v token=%d, want true/1", camp.GetLeadership(), camp.GetFencingToken())
	}

	ld, err := n.srv.Leader(ctx, &zuulv1.LeaderRequest{Name: "/test/E"})
	if err != nil {
		t.Fatalf("TestElectionBasic: Leader: got err == %s, want err == nil", err)
	}
	if !ld.GetHasLeader() || ld.GetLeaderClientId() != "a" || string(ld.GetValue()) != "a-v1" {
		t.Errorf("TestElectionBasic: Leader: hasLeader=%v leader=%q value=%q, want true/a/a-v1", ld.GetHasLeader(), ld.GetLeaderClientId(), ld.GetValue())
	}

	if _, err := n.srv.Proclaim(ctx, &zuulv1.ProclaimRequest{Name: "/test/E", ClientId: "a", FencingToken: camp.GetFencingToken(), Value: []byte("a-v2")}); err != nil {
		t.Fatalf("TestElectionBasic: Proclaim: got err == %s, want err == nil", err)
	}
	if _, err := n.srv.Proclaim(ctx, &zuulv1.ProclaimRequest{Name: "/test/E", ClientId: "a", FencingToken: 999, Value: []byte("x")}); err == nil {
		t.Errorf("TestElectionBasic: Proclaim with stale token: got err == nil, want err != nil")
	}

	ld2, _ := n.srv.Leader(ctx, &zuulv1.LeaderRequest{Name: "/test/E"})
	if string(ld2.GetValue()) != "a-v2" {
		t.Errorf("TestElectionBasic: Leader value after Proclaim: got %q, want a-v2", ld2.GetValue())
	}

	if _, err := n.srv.Resign(ctx, &zuulv1.ResignRequest{Name: "/test/E", ClientId: "a", FencingToken: camp.GetFencingToken()}); err != nil {
		t.Fatalf("TestElectionBasic: Resign: got err == %s, want err == nil", err)
	}
	ld3, _ := n.srv.Leader(ctx, &zuulv1.LeaderRequest{Name: "/test/E"})
	if ld3.GetHasLeader() {
		t.Errorf("TestElectionBasic: Leader after Resign: hasLeader=true, want false")
	}
}

// TestElectionTransfer proves a blocked candidate is promoted when the leader resigns,
// taking over with its own value and a strictly larger fencing token — across nodes.
func TestElectionTransfer(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()
	a := c.nodes[0]
	b := c.nodes[1]
	openSession(a, "a")
	openSession(b, "b")

	first, err := a.srv.Campaign(ctx, &zuulv1.CampaignRequest{Name: "/test/E", ClientId: "a", Value: []byte("a-v")})
	if err != nil || !first.GetLeadership() {
		t.Fatalf("TestElectionTransfer: a Campaign: leadership=%v err=%v, want true/nil", first.GetLeadership(), err)
	}

	type result struct {
		resp *zuulv1.CampaignResponse
		err  error
	}
	ch := make(chan result, 1)
	context.Pool(ctx).Submit(ctx, func() {
		resp, err := b.srv.Campaign(ctx, &zuulv1.CampaignRequest{Name: "/test/E", ClientId: "b", Value: []byte("b-v")})
		ch <- result{resp, err}
	})

	// Wait until b is queued behind the leader (status uses the same key namespace).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := b.srv.Status(ctx, &zuulv1.StatusRequest{Name: "/test/E"})
		if st.GetQueueDepth() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := a.srv.Resign(ctx, &zuulv1.ResignRequest{Name: "/test/E", ClientId: "a", FencingToken: first.GetFencingToken()}); err != nil {
		t.Fatalf("TestElectionTransfer: a Resign: got err == %s, want err == nil", err)
	}

	got := <-ch
	if got.err != nil || !got.resp.GetLeadership() {
		t.Fatalf("TestElectionTransfer: b Campaign result: leadership=%v err=%v, want true/nil", got.resp.GetLeadership(), got.err)
	}
	if got.resp.GetFencingToken() <= first.GetFencingToken() {
		t.Errorf("TestElectionTransfer: b token=%d, want > %d", got.resp.GetFencingToken(), first.GetFencingToken())
	}

	ld, _ := b.srv.Leader(ctx, &zuulv1.LeaderRequest{Name: "/test/E"})
	if ld.GetLeaderClientId() != "b" || string(ld.GetValue()) != "b-v" {
		t.Errorf("TestElectionTransfer: Leader after transfer: leader=%q value=%q, want b/b-v", ld.GetLeaderClientId(), ld.GetValue())
	}
}

// TestObserve proves the Observe stream reports the current leader, then each change:
// a new leader, a value update, and going leaderless.
func TestObserve(t *testing.T) {
	c := newCluster(t, 3, 4)
	ctx := t.Context()
	n := c.nodes[0]
	openSession(n, "a")

	obsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := &fakeObserve{ctx: obsCtx}
	context.Pool(ctx).Submit(ctx, func() { _ = n.srv.Observe(&zuulv1.ObserveRequest{Name: "/test/E"}, stream) })

	camp, err := n.srv.Campaign(ctx, &zuulv1.CampaignRequest{Name: "/test/E", ClientId: "a", Value: []byte("v1")})
	if err != nil {
		t.Fatalf("TestObserve: Campaign: got err == %s, want err == nil", err)
	}
	waitObserve(t, stream, func(e *zuulv1.LeaderEvent) bool {
		return e.GetHasLeader() && e.GetLeaderClientId() == "a" && string(e.GetValue()) == "v1"
	}, "leader a with v1")

	if _, err := n.srv.Proclaim(ctx, &zuulv1.ProclaimRequest{Name: "/test/E", ClientId: "a", FencingToken: camp.GetFencingToken(), Value: []byte("v2")}); err != nil {
		t.Fatalf("TestObserve: Proclaim: got err == %s, want err == nil", err)
	}
	waitObserve(t, stream, func(e *zuulv1.LeaderEvent) bool {
		return e.GetHasLeader() && string(e.GetValue()) == "v2"
	}, "value v2")

	if _, err := n.srv.Resign(ctx, &zuulv1.ResignRequest{Name: "/test/E", ClientId: "a", FencingToken: camp.GetFencingToken()}); err != nil {
		t.Fatalf("TestObserve: Resign: got err == %s, want err == nil", err)
	}
	waitObserve(t, stream, func(e *zuulv1.LeaderEvent) bool {
		return !e.GetHasLeader()
	}, "leaderless")
}

// waitObserve waits until the observe stream has sent an event satisfying pred.
func waitObserve(t *testing.T, s *fakeObserve, pred func(*zuulv1.LeaderEvent) bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range s.snapshot() {
			if pred(e) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waitObserve: never observed %s", desc)
}

// fakeObserve is an Election_ObserveServer that records streamed events.
type fakeObserve struct {
	grpc.ServerStream
	ctx    context.Context
	mu     sync.Mutex
	events []*zuulv1.LeaderEvent
}

func (f *fakeObserve) Context() context.Context { return f.ctx }

func (f *fakeObserve) Send(e *zuulv1.LeaderEvent) error {
	f.mu.Lock()
	f.events = append(f.events, e)
	f.mu.Unlock()
	return nil
}

func (f *fakeObserve) snapshot() []*zuulv1.LeaderEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*zuulv1.LeaderEvent(nil), f.events...)
}
