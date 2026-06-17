package server

import (
	"fmt"
	"slices"
	"testing"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
	"github.com/johnsiilver/zuul/internal/raft/meta/metapb"
)

// fakeMembers is a MemberLister with a fixed self id and member list.
type fakeMembers struct {
	self    uint64
	members []*metapb.Member
}

func (f fakeMembers) ReplicaID() uint64 { return f.self }
func (f fakeMembers) MetaList(ctx context.Context) ([]*metapb.Member, error) {
	return f.members, nil
}

// fakePeers serves per-address observer responses; an address absent from byAddr returns
// an error, standing in for an unreachable peer.
type fakePeers struct {
	byAddr map[string]*forwardpb.ListObserversResponse
}

func (f fakePeers) ListObservers(ctx context.Context, addr, key string) (*forwardpb.ListObserversResponse, error) {
	resp, ok := f.byAddr[addr]
	if !ok {
		return nil, fmt.Errorf("peer %s unreachable", addr)
	}
	return resp, nil
}

// TestObserverAggregator proves observers are merged from the local hub and reachable
// peers, and that an unreachable peer sets Partial without dropping the rest.
func TestObserverAggregator(t *testing.T) {
	const key = "/alice/lock"

	hub := watch.New()
	local := hub.Subscribe(watch.SubArgs{Key: key, Identity: "alice", Track: true})
	defer local.Close()

	members := fakeMembers{
		self: 1,
		members: []*metapb.Member{
			{ReplicaId: 1, ZuulGrpcAddress: "self:1"},  // skipped (self)
			{ReplicaId: 2, ZuulGrpcAddress: "nodeB:1"}, // reachable
			{ReplicaId: 3, ZuulGrpcAddress: "nodeC:1"}, // unreachable -> partial
		},
	}
	peers := fakePeers{byAddr: map[string]*forwardpb.ListObserversResponse{
		"nodeB:1": {ReplicaId: 2, Observers: []*forwardpb.ObserverEntry{{Key: key, Identity: "bob"}}},
		// nodeC:1 intentionally absent -> error
	}}

	agg := NewObserverAggregator(hub, members, peers)
	got, err := agg.Observers(t.Context(), key)
	if err != nil {
		t.Fatalf("TestObserverAggregator: got err == %s, want err == nil", err)
	}
	if !got.Partial {
		t.Errorf("TestObserverAggregator: Partial = false, want true (a peer was unreachable)")
	}
	var ids []string
	for _, o := range got.Observers {
		ids = append(ids, o.Identity)
	}
	slices.Sort(ids)
	want := []string{"alice", "bob"}
	if !slices.Equal(want, ids) {
		t.Errorf("TestObserverAggregator: identities = %v, want %v", ids, want)
	}
}
