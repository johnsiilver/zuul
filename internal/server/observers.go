package server

import (
	"time"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
	"github.com/johnsiilver/zuul/internal/raft/meta/metapb"
)

// observerFanoutTimeout bounds the per-peer observer fetch so a slow or dead node cannot
// hang a GetRecord detail view.
const observerFanoutTimeout = 3 * time.Second

// PeerObservers fetches one peer's local watch-hub observers over the forward plane.
// *forward.Dispatcher satisfies it.
type PeerObservers interface {
	ListObservers(ctx context.Context, addr, key string) (*forwardpb.ListObserversResponse, error)
}

// MemberLister lists the cluster's members and reports this node's replica id.
// *consensus.Host satisfies it.
type MemberLister interface {
	MetaList(ctx context.Context) ([]*metapb.Member, error)
	ReplicaID() uint64
}

// ObserverAggregator implements ObserverSource by combining the local watch hub with a
// fan-out to every peer's Forwarder.ListObservers. The hub is node-local, so observers
// connected to other nodes are only visible by asking those nodes. It is partial-failure
// tolerant: an unreachable peer is logged and flagged, not fatal.
type ObserverAggregator struct {
	local   *watch.Hub
	members MemberLister
	peers   PeerObservers
}

// NewObserverAggregator returns an aggregator over the local hub, the cluster member
// list, and the peer fan-out.
func NewObserverAggregator(local *watch.Hub, members MemberLister, peers PeerObservers) *ObserverAggregator {
	return &ObserverAggregator{local: local, members: members, peers: peers}
}

// Observers returns key's observers across the cluster: this node's local hub plus every
// reachable peer. Partial is set when the member list or any peer could not be reached.
func (a *ObserverAggregator) Observers(ctx context.Context, key string) (ObserverSet, error) {
	selfID := a.members.ReplicaID()
	var (
		mu      sync.Mutex
		out     []Observer
		partial bool
	)
	for _, o := range a.local.Observers(key) {
		out = append(out, Observer{Identity: o.Identity, ReplicaID: selfID})
	}

	members, err := a.members.MetaList(ctx)
	if err != nil {
		// Local observers are still useful; flag the result as incomplete.
		context.Log(ctx).Warn("browse: listing members for observer fan-out failed", "err", err.Error())
		return ObserverSet{Observers: out, Partial: true}, nil
	}

	fctx, cancel := context.WithTimeout(ctx, observerFanoutTimeout)
	defer cancel()
	g := context.Pool(fctx).Group()
	for _, m := range members {
		m := m
		if m.GetReplicaId() == selfID || m.GetZuulGrpcAddress() == "" {
			continue
		}
		g.Go(fctx, func(ctx context.Context) error {
			resp, err := a.peers.ListObservers(ctx, m.GetZuulGrpcAddress(), key)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				partial = true
				context.Log(ctx).Warn("browse: peer observer fetch failed", "addr", m.GetZuulGrpcAddress(), "err", err.Error())
				return nil
			}
			for _, e := range resp.GetObservers() {
				out = append(out, Observer{Identity: e.GetIdentity(), ReplicaID: resp.GetReplicaId()})
			}
			return nil
		})
	}
	_ = g.Wait(fctx)
	return ObserverSet{Observers: out, Partial: partial}, nil
}
