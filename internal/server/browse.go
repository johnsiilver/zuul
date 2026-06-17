package server

import (
	"fmt"
	"slices"

	"github.com/gostdlib/base/concurrency/sync"
	"google.golang.org/grpc"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/authz"
	"github.com/johnsiilver/zuul/internal/auth/keypath"
	"github.com/johnsiilver/zuul/internal/cluster/router"
	"github.com/johnsiilver/zuul/internal/raft/fsm"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// Observer is one Observe-stream watcher of a key, attributed to a node.
type Observer struct {
	// Identity is the watcher's authenticated identity; "" if unauthenticated.
	Identity string
	// ReplicaID is the node hosting the subscription.
	ReplicaID uint64
}

// ObserverSet is the result of an ObserverSource lookup.
type ObserverSet struct {
	// Observers are the watchers found.
	Observers []Observer
	// Partial reports that some node could not be reached, so the list may be incomplete.
	Partial bool
}

// ObserverSource aggregates the Observe-stream watchers of a key. The cluster-wide
// aggregator (the forward dispatcher) implements it; a fake backs tests.
type ObserverSource interface {
	Observers(ctx context.Context, key string) (ObserverSet, error)
}

// BrowseConfig configures a BrowseServer.
type BrowseConfig struct {
	// Router resolves keys to shards and lists the lock shards to enumerate. Required.
	Router *router.Router
	// Reader serves reads from the local node (every shard is hosted locally). Required.
	Reader Reader
	// Observers aggregates Observe-stream watchers for GetRecord. Required.
	Observers ObserverSource
	// Authorizer gates browsing by the caller's identity. Default AllowAll.
	Authorizer authz.Authorizer
}

func (c *BrowseConfig) validate() error {
	switch {
	case c.Router == nil:
		return fmt.Errorf("server.BrowseConfig: Router is required")
	case c.Reader == nil:
		return fmt.Errorf("server.BrowseConfig: Reader is required")
	case c.Observers == nil:
		return fmt.Errorf("server.BrowseConfig: Observers is required")
	}
	if c.Authorizer == nil {
		c.Authorizer = authz.AllowAll()
	}
	return nil
}

// BrowseServer serves the read-only Browse API: enumerate locks/elections in a
// namespace and inspect one record's holder, contenders, and observers. It never
// mutates state and reads from the local node only (every shard is hosted locally).
type BrowseServer struct {
	zuulv1.UnimplementedBrowseServer

	cfg BrowseConfig
}

// NewBrowseServer returns a BrowseServer wired to the configured dependencies.
func NewBrowseServer(cfg BrowseConfig) (*BrowseServer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &BrowseServer{cfg: cfg}, nil
}

// Register registers the Browse service on reg.
func (b *BrowseServer) Register(reg grpc.ServiceRegistrar) {
	zuulv1.RegisterBrowseServer(reg, b)
}

// authorize gates a browse read on key (or the cluster admin namespace for a
// whole-cluster enumeration).
func (b *BrowseServer) authorize(ctx context.Context, key string) error {
	identity, _ := context.IdentityFromContext(ctx)
	if err := b.cfg.Authorizer.Authorize(identity, key, authz.Read); err != nil {
		return errors.E(ctx, errors.CatPermission, errors.TypeUnauthorizedKey, fmt.Errorf("client %q is not authorized to browse %q", identity, key))
	}
	return nil
}

// ListRecords enumerates the held locks/elections under req.Prefix, fanning the
// enumeration query out across every lock shard (all hosted locally) and merging the
// results sorted by key, with the set of namespaces present.
func (b *BrowseServer) ListRecords(ctx context.Context, req *zuulv1.ListRecordsRequest) (*zuulv1.ListRecordsResponse, error) {
	authKey := req.GetPrefix()
	if authKey == "" {
		authKey = clusterKey // browsing everything is an operator action
	}
	if err := b.authorize(ctx, authKey); err != nil {
		return nil, err
	}

	var (
		mu      sync.Mutex
		summary []fsm.LockSummary
	)
	g := context.Pool(ctx).Group()
	for _, shardID := range b.cfg.Router.Shards().All() {
		shardID := shardID
		g.Go(ctx, func(ctx context.Context) error {
			v, err := b.cfg.Reader.StaleRead(shardID, fsm.ListLocksQuery{Prefix: req.GetPrefix()})
			if err != nil {
				return fmt.Errorf("shard %d: %w", shardID, err)
			}
			ls, ok := v.(fsm.LockSummaries)
			if !ok {
				return fmt.Errorf("shard %d: unexpected read result %T", shardID, v)
			}
			mu.Lock()
			summary = append(summary, ls.Locks...)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		return nil, grpcErr(ctx, err)
	}

	slices.SortFunc(summary, func(a, c fsm.LockSummary) int { return cmpString(a.Name, c.Name) })
	out := &zuulv1.ListRecordsResponse{}
	seenNS := map[string]struct{}{}
	for _, s := range summary {
		out.Records = append(out.Records, summaryToRecord(s))
		if owner, err := keypath.Owner(s.Name); err == nil {
			if _, ok := seenNS[owner]; !ok {
				seenNS[owner] = struct{}{}
				out.Namespaces = append(out.Namespaces, owner)
			}
		}
	}
	slices.Sort(out.Namespaces)
	return out, nil
}

// GetRecord returns one key's holder/leader, fencing token, published value, FIFO
// contenders, and cluster-wide observers.
func (b *BrowseServer) GetRecord(ctx context.Context, req *zuulv1.GetRecordRequest) (*zuulv1.GetRecordResponse, error) {
	if err := requireKeyPath(ctx, req.GetKey()); err != nil {
		return nil, err
	}
	if err := b.authorize(ctx, req.GetKey()); err != nil {
		return nil, err
	}

	shardID := b.cfg.Router.Shard(req.GetKey())
	v, err := b.cfg.Reader.Read(ctx, shardID, fsm.WaitersQuery{Name: req.GetKey()})
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	w, ok := v.(fsm.Waiters)
	if !ok {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("unexpected read result %T", v))
	}

	obs, err := b.cfg.Observers.Observers(ctx, req.GetKey())
	if err != nil {
		return nil, grpcErr(ctx, err)
	}

	resp := &zuulv1.GetRecordResponse{
		Record: &zuulv1.Record{
			Key:            req.GetKey(),
			Kind:           kindOf(w.Value != nil),
			Held:           w.Held,
			HolderClientId: w.Holder,
			FencingToken:   w.Token,
			QueueDepth:     uint32(len(w.Entries)),
			Revision:       w.Revision,
		},
		Value:   w.Value,
		Partial: obs.Partial,
	}
	for _, e := range w.Entries {
		resp.Contenders = append(resp.Contenders, &zuulv1.Contender{ClientId: e.ClientID, Position: e.Position, EnqueueSeq: e.Seq})
	}
	for _, o := range obs.Observers {
		resp.Observers = append(resp.Observers, &zuulv1.Observer{Identity: o.Identity, ReplicaId: o.ReplicaID})
	}
	return resp, nil
}

// summaryToRecord maps an FSM lock summary to a Browse record.
func summaryToRecord(s fsm.LockSummary) *zuulv1.Record {
	return &zuulv1.Record{
		Key:            s.Name,
		Kind:           kindOf(s.HasValue),
		Held:           s.Held,
		HolderClientId: s.Holder,
		FencingToken:   s.Token,
		QueueDepth:     s.QueueDepth,
	}
}

// kindOf infers the record kind from whether a value has been published (best-effort).
func kindOf(hasValue bool) zuulv1.RecordKind {
	if hasValue {
		return zuulv1.RecordKind_RECORD_KIND_ELECTION
	}
	return zuulv1.RecordKind_RECORD_KIND_LOCK
}

// cmpString orders two strings ascending.
func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
