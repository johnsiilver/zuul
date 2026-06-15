package consensus

// MetaResolver resolves a replica id to its forwarding gRPC address from the meta
// shard, falling back to a static seed for the initial members. The seed lets the
// dispatcher forward writes during bootstrap (before every node has recorded itself
// in the meta shard); once recorded — including for nodes added at runtime — the
// meta shard is authoritative. It satisfies forward.Resolver.
type MetaResolver struct {
	host *Host
	seed map[uint64]string
}

// NewMetaResolver returns a resolver backed by host's meta shard, with seed as the
// bootstrap fallback (replica id → forwarding address for the initial members).
func NewMetaResolver(host *Host, seed map[uint64]string) *MetaResolver {
	s := make(map[uint64]string, len(seed))
	for id, a := range seed {
		s[id] = a
	}
	return &MetaResolver{host: host, seed: s}
}

// Addr returns the forwarding address for replicaID.
func (r *MetaResolver) Addr(replicaID uint64) (string, bool) {
	if m, ok := r.host.MetaMember(replicaID); ok && m.GetZuulGrpcAddress() != "" {
		return m.GetZuulGrpcAddress(), true
	}
	a, ok := r.seed[replicaID]
	return a, ok
}
