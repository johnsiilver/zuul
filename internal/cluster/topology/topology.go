// Package topology resolves a node's replica id to the address of its internal
// forwarding (node-to-node) gRPC endpoint, so a write can be proxied to whichever
// node currently leads a shard. Phase 4 ships a static, config-provided registry;
// the gossip-mode dynamic resolver (a meta shard) plugs in behind the same
// interface later.
package topology

import (
	"fmt"

	"github.com/gostdlib/base/values/immutable"
)

// Resolver maps a replica id to its forwarding gRPC address.
type Resolver interface {
	// Addr returns the forwarding address for replicaID and whether it is known.
	Addr(replicaID uint64) (string, bool)
}

// Static is a fixed replica-id → address registry. The table is read-only after
// construction, so it is held as an immutable.Map.
type Static struct {
	addrs immutable.Map[uint64, string]
}

// NewStatic returns a Static resolver over a copy of addrs.
func NewStatic(addrs map[uint64]string) (*Static, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("topology.NewStatic: addrs is required")
	}
	m := make(map[uint64]string, len(addrs))
	for id, a := range addrs {
		m[id] = a
	}
	return &Static{addrs: immutable.NewMap(m)}, nil
}

// Addr returns the forwarding address for replicaID.
func (s *Static) Addr(replicaID uint64) (string, bool) {
	return s.addrs.Get(replicaID)
}
