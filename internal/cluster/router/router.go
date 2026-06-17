// Package router maps a lock/election key to the Raft shard that owns it. A single
// key always lands in exactly one shard, so all of its operations are single-shard
// and linearizable.
package router

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/gostdlib/base/values/immutable"
)

// Router resolves keys to shard ids by hashing the key over the shard set.
type Router struct {
	shards immutable.Slice[uint64]
}

// New returns a Router over the given shard ids. The order is fixed at
// construction so the key→shard mapping is stable.
func New(shards []uint64) (*Router, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("router.New: at least one shard is required")
	}
	return &Router{shards: immutable.NewSlice(append([]uint64(nil), shards...))}, nil
}

// Shard returns the shard id that owns key.
func (r *Router) Shard(key string) uint64 {
	return r.shards.Get(int(xxhash.Sum64String(key) % uint64(r.shards.Len())))
}

// Shards returns the shard ids this router spreads keys across.
func (r *Router) Shards() immutable.Slice[uint64] {
	return r.shards
}
