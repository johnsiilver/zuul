// Package router maps a lock/election key to the Raft shard that owns it. A single
// key always lands in exactly one shard, so all of its operations are single-shard
// and linearizable.
package router

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Router resolves keys to shard ids by hashing the key over the shard set.
type Router struct {
	shards []uint64
}

// New returns a Router over the given shard ids. The order is fixed at
// construction so the key→shard mapping is stable.
func New(shards []uint64) (*Router, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("router.New: at least one shard is required")
	}
	return &Router{shards: append([]uint64(nil), shards...)}, nil
}

// Shard returns the shard id that owns key.
func (r *Router) Shard(key string) uint64 {
	return r.shards[xxhash.Sum64String(key)%uint64(len(r.shards))]
}

// Shards returns the shard ids this router spreads keys across.
func (r *Router) Shards() []uint64 {
	return append([]uint64(nil), r.shards...)
}
