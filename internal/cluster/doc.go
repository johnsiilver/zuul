// Package cluster groups zuul's membership and placement packages. It has no code of its
// own; its subpackages answer "where do members and keys live":
//
//   - discovery: derives the cluster's member list from a deployment topology (e.g. k8s).
//   - topology:  resolves a replica id to the address of its node.
//   - router:    maps a lock/election key to the Raft shard that owns it.
//
// This grouping is purely organizational.
package cluster
