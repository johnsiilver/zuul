// Package lock groups zuul's lock-service runtime support. It has no code of its own; its
// subpackages back the client-facing locking semantics:
//
//   - session: manages client leases across shards (a lease keeps a client's locks alive).
//   - watch:   a per-node notification hub the shard FSM drives to wake waiting clients.
//
// The replicated lock state itself lives in the raft/fsm state machine; these packages are
// the runtime support around it. This grouping is purely organizational.
package lock
