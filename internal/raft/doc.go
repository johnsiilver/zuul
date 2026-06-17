// Package raft groups zuul's Raft-replicated core. It has no code of its own; its
// subpackages are the consensus layer and the replicated state machines it hosts:
//
//   - consensus: wraps a dragonboat NodeHost, hosting each shard's state machine on Raft.
//   - fsm:       the per-shard replicated state machine (locks and leases).
//   - meta:      the meta-shard replicated state machine (cluster membership map).
//   - cmd:       builds the command messages the fsm/meta state machines apply.
//   - forward:   routes a write to whichever node currently leads its shard.
//
// This grouping is purely organizational.
package raft
