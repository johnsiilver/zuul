// Package consensus wires Zuul's per-shard FSM (internal/fsm) onto a dragonboat
// Raft NodeHost running entirely in memory. It owns the state-machine adapter
// (FSM <-> dragonboat IConcurrentStateMachine), the NodeHost lifecycle, the
// propose/read paths, and the leader-driven lease-expiry sweep.
package consensus

import (
	"fmt"
	"io"

	sm "github.com/johnsiilver/zuul/internal/dragonboat/statemachine"
	"github.com/johnsiilver/zuul/internal/fsm"
	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
	"github.com/johnsiilver/zuul/internal/meta"
)

// stateMachine adapts a *fsm.FSM to dragonboat's IConcurrentStateMachine: it
// marshals commands and results on the wire and serializes the FSM snapshot. The
// FSM itself does its own locking, so concurrent Lookup during Update is safe.
type stateMachine struct {
	fsm *fsm.FSM
}

// newFactory returns a dragonboat factory that builds the right state machine for
// each shard: the meta-shard topology store for metaShardID, otherwise a lock FSM
// wired to notifier (the watch hub). metaShardID == 0 means no meta shard.
func newFactory(notifier fsm.Notifier, metaShardID uint64) sm.CreateConcurrentStateMachineFunc {
	return func(clusterID, nodeID uint64) sm.IConcurrentStateMachine {
		if metaShardID != 0 && clusterID == metaShardID {
			return &metaStateMachine{store: meta.NewStore()}
		}
		return &stateMachine{fsm: fsm.New(notifier)}
	}
}

// Update applies a batch of committed log entries, recording each command's
// marshalled CommandResult (and its Outcome code) back onto the entry. A non-nil
// error here is fatal to the node, so it is returned only for genuine corruption
// (an undecodable command or unmarshalable result), never for a domain rejection.
func (s *stateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	for i := range entries {
		cmd := &fsmpb.Command{}
		if err := cmd.UnmarshalVT(entries[i].Cmd); err != nil {
			return nil, fmt.Errorf("consensus: corrupt command at index %d: %w", entries[i].Index, err)
		}
		res, err := s.fsm.Apply(cmd)
		if err != nil {
			return nil, fmt.Errorf("consensus: apply at index %d: %w", entries[i].Index, err)
		}
		data, err := res.MarshalVT()
		if err != nil {
			return nil, fmt.Errorf("consensus: marshal result at index %d: %w", entries[i].Index, err)
		}
		entries[i].Result = sm.Result{Value: uint64(res.GetOutcome()), Data: data}
	}
	return entries, nil
}

// Lookup answers a read query (a fsm.StatusQuery / LeaseQuery / DueLeasesQuery),
// returning the corresponding fsm result value.
func (s *stateMachine) Lookup(query any) (any, error) {
	return s.fsm.Query(query)
}

// PrepareSnapshot captures a consistent point-in-time copy of the shard state for
// SaveSnapshot to serialize while Update continues.
func (s *stateMachine) PrepareSnapshot() (any, error) {
	return s.fsm.Snapshot(), nil
}

// SaveSnapshot serializes the prepared snapshot to w. The whole shard state is one
// length-agnostic blob (the snapshot file is its own boundary).
func (s *stateMachine) SaveSnapshot(state any, w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	snap, ok := state.(*fsmpb.Snapshot)
	if !ok {
		return fmt.Errorf("consensus: SaveSnapshot got %T, want *fsmpb.Snapshot", state)
	}
	b, err := snap.MarshalVT()
	if err != nil {
		return fmt.Errorf("consensus: marshal snapshot: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("consensus: write snapshot: %w", err)
	}
	return nil
}

// RecoverFromSnapshot replaces the shard state with the snapshot read from r.
func (s *stateMachine) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("consensus: read snapshot: %w", err)
	}
	snap := &fsmpb.Snapshot{}
	if err := snap.UnmarshalVT(b); err != nil {
		return fmt.Errorf("consensus: unmarshal snapshot: %w", err)
	}
	s.fsm.Restore(snap)
	return nil
}

// Close releases the state machine. The FSM holds only memory, so there is
// nothing to free.
func (s *stateMachine) Close() error {
	return nil
}
