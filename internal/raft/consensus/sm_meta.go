package consensus

import (
	"fmt"
	"io"

	sm "github.com/johnsiilver/zuul/internal/dragonboat/statemachine"
	"github.com/johnsiilver/zuul/internal/raft/meta"
	"github.com/johnsiilver/zuul/internal/raft/meta/metapb"
)

// metaStateMachine adapts a *meta.Store to dragonboat's IConcurrentStateMachine for
// the meta shard, mirroring stateMachine but over the topology map.
type metaStateMachine struct {
	store *meta.Store
}

// Update applies a batch of committed meta-shard entries.
func (s *metaStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	for i := range entries {
		c := &metapb.MetaCommand{}
		if err := c.UnmarshalVT(entries[i].Cmd); err != nil {
			return nil, fmt.Errorf("consensus: corrupt meta command at index %d: %w", entries[i].Index, err)
		}
		res, err := s.store.Apply(c)
		if err != nil {
			return nil, fmt.Errorf("consensus: apply meta at index %d: %w", entries[i].Index, err)
		}
		data, err := res.MarshalVT()
		if err != nil {
			return nil, fmt.Errorf("consensus: marshal meta result at index %d: %w", entries[i].Index, err)
		}
		entries[i].Result = sm.Result{Value: res.GetRevision(), Data: data}
	}
	return entries, nil
}

// Lookup answers a meta read (meta.MemberQuery / meta.ListQuery).
func (s *metaStateMachine) Lookup(query any) (any, error) {
	return s.store.Query(query)
}

// PrepareSnapshot captures a point-in-time copy of the meta state.
func (s *metaStateMachine) PrepareSnapshot() (any, error) {
	return s.store.Snapshot(), nil
}

// SaveSnapshot serializes the prepared meta snapshot to w.
func (s *metaStateMachine) SaveSnapshot(state any, w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	snap, ok := state.(*metapb.MetaSnapshot)
	if !ok {
		return fmt.Errorf("consensus: meta SaveSnapshot got %T, want *metapb.MetaSnapshot", state)
	}
	b, err := snap.MarshalVT()
	if err != nil {
		return fmt.Errorf("consensus: marshal meta snapshot: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("consensus: write meta snapshot: %w", err)
	}
	return nil
}

// RecoverFromSnapshot replaces the meta state with the snapshot read from r.
func (s *metaStateMachine) RecoverFromSnapshot(r io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("consensus: read meta snapshot: %w", err)
	}
	snap := &metapb.MetaSnapshot{}
	if err := snap.UnmarshalVT(b); err != nil {
		return fmt.Errorf("consensus: unmarshal meta snapshot: %w", err)
	}
	s.store.Restore(snap)
	return nil
}

// Close releases the meta state machine.
func (s *metaStateMachine) Close() error {
	return nil
}
