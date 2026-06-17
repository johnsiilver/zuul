// Package meta is the replicated state machine for Zuul's meta shard: a small map
// from replica id to that node's addresses (the cluster topology). It is pure and
// transport-agnostic, like internal/fsm; a consensus adapter drives it over Raft.
package meta

import (
	"fmt"
	"slices"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/internal/raft/meta/metapb"
)

// Store is the meta shard's state: the set of cluster members, keyed by replica id,
// plus a logical clock. Safe for concurrent use.
type Store struct {
	mu       sync.RWMutex
	members  map[uint64]*metapb.Member
	revision uint64
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{members: map[uint64]*metapb.Member{}}
}

// Apply executes one committed meta command. The error is non-nil only for a
// malformed command (log corruption), which is fatal.
func (s *Store) Apply(c *metapb.MetaCommand) (*metapb.MetaResult, error) {
	if c == nil {
		return nil, fmt.Errorf("meta.Apply: nil command")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.revision++
	switch cmd := c.GetCmd().(type) {
	case *metapb.MetaCommand_Put:
		m := cmd.Put
		s.members[m.GetReplicaId()] = cloneMember(m)
	case *metapb.MetaCommand_Delete:
		delete(s.members, cmd.Delete)
	default:
		return nil, fmt.Errorf("meta.Apply: unknown command type %T", cmd)
	}
	return &metapb.MetaResult{Revision: s.revision}, nil
}

// MemberQuery asks for a single member by replica id.
type MemberQuery struct {
	// ReplicaID is the member to look up.
	ReplicaID uint64
}

// ListQuery asks for every member.
type ListQuery struct{}

// Query answers a read. q is a MemberQuery (result *metapb.Member, nil if absent)
// or a ListQuery (result []*metapb.Member, sorted by replica id).
func (s *Store) Query(q any) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch query := q.(type) {
	case MemberQuery:
		m, ok := s.members[query.ReplicaID]
		if !ok {
			return (*metapb.Member)(nil), nil
		}
		return cloneMember(m), nil
	case ListQuery:
		return s.list(), nil
	default:
		return nil, fmt.Errorf("meta.Query: unknown query type %T", q)
	}
}

// list returns every member, sorted by replica id.
func (s *Store) list() []*metapb.Member {
	ids := make([]uint64, 0, len(s.members))
	for id := range s.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]*metapb.Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneMember(s.members[id]))
	}
	return out
}

// Snapshot returns a deterministic copy of the whole meta state.
func (s *Store) Snapshot() *metapb.MetaSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &metapb.MetaSnapshot{Revision: s.revision, Members: s.list()}
}

// Restore replaces all state with the contents of snap.
func (s *Store) Restore(snap *metapb.MetaSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision = snap.GetRevision()
	s.members = make(map[uint64]*metapb.Member, len(snap.GetMembers()))
	for _, m := range snap.GetMembers() {
		s.members[m.GetReplicaId()] = cloneMember(m)
	}
}

// cloneMember returns an independent copy of m so stored state never aliases a
// caller's proto.
func cloneMember(m *metapb.Member) *metapb.Member {
	return &metapb.Member{
		ReplicaId:       m.GetReplicaId(),
		RaftAddress:     m.GetRaftAddress(),
		ZuulGrpcAddress: m.GetZuulGrpcAddress(),
		NodeHostId:      m.GetNodeHostId(),
	}
}
