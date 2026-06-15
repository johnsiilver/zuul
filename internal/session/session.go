// Package session manages client leases across shards. A lock lives in exactly one
// shard, and acquiring it requires a lease in that shard, so a client's lease is
// per-shard and created lazily the first time the client touches a shard. One
// heartbeat on the client's KeepAlive stream fans a renewal out to exactly the
// shards the client holds leases in; stream close revokes them all.
package session

import (
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/cmd"
	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
)

// ErrNoSession indicates an operation referenced a client with no open session.
var ErrNoSession = errors.New("session: no open session for client")

// Proposer commits an opaque (marshalled) command to a shard and returns the
// marshalled result (the forward dispatcher).
type Proposer interface {
	Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error)
}

// Manager tracks open sessions and their per-shard leases.
type Manager struct {
	proposer Proposer
	now      func() int64

	mu       sync.Mutex
	sessions map[string]*session
}

// session is one client's lease state: its TTL and the shards it holds a lease in.
// Its own mutex serializes lease changes for that client without blocking others.
type session struct {
	mu     sync.Mutex
	ttlMS  int64
	shards map[uint64]struct{}
}

// New returns a Manager that proposes through proposer and stamps lease commands
// with now (unix nanoseconds).
func New(proposer Proposer, now func() int64) *Manager {
	return &Manager{
		proposer: proposer,
		now:      now,
		sessions: map[string]*session{},
	}
}

// propose marshals an FSM command, dispatches it, and decodes the result.
func (m *Manager) propose(ctx context.Context, shardID uint64, c *fsmpb.Command) (*fsmpb.CommandResult, error) {
	b, err := c.MarshalVT()
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeMarshal, err)
	}
	rb, err := m.proposer.Propose(ctx, shardID, b)
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeBackend, err)
	}
	res := &fsmpb.CommandResult{}
	if err := res.UnmarshalVT(rb); err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeMarshal, err)
	}
	return res, nil
}

// Open registers (or refreshes the TTL of) a client's session. It grants no leases
// yet — those are created lazily by EnsureLease.
func (m *Manager) Open(clientID string, ttlMS int64) {
	m.mu.Lock()
	s := m.sessions[clientID]
	if s == nil {
		s = &session{shards: map[uint64]struct{}{}}
		m.sessions[clientID] = s
	}
	m.mu.Unlock()

	s.mu.Lock()
	s.ttlMS = ttlMS
	s.mu.Unlock()
}

// Attach records that the client already holds a lease in shardID — the reconnect
// case, where the lease was granted via another node and this node's manager has no
// record of it — so keepalives renew it. ErrNoSession if no session is open.
func (m *Manager) Attach(clientID string, shardID uint64) error {
	s := m.get(clientID)
	if s == nil {
		return ErrNoSession
	}
	s.mu.Lock()
	s.shards[shardID] = struct{}{}
	s.mu.Unlock()
	return nil
}

// get returns the client's session, or nil.
func (m *Manager) get(clientID string) *session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[clientID]
}

// EnsureLease guarantees the client holds a lease in shardID, granting one if it
// does not yet. It returns ErrNoSession if the client has no open session.
func (m *Manager) EnsureLease(ctx context.Context, clientID string, shardID uint64) error {
	s := m.get(clientID)
	if s == nil {
		return errors.E(ctx, errors.CatPrecondition, errors.TypeNoSession, ErrNoSession)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[shardID]; ok {
		return nil
	}
	if _, err := m.propose(ctx, shardID, cmd.LeaseGrant(clientID, s.ttlMS, m.now())); err != nil {
		return err
	}
	s.shards[shardID] = struct{}{}
	return nil
}

// KeepAlive renews the client's lease in every shard it holds one, concurrently,
// and returns the new deadline (unix nanoseconds). A shard whose lease has lapsed
// is dropped from the session. Returns ErrNoSession if there is no open session.
func (m *Manager) KeepAlive(ctx context.Context, clientID string) (int64, error) {
	s := m.get(clientID)
	if s == nil {
		return 0, errors.E(ctx, errors.CatPrecondition, errors.TypeNoSession, ErrNoSession)
	}
	s.mu.Lock()
	shards := make([]uint64, 0, len(s.shards))
	for sh := range s.shards {
		shards = append(shards, sh)
	}
	ttlMS := s.ttlMS
	s.mu.Unlock()

	now := m.now()
	deadline := now + ttlMS*int64(time.Millisecond)

	g := context.Pool(ctx).Group()
	for _, sh := range shards {
		sh := sh
		g.Go(ctx, func(ctx context.Context) error {
			res, err := m.propose(ctx, sh, cmd.KeepAlive(clientID, now))
			if err != nil {
				return err
			}
			if res.GetOutcome() == fsmpb.Outcome_OUTCOME_NOT_FOUND {
				s.mu.Lock()
				delete(s.shards, sh)
				s.mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		return 0, err
	}
	return deadline, nil
}

// Close revokes every lease the client holds and removes its session. Best effort:
// individual revoke failures are ignored, since the lease will expire on its own.
func (m *Manager) Close(ctx context.Context, clientID string) {
	m.mu.Lock()
	s := m.sessions[clientID]
	delete(m.sessions, clientID)
	m.mu.Unlock()
	if s == nil {
		return
	}

	s.mu.Lock()
	shards := make([]uint64, 0, len(s.shards))
	for sh := range s.shards {
		shards = append(shards, sh)
	}
	s.mu.Unlock()

	g := context.Pool(ctx).Group()
	for _, sh := range shards {
		sh := sh
		g.Go(ctx, func(ctx context.Context) error {
			_, _ = m.propose(ctx, sh, cmd.Revoke(clientID))
			return nil
		})
	}
	_ = g.Wait(ctx)
}
