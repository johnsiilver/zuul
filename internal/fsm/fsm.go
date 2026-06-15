// Package fsm is Zuul's per-shard replicated state machine: the in-memory locks,
// leases, FIFO wait-queues, and fencing tokens that back one Raft group. It is
// pure and transport-agnostic — it knows nothing about dragonboat or gRPC — so it
// can be driven and tested directly with command structs. A thin adapter (added
// when the NodeHost is wired) marshals []byte <-> fsmpb.Command and forwards to
// Apply/Query/Snapshot/Restore.
//
// # Determinism
//
// Every replica applies the same committed commands in the same order and must
// reach byte-identical state. So Apply is a pure function of (command, current
// state):
//
//   - It never reads the wall clock. Anything time-dependent (lease deadlines,
//     expiry due-checks) is carried in the command as a leader-stamped
//     now_unix_nano field.
//   - When one command touches several keys (a lease drop releasing many locks),
//     those keys are processed in sorted order, never Go map order, so the
//     monotonic seq — and therefore the fencing tokens handed out — match on
//     every replica.
//
// # Tokens and revision
//
// seq is a single monotonic counter that backs both fencing tokens (assigned on
// each grant) and waiter enqueue order; because it only increases, fencing tokens
// are strictly monotonic per key. revision is a logical clock bumped once per
// applied command and reported back to callers.
package fsm

import (
	"fmt"
	"slices"
	"time"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
)

// Event describes a change in ownership of a key, published to a Notifier so a
// per-node watch hub can wake a blocked Lock waiter or an Election observer. A
// promotion, a fresh grant, a value change (Proclaim), and a key going free all
// emit one.
type Event struct {
	// Key is the lock/election name that changed.
	Key string
	// Holder is the new holder/leader's client id, or "" if the key is now free.
	Holder string
	// Token is the new holder's fencing token, or 0 if the key is now free.
	Token uint64
	// Value is the leader's published value for an election; nil for plain locks.
	Value []byte
	// Revision is the FSM logical clock at which the change happened.
	Revision uint64
}

// Notifier receives ownership-change events as they are applied. Implementations
// must not block the caller (the Raft apply loop) and must not call back into the
// FSM. A nil Notifier disables notification.
type Notifier interface {
	Notify(Event)
}

// lock is the in-memory state of one lock or election key. A lock is removed from
// the map entirely once it has no holder and no waiters, so an existing lock
// always has a non-empty holder.
type lock struct {
	name   string
	holder string // "" only transiently during release, before promote runs
	token  uint64
	value  []byte
	queue  []*waiter
}

// waiter is one entry in a key's FIFO queue.
type waiter struct {
	clientID string
	seq      uint64 // enqueue order, from the monotonic seq
	value    []byte // election value to publish if promoted; nil for plain locks
}

// lease is one client's lease within this shard. held and waiting index the keys
// the client holds and is queued on, so a lease drop can release/dequeue them
// without scanning every lock. Both are rebuilt from the lock map on Restore.
type lease struct {
	clientID string
	ttlMS    int64
	expireAt int64 // unix-nano; leader expires the lease once its clock passes this
	held     map[string]struct{}
	waiting  map[string]struct{}
}

// FSM is one shard's replicated state machine. It is safe for concurrent use:
// Apply/Restore take a write lock, Query/Snapshot a read lock.
type FSM struct {
	mu       sync.RWMutex
	locks    map[string]*lock
	leases   map[string]*lease
	seq      uint64
	revision uint64
	notifier Notifier
}

// New returns an empty FSM. notifier may be nil to disable ownership-change
// notifications (e.g. in unit tests that only assert state).
func New(notifier Notifier) *FSM {
	return &FSM{
		locks:    map[string]*lock{},
		leases:   map[string]*lease{},
		notifier: notifier,
	}
}

// Apply executes one committed command and returns its result. The error is
// non-nil only for a malformed command (nil, or an unset oneof), which indicates
// log corruption and is fatal; all ordinary rejections are reported as an Outcome
// on the result, never as an error.
func (f *FSM) Apply(cmd *fsmpb.Command) (*fsmpb.CommandResult, error) {
	if cmd == nil {
		return nil, fmt.Errorf("fsm.Apply: nil command")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.revision++

	switch c := cmd.GetCmd().(type) {
	case *fsmpb.Command_AcquireLock:
		a := c.AcquireLock
		return f.acquire(a.GetName(), a.GetClientId(), nil, a.GetTryLock()), nil
	case *fsmpb.Command_Campaign:
		cp := c.Campaign
		return f.acquire(cp.GetName(), cp.GetClientId(), cp.GetValue(), false), nil
	case *fsmpb.Command_ReleaseLock:
		r := c.ReleaseLock
		return f.release(r.GetName(), r.GetClientId(), r.GetFencingToken()), nil
	case *fsmpb.Command_Resign:
		r := c.Resign
		return f.release(r.GetName(), r.GetClientId(), r.GetFencingToken()), nil
	case *fsmpb.Command_CancelWait:
		cw := c.CancelWait
		return f.cancelWait(cw.GetName(), cw.GetClientId()), nil
	case *fsmpb.Command_Proclaim:
		p := c.Proclaim
		return f.proclaim(p.GetName(), p.GetClientId(), p.GetFencingToken(), p.GetValue()), nil
	case *fsmpb.Command_LeaseGrant:
		lg := c.LeaseGrant
		return f.leaseGrant(lg.GetClientId(), lg.GetTtlMs(), lg.GetNowUnixNano()), nil
	case *fsmpb.Command_LeaseKeepAlive:
		ka := c.LeaseKeepAlive
		return f.leaseKeepAlive(ka.GetClientId(), ka.GetNowUnixNano()), nil
	case *fsmpb.Command_LeaseRevoke:
		return f.leaseRevoke(c.LeaseRevoke.GetClientId()), nil
	case *fsmpb.Command_LeaseExpire:
		le := c.LeaseExpire
		return f.leaseExpire(le.GetClientId(), le.GetNowUnixNano()), nil
	default:
		return nil, fmt.Errorf("fsm.Apply: unknown command type %T", c)
	}
}

// result builds a CommandResult stamped with the current revision.
func (f *FSM) result(o fsmpb.Outcome) *fsmpb.CommandResult {
	return &fsmpb.CommandResult{Outcome: o, Revision: f.revision}
}

// resultKey is result with the affected key set.
func (f *FSM) resultKey(o fsmpb.Outcome, key string) *fsmpb.CommandResult {
	r := f.result(o)
	r.LockKey = key
	return r
}

// emit publishes an ownership-change event, if a Notifier is configured.
func (f *FSM) emit(key, holder string, token uint64, value []byte) {
	if f.notifier == nil {
		return
	}
	f.notifier.Notify(Event{Key: key, Holder: holder, Token: token, Value: value, Revision: f.revision})
}

// leaseDeadline returns the unix-nano deadline for a lease created/renewed at now
// with the given millisecond TTL.
func leaseDeadline(now, ttlMS int64) int64 {
	return now + ttlMS*int64(time.Millisecond)
}

// StatusQuery asks for the holder/queue state of one lock or election key.
type StatusQuery struct {
	// Name is the lock/election key.
	Name string
}

// Status is the answer to a StatusQuery. For an election, Holder is the leader
// and Value is the published payload.
type Status struct {
	// Held reports whether the key currently has a holder/leader.
	Held bool
	// Holder is the holder/leader's client id; "" when not held.
	Holder string
	// Token is the holder's fencing token; 0 when not held.
	Token uint64
	// Value is the election leader's published value; nil for plain locks.
	Value []byte
	// QueueDepth is the number of waiters behind the holder.
	QueueDepth uint32
	// Revision is the FSM logical clock at which this was read.
	Revision uint64
}

// LeaseQuery asks for the state of one client's lease.
type LeaseQuery struct {
	// ClientID is the lease owner.
	ClientID string
}

// DueLeasesQuery asks for the client ids of every lease due to expire at or
// before NowUnixNano. The shard leader's expiry ticker uses it to find leases to
// expire; the FSM re-checks each deadline when the LeaseExpire is applied, so a
// slightly stale read here is harmless.
type DueLeasesQuery struct {
	// NowUnixNano is the leader's clock; a lease is due when its deadline <= this.
	NowUnixNano int64
}

// LeaseInfo is the answer to a LeaseQuery. The caller computes remaining TTL from
// ExpireAtUnixNano against its own clock.
type LeaseInfo struct {
	// Exists reports whether the lease is present in this shard.
	Exists bool
	// TTLMillis is the lease duration in milliseconds.
	TTLMillis int64
	// ExpireAtUnixNano is the absolute deadline the lease expires at.
	ExpireAtUnixNano int64
	// HeldKeys are the keys this client holds, sorted.
	HeldKeys []string
	// Revision is the FSM logical clock at which this was read.
	Revision uint64
}

// Query answers a read. q is a StatusQuery or LeaseQuery; the result is a Status
// or LeaseInfo respectively. An unknown query type returns an error.
func (f *FSM) Query(q any) (any, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	switch query := q.(type) {
	case StatusQuery:
		return f.status(query.Name), nil
	case LeaseQuery:
		return f.leaseInfo(query.ClientID), nil
	case DueLeasesQuery:
		return f.dueLeases(query.NowUnixNano), nil
	default:
		return nil, fmt.Errorf("fsm.Query: unknown query type %T", q)
	}
}

// dueLeases returns, in sorted order, the client ids of leases whose deadline is
// at or before now.
func (f *FSM) dueLeases(now int64) []string {
	var due []string
	for _, clientID := range sortedKeys(f.leases) {
		if f.leases[clientID].expireAt <= now {
			due = append(due, clientID)
		}
	}
	return due
}

// status reports the holder/queue state of a key.
func (f *FSM) status(name string) Status {
	lk := f.locks[name]
	if lk == nil || lk.holder == "" {
		return Status{Revision: f.revision}
	}
	return Status{
		Held:       true,
		Holder:     lk.holder,
		Token:      lk.token,
		Value:      lk.value,
		QueueDepth: uint32(len(lk.queue)),
		Revision:   f.revision,
	}
}

// leaseInfo reports the state of a client's lease.
func (f *FSM) leaseInfo(clientID string) LeaseInfo {
	ls := f.leases[clientID]
	if ls == nil {
		return LeaseInfo{Revision: f.revision}
	}
	return LeaseInfo{
		Exists:           true,
		TTLMillis:        ls.ttlMS,
		ExpireAtUnixNano: ls.expireAt,
		HeldKeys:         sortedSet(ls.held),
		Revision:         f.revision,
	}
}

// Snapshot returns a deterministic, point-in-time copy of the entire shard state,
// ready to be marshalled for SaveSnapshot. Locks and leases are sorted by key so
// the output is stable.
func (f *FSM) Snapshot() *fsmpb.Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snap := &fsmpb.Snapshot{Seq: f.seq, Revision: f.revision}
	for _, name := range sortedKeys(f.locks) {
		lk := f.locks[name]
		ls := &fsmpb.LockState{Name: name, Token: lk.token, Value: lk.value}
		if lk.holder != "" {
			ls.Holder = &fsmpb.Owner{ClientId: lk.holder}
		}
		for _, w := range lk.queue {
			ls.Queue = append(ls.Queue, &fsmpb.Waiter{ClientId: w.clientID, EnqueueSeq: w.seq, Value: w.value})
		}
		snap.Locks = append(snap.Locks, ls)
	}
	for _, clientID := range sortedKeys(f.leases) {
		l := f.leases[clientID]
		snap.Leases = append(snap.Leases, &fsmpb.LeaseState{
			ClientId:         clientID,
			TtlMs:            l.ttlMS,
			ExpireAtUnixNano: l.expireAt,
			HeldKeys:         sortedSet(l.held),
		})
	}
	return snap
}

// Restore replaces all state with the contents of snap. The held/waiting indexes
// on each lease are rebuilt from the (authoritative) lock map, so they cannot
// drift from the holders and queues.
func (f *FSM) Restore(snap *fsmpb.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq = snap.GetSeq()
	f.revision = snap.GetRevision()
	f.locks = make(map[string]*lock, len(snap.GetLocks()))
	f.leases = make(map[string]*lease, len(snap.GetLeases()))

	for _, l := range snap.GetLeases() {
		f.leases[l.GetClientId()] = &lease{
			clientID: l.GetClientId(),
			ttlMS:    l.GetTtlMs(),
			expireAt: l.GetExpireAtUnixNano(),
			held:     map[string]struct{}{},
			waiting:  map[string]struct{}{},
		}
	}
	for _, l := range snap.GetLocks() {
		lk := &lock{name: l.GetName(), token: l.GetToken(), value: l.GetValue()}
		if l.GetHolder() != nil {
			lk.holder = l.GetHolder().GetClientId()
		}
		for _, w := range l.GetQueue() {
			lk.queue = append(lk.queue, &waiter{clientID: w.GetClientId(), seq: w.GetEnqueueSeq(), value: w.GetValue()})
		}
		f.locks[l.GetName()] = lk
	}
	// Rebuild lease->key indexes from the locks (order-independent: sets only).
	for _, lk := range f.locks {
		if lk.holder != "" {
			if l := f.leases[lk.holder]; l != nil {
				l.held[lk.name] = struct{}{}
			}
		}
		for _, w := range lk.queue {
			if l := f.leases[w.clientID]; l != nil {
				l.waiting[lk.name] = struct{}{}
			}
		}
	}
}

// sortedKeys returns the map keys in ascending order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// sortedSet returns the members of a set in ascending order.
func sortedSet(m map[string]struct{}) []string {
	return sortedKeys(m)
}
