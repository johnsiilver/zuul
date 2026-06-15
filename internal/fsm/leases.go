package fsm

import "github.com/johnsiilver/zuul/internal/fsm/fsmpb"

// leaseGrant creates a client's lease, or refreshes it if it already exists. The
// deadline is now + ttl, where now is the leader's stamped clock.
func (f *FSM) leaseGrant(clientID string, ttlMS, now int64) *fsmpb.CommandResult {
	ls := f.leases[clientID]
	if ls == nil {
		ls = &lease{clientID: clientID, held: map[string]struct{}{}, waiting: map[string]struct{}{}}
		f.leases[clientID] = ls
	}
	ls.ttlMS = ttlMS
	ls.expireAt = leaseDeadline(now, ttlMS)

	r := f.result(fsmpb.Outcome_OUTCOME_LEASE_GRANTED)
	r.LeaseDeadlineUnixNano = ls.expireAt
	return r
}

// leaseKeepAlive extends an existing lease to now + its stored ttl. A missing
// lease yields NOT_FOUND.
func (f *FSM) leaseKeepAlive(clientID string, now int64) *fsmpb.CommandResult {
	ls := f.leases[clientID]
	if ls == nil {
		return f.result(fsmpb.Outcome_OUTCOME_NOT_FOUND)
	}
	ls.expireAt = leaseDeadline(now, ls.ttlMS)

	r := f.result(fsmpb.Outcome_OUTCOME_LEASE_RENEWED)
	r.LeaseDeadlineUnixNano = ls.expireAt
	return r
}

// leaseRevoke removes a lease immediately and releases everything it holds.
func (f *FSM) leaseRevoke(clientID string) *fsmpb.CommandResult {
	ls := f.leases[clientID]
	if ls == nil {
		return f.result(fsmpb.Outcome_OUTCOME_NOT_FOUND)
	}
	f.dropLease(ls)
	return f.result(fsmpb.Outcome_OUTCOME_LEASE_REVOKED)
}

// leaseExpire expires a due lease. It re-checks the deadline against the leader's
// stamped now and is a NOOP if a keepalive has since pushed the deadline out (the
// keepalive committed after this expire was proposed), so a live lease is never
// wrongly reclaimed. A missing lease yields NOT_FOUND.
func (f *FSM) leaseExpire(clientID string, now int64) *fsmpb.CommandResult {
	ls := f.leases[clientID]
	if ls == nil {
		return f.result(fsmpb.Outcome_OUTCOME_NOT_FOUND)
	}
	if now < ls.expireAt {
		return f.result(fsmpb.Outcome_OUTCOME_NOOP)
	}
	f.dropLease(ls)
	return f.result(fsmpb.Outcome_OUTCOME_LEASE_EXPIRED)
}

// dropLease releases every lock the lease holds (promoting the next waiter on
// each), removes the client from every queue it waits on, and deletes the lease.
// Held keys are processed in sorted order so the fencing tokens handed to the
// promoted waiters are assigned identically on every replica.
func (f *FSM) dropLease(ls *lease) {
	for _, name := range sortedSet(ls.held) {
		lk := f.locks[name]
		if lk == nil || lk.holder != ls.clientID {
			continue
		}
		f.promote(lk)
	}
	for _, name := range sortedSet(ls.waiting) {
		lk := f.locks[name]
		if lk == nil {
			continue
		}
		for i, w := range lk.queue {
			if w.clientID == ls.clientID {
				lk.queue = append(lk.queue[:i], lk.queue[i+1:]...)
				break
			}
		}
	}
	delete(f.leases, ls.clientID)
}
