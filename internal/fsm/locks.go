package fsm

import "github.com/johnsiilver/zuul/internal/fsm/fsmpb"

// acquire grants the named key to clientID if free, returns an idempotent grant if
// clientID already holds it, fails fast for a try-lock on a held key, and
// otherwise enqueues the client (FIFO). value is the election payload to publish
// on grant; it is nil for a plain lock. A client with no live lease in this shard
// is rejected (NO_LEASE), since nothing would ever release its hold.
func (f *FSM) acquire(name, clientID string, value []byte, try bool) *fsmpb.CommandResult {
	ls := f.leases[clientID]
	if ls == nil {
		return f.resultKey(fsmpb.Outcome_OUTCOME_NO_LEASE, name)
	}

	lk := f.locks[name]
	if lk == nil || lk.holder == "" {
		return f.grant(name, ls, value)
	}

	if lk.holder == clientID { // idempotent re-apply by the current holder
		r := f.resultKey(fsmpb.Outcome_OUTCOME_GRANTED, name)
		r.FencingToken = lk.token
		r.Value = lk.value
		return r
	}

	if try {
		r := f.resultKey(fsmpb.Outcome_OUTCOME_NOT_ACQUIRED, name)
		r.CurrentHolder = lk.holder
		return r
	}

	if _, queued := ls.waiting[name]; queued { // idempotent re-enqueue
		r := f.resultKey(fsmpb.Outcome_OUTCOME_QUEUED, name)
		r.QueuePosition = queuePos(lk, clientID)
		return r
	}

	f.seq++
	lk.queue = append(lk.queue, &waiter{clientID: clientID, seq: f.seq, value: value})
	ls.waiting[name] = struct{}{}
	r := f.resultKey(fsmpb.Outcome_OUTCOME_QUEUED, name)
	r.QueuePosition = uint32(len(lk.queue))
	return r
}

// grant gives a free key to the lease owner with a fresh fencing token.
func (f *FSM) grant(name string, ls *lease, value []byte) *fsmpb.CommandResult {
	lk := f.locks[name]
	if lk == nil {
		lk = &lock{name: name}
		f.locks[name] = lk
	}
	f.seq++
	lk.holder = ls.clientID
	lk.token = f.seq
	lk.value = value
	ls.held[name] = struct{}{}
	delete(ls.waiting, name)

	f.emit(name, lk.holder, lk.token, lk.value)

	r := f.resultKey(fsmpb.Outcome_OUTCOME_GRANTED, name)
	r.FencingToken = lk.token
	r.Value = lk.value
	return r
}

// release verifies holder + fencing token, releases the key, and promotes the
// head of its FIFO queue (or removes the key if no one is waiting). It backs both
// Unlock and Resign — releasing a lock and resigning leadership are the same
// operation on the shared primitive.
func (f *FSM) release(name, clientID string, token uint64) *fsmpb.CommandResult {
	lk := f.locks[name]
	if lk == nil || lk.holder == "" || lk.holder != clientID {
		return f.resultKey(fsmpb.Outcome_OUTCOME_NOT_HOLDER, name)
	}
	if lk.token != token {
		return f.resultKey(fsmpb.Outcome_OUTCOME_STALE_TOKEN, name)
	}

	if ls := f.leases[clientID]; ls != nil {
		delete(ls.held, name)
	}
	f.promote(lk)
	return f.resultKey(fsmpb.Outcome_OUTCOME_RELEASED, name)
}

// promote hands a released key to the next viable waiter (assigning a new fencing
// token and publishing the promotion), or removes the key entirely when no waiter
// remains. Waiters whose lease has since vanished are skipped — though the lease
// drop path dequeues those, so it should not occur.
func (f *FSM) promote(lk *lock) {
	for len(lk.queue) > 0 {
		w := lk.queue[0]
		lk.queue = lk.queue[1:]
		ls := f.leases[w.clientID]
		if ls == nil {
			continue
		}
		f.seq++
		lk.holder = w.clientID
		lk.token = f.seq
		lk.value = w.value
		delete(ls.waiting, lk.name)
		ls.held[lk.name] = struct{}{}
		f.emit(lk.name, lk.holder, lk.token, lk.value)
		return
	}
	delete(f.locks, lk.name)
	f.emit(lk.name, "", 0, nil)
}

// cancelWait removes a client from a key's FIFO queue without acquiring it (the
// server proposes it when a bounded-wait acquire times out). It never touches the
// holder, so the key is never removed here. RELEASED means the waiter was
// dequeued; NOOP means it was not queued. The result is advisory — the server has
// already failed the wait.
func (f *FSM) cancelWait(name, clientID string) *fsmpb.CommandResult {
	lk := f.locks[name]
	if lk == nil {
		return f.resultKey(fsmpb.Outcome_OUTCOME_NOOP, name)
	}
	for i, w := range lk.queue {
		if w.clientID == clientID {
			lk.queue = append(lk.queue[:i], lk.queue[i+1:]...)
			if ls := f.leases[clientID]; ls != nil {
				delete(ls.waiting, name)
			}
			return f.resultKey(fsmpb.Outcome_OUTCOME_RELEASED, name)
		}
	}
	return f.resultKey(fsmpb.Outcome_OUTCOME_NOOP, name)
}

// proclaim updates the published value of an election leader without changing
// leadership. It requires the leader's matching fencing token.
func (f *FSM) proclaim(name, clientID string, token uint64, value []byte) *fsmpb.CommandResult {
	lk := f.locks[name]
	if lk == nil || lk.holder == "" || lk.holder != clientID {
		return f.resultKey(fsmpb.Outcome_OUTCOME_NOT_HOLDER, name)
	}
	if lk.token != token {
		return f.resultKey(fsmpb.Outcome_OUTCOME_STALE_TOKEN, name)
	}

	lk.value = value
	f.emit(name, lk.holder, lk.token, lk.value)

	r := f.resultKey(fsmpb.Outcome_OUTCOME_VALUE_UPDATED, name)
	r.FencingToken = lk.token
	r.Value = value
	return r
}

// queuePos returns the 1-based position of clientID in the key's FIFO queue, or 0
// if it is not queued.
func queuePos(lk *lock, clientID string) uint32 {
	for i, w := range lk.queue {
		if w.clientID == clientID {
			return uint32(i + 1)
		}
	}
	return 0
}
