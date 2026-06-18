package client

import (
	"github.com/johnsiilver/zuul/context"
	"github.com/johnsiilver/zuul/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// Mutex is a distributed lock on a named key, bound to a Client's session. After a
// successful Lock/TryLock its Token is the fencing token to pass to whatever the
// lock guards (so a stale holder is rejected by the resource).
type Mutex struct {
	client *Client
	name   string
	token  uint64
	held   bool
}

// NewMutex returns a Mutex for the named key on this client's session.
func (c *Client) NewMutex(name string) *Mutex {
	return &Mutex{client: c, name: name}
}

// Lock blocks until the lock is held. If context.Deadline is set, this will return ErrNotAcquired if it expires.
// Once the Lock is held, a cancelled context will not cause the lock to be released; it must be explicitly unlocked.
func (m *Mutex) Lock(ctx context.Context) (err error) {
	defer func() {
		switch status.Code(err) {
		case codes.DeadlineExceeded:
			err = ErrNotAcquired
		}
		if errors.Is(err, context.DeadlineExceeded) {
			err = ErrNotAcquired
		}
	}()

	req := &zuulv1.LockRequest{Name: m.name, ClientId: m.client.clientID}
	if d, ok := ctx.Deadline(); ok {
		req.WaitDeadlineUnixNano = d.UnixNano()
	}
	resp, err := m.client.locker.Lock(ctx, req)
	if err != nil {
		return err
	}
	if !resp.GetAcquired() {
		return ErrNotAcquired
	}
	m.token = resp.GetFencingToken()
	m.held = true
	return nil
}

// TryLock acquires the lock only if it is immediately free, reporting whether it did.
func (m *Mutex) TryLock(ctx context.Context) (bool, error) {
	resp, err := m.client.locker.TryLock(ctx, &zuulv1.TryLockRequest{Name: m.name, ClientId: m.client.clientID})
	if err != nil {
		return false, err
	}
	if resp.GetAcquired() {
		m.token = resp.GetFencingToken()
		m.held = true
	}
	return resp.GetAcquired(), nil
}

// Unlock releases the lock. It is an error to call it when the lock is not held.
func (m *Mutex) Unlock(ctx context.Context) error {
	if !m.held {
		return ErrNotHeld
	}
	_, err := m.client.locker.Unlock(ctx, &zuulv1.UnlockRequest{LockKey: m.name, ClientId: m.client.clientID, FencingToken: m.token})
	if err != nil {
		return err
	}
	m.held = false
	return nil
}

// Token returns the fencing token from the most recent acquisition, or 0.
func (m *Mutex) Token() uint64 {
	return m.token
}

// Held reports whether this Mutex believes it currently holds the lock.
func (m *Mutex) Held() bool {
	return m.held
}
