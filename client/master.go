package client

import (
	"fmt"
	"net"
	"strconv"

	"github.com/gostdlib/base/concurrency/sync"

	"github.com/johnsiilver/zuul/context"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// Master is the resolved network location of an election's current leader, decoded
// from the Endpoint the leader published as its election value.
type Master struct {
	// Endpoint is the full decoded Endpoint the leader published (host, port, metadata).
	Endpoint *zuulv1.Endpoint
	// LeaderID is the leader's client id.
	LeaderID string
	// Token is the leadership fencing token.
	Token uint64
	// Revision is the FSM logical clock at which this was observed.
	Revision uint64
}

// Address returns the master as a dialable "host:port" string (host may be a DNS
// name or an IP literal). It is empty when the Endpoint is nil.
func (m Master) Address() string {
	if m.Endpoint == nil {
		return ""
	}
	return net.JoinHostPort(m.Endpoint.GetHost(), strconv.Itoa(int(m.Endpoint.GetPort())))
}

// MarshalEndpoint encodes ep for publishing as an Election value (the value passed to
// Election.Campaign or Election.Proclaim). It validates that ep is dialable: a
// non-empty host (a DNS name or IP literal) and a port in [1, 65535].
func MarshalEndpoint(ep *zuulv1.Endpoint) ([]byte, error) {
	if err := validateEndpoint(ep); err != nil {
		return nil, err
	}
	return ep.MarshalVT()
}

// validateEndpoint reports whether ep is dialable: a non-empty host (a DNS name or
// IP literal) and a port in [1, 65535].
func validateEndpoint(ep *zuulv1.Endpoint) error {
	switch {
	case ep == nil:
		return fmt.Errorf("zuul: endpoint is nil")
	case ep.GetHost() == "":
		return fmt.Errorf("zuul: endpoint has no host")
	case ep.GetPort() == 0 || ep.GetPort() > 65535:
		return fmt.Errorf("zuul: endpoint port %d is out of range [1, 65535]", ep.GetPort())
	}
	return nil
}

// decodeEndpoint decodes an election value into an Endpoint.
// It errors when value is not a valid, dialable Endpoint.
func decodeEndpoint(value []byte) (*zuulv1.Endpoint, error) {
	ep := &zuulv1.Endpoint{}
	if err := ep.UnmarshalVT(value); err != nil {
		return nil, fmt.Errorf("zuul: election value is not a valid Endpoint: %w", err)
	}
	if err := validateEndpoint(ep); err != nil {
		return nil, err
	}
	return ep, nil
}

// masterFromInfo resolves a LeaderInfo into a Master. deliver is false when the
// election is leaderless or its published value is not a valid Endpoint.
func masterFromInfo(info Leader) (Master, bool) {
	if !info.Has {
		return Master{}, false
	}
	ep, err := decodeEndpoint(info.Value)
	if err != nil {
		return Master{}, false
	}
	return Master{Endpoint: ep, LeaderID: info.ID, Token: info.Token, Revision: info.Revision}, true
}

// Follower tracks an election's current master in the background, updating as
// leadership or the published endpoint changes, until Close (or the ctx passed to
// FollowMaster ends).
type Follower struct {
	updates chan Master
	cancel  context.CancelFunc

	mu      sync.Mutex
	current Master
	ok      bool
}

// run consumes leadership events, keeping current up to date and publishing each
// newly resolved master to updates. Leaderless or undecodable events update current
// (ok=false) but are not delivered on updates.
func (f *Follower) run(ctx context.Context, events <-chan Leader) {
	defer close(f.updates)
	for {
		select {
		case <-ctx.Done():
			return
		case info, open := <-events:
			if !open {
				return
			}
			m, deliver := masterFromInfo(info)
			f.set(m, deliver)
			if !deliver {
				continue
			}
			select {
			case f.updates <- m:
			case <-ctx.Done():
				return
			}
		}
	}
}

// set replaces the follower's latest resolved view.
func (f *Follower) set(m Master, ok bool) {
	f.mu.Lock()
	f.current, f.ok = m, ok
	f.mu.Unlock()
}

// Current returns the latest resolved master. ok is false while the election is
// leaderless or its published value is not a valid Endpoint.
func (f *Follower) Current() (Master, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current, f.ok
}

// Updates delivers each newly resolved master as leadership or the published endpoint
// changes. Leaderless and undecodable states are reflected by Current but not sent
// here. The channel is closed when the follower stops (Close or ctx end).
func (f *Follower) Updates() <-chan Master {
	return f.updates
}

// Close stops following. After Close the Updates channel is closed.
func (f *Follower) Close() {
	f.cancel()
}
