package client

import (
	"time"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// Leader describes the leader of an election at a point in time.
type Leader struct {
	// Has reports whether the election currently has a leader.
	Has bool
	// ID is the leader's client id; empty when leaderless.
	ID string
	// Token is the leadership fencing token; 0 when leaderless.
	Token uint64
	// Value is the leader's published payload.
	Value []byte
	// Revision is the FSM logical clock at which this was observed.
	Revision uint64
}

// Election is a leader-election contest on a named key, bound to a Client's session.
type Election struct {
	client  *Client
	name    string
	token   uint64
	leading bool
}

// NewElection returns an Election for the named contest on this client's session.
func (c *Client) NewElection(name string) *Election {
	return &Election{client: c, name: name}
}

// Campaign enters the election and blocks until this client leads it. With a
// non-zero wait it bounds the wait and returns ErrNotAcquired if it expires.
func (e *Election) Campaign(ctx context.Context, value []byte, wait time.Duration) error {
	req := &zuulv1.CampaignRequest{Name: e.name, ClientId: e.client.clientID, Value: value}
	if wait > 0 {
		req.WaitDeadlineUnixNano = time.Now().Add(wait).UnixNano()
	}
	resp, err := e.client.election.Campaign(ctx, req)
	if err != nil {
		return err
	}
	if !resp.GetLeadership() {
		return ErrNotAcquired
	}
	e.token = resp.GetFencingToken()
	e.leading = true
	return nil
}

// Proclaim updates the value published while leading. It is an error if not leading.
func (e *Election) Proclaim(ctx context.Context, value []byte) error {
	if !e.leading {
		return ErrNotHeld
	}
	_, err := e.client.election.Proclaim(ctx, &zuulv1.ProclaimRequest{Name: e.name, ClientId: e.client.clientID, FencingToken: e.token, Value: value})
	return err
}

// Resign relinquishes leadership, promoting the next candidate.
func (e *Election) Resign(ctx context.Context) error {
	if !e.leading {
		return ErrNotHeld
	}
	_, err := e.client.election.Resign(ctx, &zuulv1.ResignRequest{Name: e.name, ClientId: e.client.clientID, FencingToken: e.token})
	if err != nil {
		return err
	}
	e.leading = false
	return nil
}

// Leader returns the current leader and published value.
func (e *Election) Leader(ctx context.Context) (Leader, error) {
	resp, err := e.client.election.Leader(ctx, &zuulv1.LeaderRequest{Name: e.name})
	if err != nil {
		return Leader{}, err
	}
	return Leader{
		Has:      resp.GetHasLeader(),
		ID:       resp.GetLeaderClientId(),
		Token:    resp.GetFencingToken(),
		Value:    resp.GetValue(),
		Revision: resp.GetRevision(),
	}, nil
}

// Token returns the leadership fencing token from the last successful Campaign, or 0.
func (e *Election) Token() uint64 {
	return e.token
}

// Observe streams leadership changes until ctx is cancelled, delivering the current
// leader first and then each change. If the stream breaks (node death, network
// blip), it resumes automatically — the resumed stream re-reports the current leader,
// so observers may see a repeated event but never miss the latest state. The
// returned channel is closed when ctx ends or resumption is abandoned.
func (e *Election) Observe(ctx context.Context) (<-chan Leader, error) {
	stream, err := e.client.election.Observe(ctx, &zuulv1.ObserveRequest{Name: e.name})
	if err != nil {
		return nil, err
	}
	ch := make(chan Leader, 16)

	// The observe loop must also stop when the Client is closed — not only when the
	// caller's ctx ends — or the resume loop would retry a dead connection forever.
	octx, cancel := context.WithCancel(ctx)
	context.Pool(ctx).Submit(ctx, func() {
		select {
		case <-e.client.sessCtx.Done():
			cancel()
		case <-octx.Done():
		}
	})
	context.Pool(ctx).Submit(ctx, func() {
		defer close(ch)
		defer cancel()
		for {
			ev, err := stream.Recv()
			if err != nil {
				stream = e.reopenObserve(octx)
				if stream == nil {
					return
				}
				continue
			}
			info := Leader{
				Has:      ev.GetHasLeader(),
				ID:       ev.GetLeaderClientId(),
				Token:    ev.GetFencingToken(),
				Value:    ev.GetValue(),
				Revision: ev.GetRevision(),
			}
			select {
			case ch <- info:
			case <-octx.Done():
				return
			}
		}
	})
	return ch, nil
}

// Master resolves the election's current leader to a dialable address. ok is false
// when the election is currently leaderless; err is non-nil only when the leader's
// published value is not a valid Endpoint. It is a one-shot lookup — use FollowMaster
// to track the master across leadership changes.
func (e *Election) Master(ctx context.Context) (Master, bool, error) {
	info, err := e.Leader(ctx)
	if err != nil {
		return Master{}, false, err
	}
	if !info.Has {
		return Master{}, false, nil
	}
	ep, err := decodeEndpoint(info.Value)
	if err != nil {
		return Master{}, false, err
	}
	return Master{Endpoint: ep, LeaderID: info.ID, Token: info.Token, Revision: info.Revision}, true, nil
}

// FollowMaster starts tracking the election's current master. It builds on Observe,
// inheriting its automatic stream resumption, so the follower's view stays current
// across leadership changes until Close or ctx ends.
func (e *Election) FollowMaster(ctx context.Context) (*Follower, error) {
	octx, cancel := context.WithCancel(ctx)
	events, err := e.Observe(octx)
	if err != nil {
		cancel()
		return nil, err
	}
	f := &Follower{updates: make(chan Master, 16), cancel: cancel}
	context.Pool(ctx).Submit(ctx, func() { f.run(octx, events) })
	return f, nil
}

// reopenObserve retries opening the observe stream with exponential backoff until
// success or ctx ends (returning nil).
func (e *Election) reopenObserve(ctx context.Context) zuulv1.Election_ObserveClient {
	var stream zuulv1.Election_ObserveClient
	err := e.client.boff.Retry(ctx, func(context.Context, exponential.Record) error {
		var oerr error
		// Built on the observe ctx, not the attempt's: the stream must outlive the retry.
		stream, oerr = e.client.election.Observe(ctx, &zuulv1.ObserveRequest{Name: e.name})
		return oerr
	})
	if err != nil {
		return nil
	}
	return stream
}
