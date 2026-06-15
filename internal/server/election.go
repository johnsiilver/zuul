package server

import (
	"fmt"
	"time"

	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/cmd"
	"github.com/johnsiilver/zuul/internal/fsm"
	"github.com/johnsiilver/zuul/internal/fsm/fsmpb"
	"github.com/johnsiilver/zuul/internal/watch"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// Election is the lock primitive plus a published value: campaigning is acquiring,
// the winner is the leader, runners-up wait in FIFO order, and resigning promotes
// the next. It routes, leases, proposes, and waits exactly like Locker.

// Campaign enters the named election and blocks until the caller leads it (or the
// optional wait deadline passes).
func (s *Server) Campaign(ctx context.Context, req *zuulv1.CampaignRequest) (*zuulv1.CampaignResponse, error) {
	if err := requireKeyAndClient(ctx, req.GetName(), req.GetClientId()); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, req.GetName(), authz.Write); err != nil {
		return nil, err
	}
	shardID := s.cfg.Router.Shard(req.GetName())
	if err := s.cfg.Sessions.EnsureLease(ctx, req.GetClientId(), shardID); err != nil {
		return nil, sessionErr(ctx, err)
	}

	sub := s.cfg.Hub.Subscribe(req.GetName())
	defer sub.Close()

	res, err := s.propose(ctx, shardID, cmd.Campaign(req.GetName(), req.GetClientId(), req.GetValue()))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "campaign", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_GRANTED:
		return &zuulv1.CampaignResponse{Leadership: true, LeaderKey: res.GetLockKey(), FencingToken: res.GetFencingToken(), Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_NO_LEASE:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNoLiveLease, fmt.Errorf("client %q has no live lease", req.GetClientId()))
	case fsmpb.Outcome_OUTCOME_QUEUED:
		return s.waitForLeadership(ctx, req, sub, shardID)
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("campaign: unexpected outcome %s", res.GetOutcome()))
	}
}

// waitForLeadership blocks a queued candidate until it is promoted to leader, or the
// wait deadline passes — in which case it withdraws and reports leadership=false.
func (s *Server) waitForLeadership(ctx context.Context, req *zuulv1.CampaignRequest, sub *watch.Sub, shardID uint64) (*zuulv1.CampaignResponse, error) {
	wait := ctx
	if d := req.GetWaitDeadlineUnixNano(); d > 0 {
		var cancel context.CancelFunc
		wait, cancel = context.WithDeadline(ctx, time.Unix(0, d))
		defer cancel()
	}
	for {
		e, err := sub.Next(wait)
		if err != nil {
			s.cancelWait(req.GetName(), req.GetClientId(), shardID)
			if req.GetWaitDeadlineUnixNano() > 0 && ctx.Err() == nil {
				return &zuulv1.CampaignResponse{Leadership: false, LeaderKey: req.GetName()}, nil
			}
			return nil, grpcErr(ctx, err)
		}
		if e.Holder == req.GetClientId() {
			return &zuulv1.CampaignResponse{Leadership: true, LeaderKey: e.Key, FencingToken: e.Token, Revision: e.Revision}, nil
		}
	}
}

// Proclaim updates the leader's published value without changing leadership.
func (s *Server) Proclaim(ctx context.Context, req *zuulv1.ProclaimRequest) (*zuulv1.ProclaimResponse, error) {
	if err := requireKeyAndClient(ctx, req.GetName(), req.GetClientId()); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, req.GetName(), authz.Write); err != nil {
		return nil, err
	}
	shardID := s.cfg.Router.Shard(req.GetName())
	res, err := s.propose(ctx, shardID, cmd.Proclaim(req.GetName(), req.GetClientId(), req.GetFencingToken(), req.GetValue()))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "proclaim", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_VALUE_UPDATED:
		return &zuulv1.ProclaimResponse{Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_STALE_TOKEN:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeStaleFencingToken, errors.New("stale fencing token"))
	case fsmpb.Outcome_OUTCOME_NOT_HOLDER:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLeader, errors.New("client is not the leader"))
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("proclaim: unexpected outcome %s", res.GetOutcome()))
	}
}

// Resign relinquishes leadership, promoting the next candidate.
func (s *Server) Resign(ctx context.Context, req *zuulv1.ResignRequest) (*zuulv1.ResignResponse, error) {
	if err := requireKeyAndClient(ctx, req.GetName(), req.GetClientId()); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, req.GetName(), authz.Write); err != nil {
		return nil, err
	}
	shardID := s.cfg.Router.Shard(req.GetName())
	res, err := s.propose(ctx, shardID, cmd.Resign(req.GetName(), req.GetClientId(), req.GetFencingToken()))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "resign", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_RELEASED:
		return &zuulv1.ResignResponse{Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_STALE_TOKEN:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeStaleFencingToken, errors.New("stale fencing token"))
	case fsmpb.Outcome_OUTCOME_NOT_HOLDER:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLeader, errors.New("client is not the leader"))
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("resign: unexpected outcome %s", res.GetOutcome()))
	}
}

// Leader returns the current leader and published value at the requested consistency.
func (s *Server) Leader(ctx context.Context, req *zuulv1.LeaderRequest) (*zuulv1.LeaderResponse, error) {
	if err := requireKeyPath(ctx, req.GetName()); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, req.GetName(), authz.Read); err != nil {
		return nil, err
	}
	shardID := s.cfg.Router.Shard(req.GetName())
	st, err := s.readStatus(ctx, shardID, req.GetName(), req.GetReadMode())
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	return &zuulv1.LeaderResponse{
		HasLeader:      st.Held,
		LeaderClientId: st.Holder,
		FencingToken:   st.Token,
		Value:          st.Value,
		Revision:       st.Revision,
	}, nil
}

// Observe streams leadership changes: the current leader first, then one event each
// time leadership or the published value changes. The stream ends when the client
// goes away.
func (s *Server) Observe(req *zuulv1.ObserveRequest, stream zuulv1.Election_ObserveServer) error {
	ctx := stream.Context()
	if err := requireKeyPath(ctx, req.GetName()); err != nil {
		return err
	}
	if err := s.authorize(ctx, req.GetName(), authz.Read); err != nil {
		return err
	}
	shardID := s.cfg.Router.Shard(req.GetName())

	// Subscribe before reading the current state so no change is missed.
	sub := s.cfg.Hub.Subscribe(req.GetName())
	defer sub.Close()

	st, err := s.readStatus(ctx, shardID, req.GetName(), zuulv1.ReadMode_READ_MODE_LINEARIZABLE)
	if err != nil {
		return grpcErr(ctx, err)
	}
	if err := stream.Send(statusToLeaderEvent(st)); err != nil {
		return err
	}

	for {
		e, err := sub.Next(ctx)
		if err != nil {
			return nil // context cancelled: the client disconnected — close the stream.
		}
		if err := stream.Send(eventToLeaderEvent(e)); err != nil {
			return err
		}
	}
}

// statusToLeaderEvent renders a Status read as a leadership event.
func statusToLeaderEvent(st fsm.Status) *zuulv1.LeaderEvent {
	return &zuulv1.LeaderEvent{
		HasLeader:      st.Held,
		LeaderClientId: st.Holder,
		FencingToken:   st.Token,
		Value:          st.Value,
		Revision:       st.Revision,
	}
}

// eventToLeaderEvent renders an ownership-change event as a leadership event.
func eventToLeaderEvent(e fsm.Event) *zuulv1.LeaderEvent {
	return &zuulv1.LeaderEvent{
		HasLeader:      e.Holder != "",
		LeaderClientId: e.Holder,
		FencingToken:   e.Token,
		Value:          e.Value,
		Revision:       e.Revision,
	}
}
