// Package server implements Zuul's client-facing gRPC services (Session and
// Locker). It routes each key to its shard, manages the client's per-shard leases
// through the session manager, proposes writes through the forward dispatcher
// (local-or-leader), reads through the local node, and parks blocking Lock calls
// on the per-node watch hub until they are promoted.
package server

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/authz"
	"github.com/johnsiilver/zuul/internal/auth/keypath"
	"github.com/johnsiilver/zuul/internal/cluster/router"
	"github.com/johnsiilver/zuul/internal/lock/session"
	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/otel/metrics"
	"github.com/johnsiilver/zuul/internal/raft/cmd"
	"github.com/johnsiilver/zuul/internal/raft/fsm"
	"github.com/johnsiilver/zuul/internal/raft/fsm/fsmpb"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// proposeTimeout bounds best-effort proposes (cancel-wait, session close) made on
// a detached context after the originating RPC has ended.
const proposeTimeout = 5 * time.Second

// Proposer commits an opaque (marshalled) command to a shard's leader and returns
// the marshalled result (the forward dispatcher).
type Proposer interface {
	Propose(ctx context.Context, shardID uint64, cmd []byte) ([]byte, error)
}

// Reader reads from the local node's replica of a shard.
type Reader interface {
	Read(ctx context.Context, shardID uint64, query any) (any, error)
	StaleRead(shardID uint64, query any) (any, error)
}

// Config configures a Server.
type Config struct {
	// Router resolves keys to shards. Required.
	Router *router.Router
	// Proposer commits writes (local-or-forward to leader). Required.
	Proposer Proposer
	// Reader serves reads from the local node. Required.
	Reader Reader
	// Sessions manages per-client, per-shard leases. Required.
	Sessions *session.Manager
	// Hub is the per-node watch hub the local FSM notifies. Required.
	Hub *watch.Hub
	// Now returns the current time in unix nanoseconds. Default time.Now().UnixNano.
	Now func() int64
	// DefaultTTL is the lease TTL used when a client requests none. Default 30s.
	DefaultTTL time.Duration
	// MinTTL and MaxTTL clamp a client-requested TTL. Defaults 1s and 5m.
	MinTTL, MaxTTL time.Duration
	// Authorizer gates key operations by the caller's mTLS identity. Default AllowAll.
	Authorizer authz.Authorizer
}

func (c *Config) validate() error {
	switch {
	case c.Router == nil:
		return fmt.Errorf("server.Config: Router is required")
	case c.Proposer == nil:
		return fmt.Errorf("server.Config: Proposer is required")
	case c.Reader == nil:
		return fmt.Errorf("server.Config: Reader is required")
	case c.Sessions == nil:
		return fmt.Errorf("server.Config: Sessions is required")
	case c.Hub == nil:
		return fmt.Errorf("server.Config: Hub is required")
	}
	if c.Now == nil {
		c.Now = func() int64 { return time.Now().UnixNano() }
	}
	if c.DefaultTTL == 0 {
		c.DefaultTTL = 30 * time.Second
	}
	if c.MinTTL == 0 {
		c.MinTTL = time.Second
	}
	if c.MaxTTL == 0 {
		c.MaxTTL = 5 * time.Minute
	}
	if c.Authorizer == nil {
		c.Authorizer = authz.AllowAll()
	}
	return nil
}

// Server serves the Session, Locker, and Election gRPC services for one node.
type Server struct {
	zuulv1.UnimplementedSessionServer
	zuulv1.UnimplementedLockerServer
	zuulv1.UnimplementedElectionServer

	cfg Config
}

// New returns a Server wired to the configured dependencies.
func New(cfg Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Server{cfg: cfg}, nil
}

// Register registers the Session, Locker, and Election services on reg.
func (s *Server) Register(reg grpc.ServiceRegistrar) {
	zuulv1.RegisterSessionServer(reg, s)
	zuulv1.RegisterLockerServer(reg, s)
	zuulv1.RegisterElectionServer(reg, s)
}

// outcomeLabels maps each FSM outcome to its metric label, precomputed so the hot
// path builds no strings.
var outcomeLabels = func() map[fsmpb.Outcome]string {
	m := make(map[fsmpb.Outcome]string, len(fsmpb.Outcome_name))
	for v, name := range fsmpb.Outcome_name {
		m[fsmpb.Outcome(v)] = strings.ToLower(strings.TrimPrefix(name, "OUTCOME_"))
	}
	return m
}()

// recordReq records a client request's outcome metric (op + short outcome label).
func recordReq(ctx context.Context, op string, res *fsmpb.CommandResult) {
	label, ok := outcomeLabels[res.GetOutcome()]
	if !ok {
		label = "unknown"
	}
	metrics.Request(ctx, op, label)
}

// propose marshals an FSM command, dispatches it to the shard leader, and decodes
// the result.
func (s *Server) propose(ctx context.Context, shardID uint64, c *fsmpb.Command) (*fsmpb.CommandResult, error) {
	b, err := c.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("server: marshal command: %w", err)
	}
	rb, err := s.cfg.Proposer.Propose(ctx, shardID, b)
	if err != nil {
		return nil, err
	}
	res := &fsmpb.CommandResult{}
	if err := res.UnmarshalVT(rb); err != nil {
		return nil, fmt.Errorf("server: unmarshal result: %w", err)
	}
	return res, nil
}

// KeepAlive runs one session: the first request opens (or claims) it, each later
// request renews every lease the client holds, and stream close revokes them.
func (s *Server) KeepAlive(stream zuulv1.Session_KeepAliveServer) error {
	ctx := stream.Context()
	var (
		clientID string
		ttlMS    int64
	)
	defer func() {
		if clientID != "" {
			metrics.SessionsActive(ctx, -1)
		}
	}()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var deadline int64
		if clientID == "" {
			clientID = req.GetClientId()
			reconnect := clientID != ""
			if clientID == "" {
				clientID = uuid.NewString()
			}
			ttlMS = s.clampTTL(req.GetTtlMs())
			s.cfg.Sessions.Open(clientID, ttlMS)
			metrics.SessionsActive(ctx, 1)
			if reconnect {
				// A client-supplied id may be a reconnect from another node: re-attach
				// any per-shard leases it already holds so keepalives keep renewing them.
				s.recoverSession(ctx, clientID)
			}
			deadline = s.cfg.Now() + ttlMS*int64(time.Millisecond)
		} else {
			deadline, err = s.cfg.Sessions.KeepAlive(ctx, clientID)
			if err != nil {
				return grpcErr(ctx, err)
			}
		}

		if err := stream.Send(&zuulv1.KeepAliveResponse{ClientId: clientID, TtlMs: ttlMS, DeadlineUnixNano: deadline}); err != nil {
			return err
		}
	}

	if clientID != "" {
		rctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
		defer cancel()
		s.cfg.Sessions.Close(rctx, clientID)
	}
	return nil
}

// recoverSession scans every shard for leases the client already holds and attaches
// them to its session on this node. Best effort: a shard that cannot be read right
// now is skipped (its lease will lapse only if the client truly holds nothing there,
// or be re-attached on the next reconnect).
func (s *Server) recoverSession(ctx context.Context, clientID string) {
	g := context.Pool(ctx).Group()
	for _, shardID := range s.cfg.Router.Shards().All() {
		shardID := shardID
		g.Go(ctx, func(ctx context.Context) error {
			v, err := s.cfg.Reader.Read(ctx, shardID, fsm.LeaseQuery{ClientID: clientID})
			if err != nil {
				return nil
			}
			if info, ok := v.(fsm.LeaseInfo); ok && info.Exists {
				_ = s.cfg.Sessions.Attach(clientID, shardID)
			}
			return nil
		})
	}
	_ = g.Wait(ctx)
}

// clampTTL converts a requested millisecond TTL into the configured bounds,
// substituting the default when none is requested.
func (s *Server) clampTTL(reqMS int64) int64 {
	ttl := time.Duration(reqMS) * time.Millisecond
	switch {
	case reqMS <= 0:
		ttl = s.cfg.DefaultTTL
	case ttl < s.cfg.MinTTL:
		ttl = s.cfg.MinTTL
	case ttl > s.cfg.MaxTTL:
		ttl = s.cfg.MaxTTL
	}
	return ttl.Milliseconds()
}

// Lock acquires a lock, blocking (up to the optional wait deadline) until held.
func (s *Server) Lock(ctx context.Context, req *zuulv1.LockRequest) (*zuulv1.LockResponse, error) {
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

	// Subscribe before proposing so a promotion event cannot be missed. This caller is a
	// contender, not an observer (Track false): it is reported from the FSM wait-queue.
	sub := s.cfg.Hub.Subscribe(watch.SubArgs{Key: req.GetName()})
	defer sub.Close()

	res, err := s.propose(ctx, shardID, cmd.Acquire(req.GetName(), req.GetClientId(), false))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "lock", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_GRANTED:
		return &zuulv1.LockResponse{Acquired: true, LockKey: res.GetLockKey(), FencingToken: res.GetFencingToken(), Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_NO_LEASE:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNoLiveLease, fmt.Errorf("client %q has no live lease", req.GetClientId()))
	case fsmpb.Outcome_OUTCOME_QUEUED:
		return s.waitForLock(ctx, req, sub, shardID)
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("lock: unexpected outcome %s", res.GetOutcome()))
	}
}

// now returns the configured clock in unix nanoseconds, defaulting to wall-clock time
// when unset (e.g. a Server built in tests without validate()).
func (s *Server) now() int64 {
	if s.cfg.Now == nil {
		return time.Now().UnixNano()
	}
	return s.cfg.Now()
}

// boundedWaitGrace is reserved at the tail of a bounded wait so the handler can confirm
// the outcome against the FSM and return a clean response while the RPC context — whose
// deadline the client derives from the same wait instant — is still alive. Without it the
// internal wait deadline and the RPC deadline fire together: the confirm read then runs on
// a cancelled context, and a grant committed in that window strands the lock or leadership
// until the client's lease lapses.
const boundedWaitGrace = 500 * time.Millisecond

// boundedWait derives the internal wait context for a queued caller from the requested
// wait-deadline instant d (unix nanos), pulled earlier by up to boundedWaitGrace — never
// more than half the remaining budget, so a real wait and a real grace window both remain.
// When d is zero the wait is unbounded and ctx is returned unchanged. The returned cancel
// must always be called.
func (s *Server) boundedWait(ctx context.Context, d int64) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	grace := time.Duration(d-s.now()) / 2
	if grace > boundedWaitGrace {
		grace = boundedWaitGrace
	}
	if grace < 0 {
		grace = 0
	}
	return context.WithDeadline(ctx, time.Unix(0, d).Add(-grace))
}

// waitForLock blocks a queued caller until it is promoted (its client id becomes
// the holder), or the wait deadline passes — in which case it dequeues itself and
// reports acquired=false. The promotion event is delivered by the local hub, since
// the local FSM applies every committed command.
func (s *Server) waitForLock(ctx context.Context, req *zuulv1.LockRequest, sub *watch.Sub, shardID uint64) (*zuulv1.LockResponse, error) {
	wait, cancel := s.boundedWait(ctx, req.GetWaitDeadlineUnixNano())
	defer cancel()
	for {
		e, err := sub.Next(wait)
		if err != nil {
			// Our promotion may have been coalesced away entirely, so the wait deadline
			// fires before any matching event arrives. cancelWait never releases a holder
			// (see fsm.cancelWait), so reporting Acquired:false here while we are in fact
			// the holder would strand the lock until our lease lapses. Confirm against the
			// FSM before giving up — the same backstop the per-event loop uses below. The
			// bounded wait reserves a grace window before the RPC deadline, so this read
			// runs on a live ctx rather than racing the client's cancellation.
			if held, herr := s.holdsLock(ctx, shardID, req.GetName(), req.GetClientId()); herr == nil && held != nil {
				return held, nil
			}
			s.cancelWait(req.GetName(), req.GetClientId(), shardID)
			// If the RPC's own context is still alive, only the derived wait deadline
			// fired → a bounded-wait timeout, not a fault.
			if req.GetWaitDeadlineUnixNano() > 0 && ctx.Err() == nil {
				return &zuulv1.LockResponse{Acquired: false, LockKey: req.GetName()}, nil
			}
			return nil, grpcErr(ctx, err)
		}
		if e.Holder == req.GetClientId() {
			return &zuulv1.LockResponse{Acquired: true, LockKey: e.Key, FencingToken: e.Token, Revision: e.Revision}, nil
		}
		// The hub coalesces: it keeps only the latest event, so our own promotion can be
		// overwritten by a later event for the same key before we read it. The event
		// holder not being us is therefore not proof we were not granted the lock — only
		// an authoritative read is. Confirm against the FSM before looping back to wait.
		if held, err := s.holdsLock(ctx, shardID, req.GetName(), req.GetClientId()); err == nil && held != nil {
			return held, nil
		}
	}
}

// holdsLock authoritatively checks whether clientID currently holds name on shardID
// via a linearizable read. It returns a granted LockResponse when the client holds the
// lock, nil when it does not, and an error if the read fails. It is the backstop for
// the hub's event coalescing: a promotion that was overwritten before the waiter read
// it is still observable here.
func (s *Server) holdsLock(ctx context.Context, shardID uint64, name, clientID string) (*zuulv1.LockResponse, error) {
	st, err := s.readStatus(ctx, shardID, name, zuulv1.ReadMode_READ_MODE_LINEARIZABLE)
	if err != nil {
		return nil, err
	}
	if st.Held && st.Holder == clientID {
		return &zuulv1.LockResponse{Acquired: true, LockKey: name, FencingToken: st.Token, Revision: st.Revision}, nil
	}
	return nil, nil
}

// cancelWait dequeues a timed-out waiter on a detached context (the RPC's is done).
// It retries: if the waiter were left queued, a later release could promote it to a
// lock its caller no longer expects to hold (until its lease lapses). On final
// failure it logs loudly — the residual risk window is bounded by the client's TTL.
func (s *Server) cancelWait(name, clientID string, shardID uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	defer cancel()
	var err error
	for attempt := 0; attempt < 3 && ctx.Err() == nil; attempt++ {
		if _, err = s.propose(ctx, shardID, cmd.Cancel(name, clientID)); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	context.Log(ctx).Error("cancel-wait failed; timed-out waiter remains queued until its lease expires", "key", name, "client", clientID, "err", err.Error())
}

// TryLock acquires a lock only if it is immediately free.
func (s *Server) TryLock(ctx context.Context, req *zuulv1.TryLockRequest) (*zuulv1.TryLockResponse, error) {
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
	res, err := s.propose(ctx, shardID, cmd.Acquire(req.GetName(), req.GetClientId(), true))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "try_lock", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_GRANTED:
		return &zuulv1.TryLockResponse{Acquired: true, LockKey: res.GetLockKey(), FencingToken: res.GetFencingToken(), Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_NOT_ACQUIRED:
		return &zuulv1.TryLockResponse{Acquired: false, LockKey: res.GetLockKey(), Revision: res.GetRevision(), CurrentHolder: res.GetCurrentHolder()}, nil
	case fsmpb.Outcome_OUTCOME_NO_LEASE:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNoLiveLease, fmt.Errorf("client %q has no live lease", req.GetClientId()))
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("try-lock: unexpected outcome %s", res.GetOutcome()))
	}
}

// Unlock releases a lock the caller holds, given its fencing token.
func (s *Server) Unlock(ctx context.Context, req *zuulv1.UnlockRequest) (*zuulv1.UnlockResponse, error) {
	if err := requireKeyAndClient(ctx, req.GetLockKey(), req.GetClientId()); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, req.GetLockKey(), authz.Write); err != nil {
		return nil, err
	}
	shardID := s.cfg.Router.Shard(req.GetLockKey())
	res, err := s.propose(ctx, shardID, cmd.Release(req.GetLockKey(), req.GetClientId(), req.GetFencingToken()))
	if err != nil {
		return nil, grpcErr(ctx, err)
	}
	recordReq(ctx, "unlock", res)
	switch res.GetOutcome() {
	case fsmpb.Outcome_OUTCOME_RELEASED:
		return &zuulv1.UnlockResponse{Revision: res.GetRevision()}, nil
	case fsmpb.Outcome_OUTCOME_STALE_TOKEN:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeStaleFencingToken, errors.New("stale fencing token"))
	case fsmpb.Outcome_OUTCOME_NOT_HOLDER:
		return nil, errors.E(ctx, errors.CatPrecondition, errors.TypeNotLockHolder, errors.New("client does not hold this lock"))
	default:
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeUnexpectedOutcome, fmt.Errorf("unlock: unexpected outcome %s", res.GetOutcome()))
	}
}

// Status reports the holder and queue depth of a lock at the requested consistency.
func (s *Server) Status(ctx context.Context, req *zuulv1.StatusRequest) (*zuulv1.StatusResponse, error) {
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
	return &zuulv1.StatusResponse{
		Held:           st.Held,
		HolderClientId: st.Holder,
		FencingToken:   st.Token,
		QueueDepth:     st.QueueDepth,
		Revision:       st.Revision,
	}, nil
}

// readStatus reads a key's holder/queue state from the local node at the requested
// consistency. It backs both Locker.Status and Election.Leader/Observe.
func (s *Server) readStatus(ctx context.Context, shardID uint64, name string, mode zuulv1.ReadMode) (fsm.Status, error) {
	q := fsm.StatusQuery{Name: name}
	var (
		v   any
		err error
	)
	if mode == zuulv1.ReadMode_READ_MODE_STALE {
		v, err = s.cfg.Reader.StaleRead(shardID, q)
	} else {
		v, err = s.cfg.Reader.Read(ctx, shardID, q)
	}
	if err != nil {
		return fsm.Status{}, err
	}
	st, ok := v.(fsm.Status)
	if !ok {
		return fsm.Status{}, fmt.Errorf("unexpected read result %T", v)
	}
	return st, nil
}

// authorize checks the caller's authenticated identity against the configured
// policy for key. Authorization is per-key, not per-lock-owner: any identity
// authorized to write a key may operate on locks under it (the request's clientID +
// fencing token select WHICH lock state is acted on, they are not credentials). Use
// distinct key prefixes per identity when clients must not touch each other's locks.
func (s *Server) authorize(ctx context.Context, key string, op authz.Op) error {
	identity, _ := context.IdentityFromContext(ctx)
	if err := s.cfg.Authorizer.Authorize(identity, key, op); err != nil {
		return errors.E(ctx, errors.CatPermission, errors.TypeUnauthorizedKey, fmt.Errorf("client %q is not authorized for key %q", identity, key))
	}
	return nil
}

// requireKeyAndClient validates the two fields every mutating call needs.
func requireKeyAndClient(ctx context.Context, key, clientID string) error {
	if clientID == "" {
		return errors.E(ctx, errors.CatRequest, errors.TypeMissingClientID, errors.New("client_id is required"))
	}
	return requireKeyPath(ctx, key)
}

// requireKeyPath validates that key is a canonical Zuul resource path
// (/<user>/<dir.../><name>). Every lock and election key must be a path.
func requireKeyPath(ctx context.Context, key string) error {
	if err := keypath.Validate(key); err != nil {
		return errors.E(ctx, errors.CatRequest, errors.TypeInvalidKeyPath, fmt.Errorf("invalid key path: %s", err))
	}
	return nil
}

// sessionErr maps a session-manager error to a classified Error.
func sessionErr(ctx context.Context, err error) error {
	if errors.Is(err, session.ErrNoSession) {
		return errors.E(ctx, errors.CatPrecondition, errors.TypeNoSession, errors.New("no open session for client; open a KeepAlive stream first"))
	}
	return grpcErr(ctx, err)
}

// grpcErr maps a propose/read transport error to a classified Error, passing through an
// error that already carries a gRPC status and otherwise reporting the node as unavailable.
func grpcErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return errors.E(ctx, errors.CatUnavailable, errors.TypeBackend, err)
}
