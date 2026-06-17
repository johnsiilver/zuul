// Package client is the Go client for Zuul. A Client holds one session (a lease the
// library keeps alive in the background) and hands out Mutex and Election helpers
// that ride on it. A single Client may be shared across goroutines for distinct
// keys; one key should be driven by one goroutine at a time.
//
// The client is resilient: give it several Endpoints and it fails over between
// them, and if the session's keepalive stream breaks (node death, network blip) it
// re-establishes the session — on whichever node it reaches — before the lease TTL
// expires, so held locks survive the failover.
package client

import (
	"fmt"
	"net"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/retry/exponential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/authz"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

var (
	// ErrNotAcquired is returned by Mutex.Lock when a bounded wait expired without the
	// lock being granted.
	ErrNotAcquired = errors.New("zuul: lock not acquired")

	// ErrNotHeld is returned when releasing or proclaiming on something not held.
	ErrNotHeld = errors.New("zuul: not held by this client")

	// errNoStream indicates the session stream is gone and must be re-established.
	errNoStream = errors.New("zuul: session stream not established")
)

// options hold New options.
type options struct {
	// ClientID is the session identity. Empty asks the server to assign one.
	ClientID string
	// User is the principal asserted for authorization when the server runs no
	// authenticating method that exposes one. Sent as the zuul-user metadata header
	// on every request. When the server DOES authenticate, a non-empty User must
	// match the authenticated identity or requests are rejected.
	User string
	// TTL is the requested lease duration. Default 30s.
	TTL time.Duration
	// AuthToken is a static bearer token sent with every request, for servers
	// running token authentication (zuuld --auth-tokens-file).
	AuthToken string
	// TokenSource supplies a fresh bearer token per request, for rotating
	// credentials (OIDC). Takes precedence over AuthToken.
	TokenSource func(ctx context.Context) (string, error)
	// AzureMSI fetches tokens from Azure Managed Service Identity (IMDS) for
	// servers validating Entra ID tokens (zuuld --oidc-issuer). Used when
	// TokenSource is nil; takes precedence over AuthToken.
	AzureMSI *AzureMSI
	// Insecure opts into an unencrypted connection (development only). It is rejected
	// together with a bearer token, which must never travel in clear text.
	Insecure bool
	// Creds are the transport credentials (TLS) for the connection, set via
	// WithTransportCredentials. This is the one channel the client inspects to decide a
	// bearer token may be sent: a token requires non-insecure credentials here.
	Creds credentials.TransportCredentials
	// DialOptions are extra, non-credential gRPC dial options (interceptors, stats
	// handlers, etc.). Transport credentials must be set via WithTransportCredentials,
	// not here — the client cannot introspect an opaque DialOption to confirm it is
	// secure, so a token would be unprotected.
	DialOptions []grpc.DialOption
}

// defaults returns o with the non-zero defaults applied (lease TTL 30s).
func (o options) defaults() options {
	o.TTL = 30 * time.Second
	return o
}

// validate reports invalid option combinations (an insecure connection paired with a
// bearer token, which must never travel unencrypted).
func (o options) validate() error {
	if o.Insecure && (o.AuthToken != "" || o.TokenSource != nil || o.AzureMSI != nil) {
		return fmt.Errorf("insecure connection cannot be combined with a bearer token — tokens must not be sent over an unencrypted connection")
	}
	return nil
}

// tokenSource resolves the configured credential into a per-request token function,
// or nil when none is configured. Precedence: WithTokenSource, WithAzureMSI,
// WithAuthToken.
func (o options) tokenSource() (func(ctx context.Context) (string, error), error) {
	switch {
	case o.TokenSource != nil:
		return o.TokenSource, nil
	case o.AzureMSI != nil:
		return o.AzureMSI.tokenSource()
	case o.AuthToken != "":
		token := o.AuthToken
		return func(context.Context) (string, error) { return token, nil }, nil
	}
	return nil, nil
}

// dialOptions builds the final gRPC dial options for the given token source.
//
// A bearer credential must never travel over an unencrypted connection. Transport
// credentials are supplied via WithTransportCredentials — the one channel the client can
// introspect — so a token is allowed only when real (non-insecure) credentials are set;
// otherwise we refuse rather than leak the token. When there is no token to protect and no
// credentials were set, insecure credentials are added (the development default, or an
// explicit WithInsecure dev dial).
func (o options) dialOptions(tokenSource func(ctx context.Context) (string, error)) ([]grpc.DialOption, error) {
	secure := o.Creds != nil && !isInsecureCreds(o.Creds)
	switch {
	case tokenSource != nil && o.Insecure:
		return nil, fmt.Errorf("insecure connection cannot be combined with a bearer token — tokens must not be sent over an unencrypted connection")
	case tokenSource != nil && !secure:
		return nil, fmt.Errorf("a bearer token is configured but no transport credentials were provided — pass WithTransportCredentials with TLS credentials")
	}

	var dialOpts []grpc.DialOption
	switch {
	case o.Creds != nil:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(o.Creds))
	default:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, o.DialOptions...)
	// Outermost interceptor: wrap every RPC error so an unwrapped transport error never
	// reaches the caller (status.Code still works, the original is kept in the chain).
	dialOpts = append(dialOpts,
		grpc.WithChainUnaryInterceptor(errors.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(errors.StreamClientInterceptor()))
	if tokenSource != nil {
		dialOpts = append(dialOpts,
			grpc.WithChainUnaryInterceptor(tokenUnary(tokenSource)),
			grpc.WithChainStreamInterceptor(tokenStream(tokenSource)))
	}
	if o.User != "" {
		dialOpts = append(dialOpts,
			grpc.WithChainUnaryInterceptor(userUnary(o.User)),
			grpc.WithChainStreamInterceptor(userStream(o.User)))
	}
	return dialOpts, nil
}

// Option configures New. Options are applied in order, so later options override earlier ones.
// All options are optional; zero values ask for defaults.
type Option func(options) (options, error)

// WithClientID sets the session identity. By default, the server assigns a random ID.
func WithClientID(id string) Option {
	return func(o options) (options, error) {
		o.ClientID = id
		return o, nil
	}
}

// WithUser sets the principal asserted for authorization, sent as the zuul-user
// metadata header. Use it when the server runs no authenticating method that exposes
// a user (e.g. an insecure/dev deployment): the header then names the principal the
// ACL is evaluated against. When the server authenticates the caller (mTLS, OIDC, or
// a token), a user set here must match the authenticated identity, or requests are
// rejected. The header is not a secret and may travel unencrypted.
func WithUser(user string) Option {
	return func(o options) (options, error) {
		o.User = user
		return o, nil
	}
}

// WithTTL sets the requested lease duration. Default 30s.
func WithTTL(d time.Duration) Option {
	return func(o options) (options, error) {
		o.TTL = d
		return o, nil
	}
}

// WithAuthToken sets a static bearer token sent with every request, for servers
// running token authentication (zuuld --auth-tokens-file).
func WithAuthToken(token string) Option {
	return func(o options) (options, error) {
		o.AuthToken = token
		return o, nil
	}
}

// WithTokenSource sets a function that supplies a fresh bearer token per request,
// for rotating credentials (OIDC). Takes precedence over WithAuthToken.
func WithTokenSource(source func(ctx context.Context) (string, error)) Option {
	return func(o options) (options, error) {
		o.TokenSource = source
		return o, nil
	}
}

// WithAzureMSI configures token fetching from Azure Managed Service Identity (IMDS)
// for servers validating Entra ID tokens (zuuld --oidc-issuer). Used when
// WithTokenSource is not set; takes precedence over WithAuthToken.
func WithAzureMSI(msi *AzureMSI) Option {
	return func(o options) (options, error) {
		o.AzureMSI = msi
		return o, nil
	}
}

// WithInsecure opts into an unencrypted connection (development only). It is rejected
// together with a bearer token, which must never travel in clear text.
func WithInsecure() Option {
	return func(o options) (options, error) {
		o.Insecure = true
		return o, nil
	}
}

// WithTransportCredentials sets the gRPC transport credentials (TLS) for the connection.
// A bearer token (WithAuthToken/WithTokenSource/WithAzureMSI) requires these — the token
// must travel encrypted — so set TLS here, not via WithDialOptions: this is the channel
// the client inspects to confirm the connection is secure before sending a token.
func WithTransportCredentials(tc credentials.TransportCredentials) Option {
	return func(o options) (options, error) {
		o.Creds = tc
		return o, nil
	}
}

// WithDialOptions adds extra, non-credential gRPC dial options (interceptors, stats
// handlers, etc.). Set transport credentials with WithTransportCredentials instead: the
// client cannot introspect an opaque DialOption to confirm it is secure, so a token
// configured alongside credentials passed here would be treated as unprotected.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o options) (options, error) {
		o.DialOptions = append(o.DialOptions, opts...)
		return o, nil
	}
}

// Client is a client for talking to the Zuul cluster.
type Client struct {
	conn     *grpc.ClientConn
	session  zuulv1.SessionClient
	locker   zuulv1.LockerClient
	election zuulv1.ElectionClient
	clientID string
	ttl      time.Duration
	sessCtx  context.Context // cancelled by Close; bounds every background loop
	cancel   context.CancelFunc
	boff     *exponential.Backoff
	done     chan struct{} // closed once when the session is permanently lost

	mu           sync.Mutex
	stream       zuulv1.Session_KeepAliveClient
	streamCancel context.CancelFunc // cancels the current stream's context
	deadlineNano int64              // best estimate of when the lease lapses
	lostErr      error              // set once before done is closed
}

// Endpoints is a list of Zuul node addresses. With more than one address, the client fails over
// between them when a node dies. We dial the first address.
type Endpoints []string

// New connects to a Zuul node (or, with several endpoints, a set of nodes for
// failover), opens a session, and keeps its lease alive in the background until
// Close. If the session stream later breaks, it is re-established automatically.
func New(ctx context.Context, endpoints Endpoints, opts ...Option) (*Client, error) {
	o := options{}.defaults()
	for _, opt := range opts {
		var err error
		o, err = opt(o)
		if err != nil {
			return nil, fmt.Errorf("zuul.New: %w", err)
		}
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("zuul.New: at least one endpoint is required")
	}
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("zuul.New: %w", err)
	}

	tokenSource, err := o.tokenSource()
	if err != nil {
		return nil, fmt.Errorf("zuul.New: %w", err)
	}
	dialOpts, err := o.dialOptions(tokenSource)
	if err != nil {
		return nil, fmt.Errorf("zuul.New: %w", err)
	}

	// We always dial through the manual resolver so the client can fail over between
	// every configured endpoint; pick_first lands on the first until it dies.
	rb := manual.NewBuilderWithScheme("zuul")
	state := resolver.State{}
	for _, e := range endpoints {
		// Pin the TLS ServerName to each endpoint's host. Without it the channel
		// authority (from the "zuul:///cluster" target) would be used, so a TLS dial
		// would verify the server certificate against "cluster" instead of its host.
		addr := resolver.Address{Addr: e}
		if host, _, err := net.SplitHostPort(e); err == nil {
			addr.ServerName = host
		}
		state.Addresses = append(state.Addresses, addr)
	}
	rb.InitialState(state)
	dialOpts = append(dialOpts, grpc.WithResolvers(rb))
	target := "zuul:///cluster"

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("zuul.New: %w", err)
	}
	boff, err := exponential.New()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("zuul.New: %w", err)
	}

	// The session and its keepalive stream must outlive this call, so they run on a
	// background context that Close cancels.
	sessCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:     conn,
		session:  zuulv1.NewSessionClient(conn),
		locker:   zuulv1.NewLockerClient(conn),
		election: zuulv1.NewElectionClient(conn),
		clientID: o.ClientID,
		ttl:      o.TTL,
		sessCtx:  sessCtx,
		cancel:   cancel,
		boff:     boff,
		done:     make(chan struct{}),
	}

	// Establish the session, but bound the first round-trip by the caller's ctx so a
	// failed handshake (e.g. wrong credentials) returns promptly rather than hanging
	// on the background stream. The stream must outlive this call, so it runs on its own
	// context derived from sessCtx (cancellable so a later swap can unblock it).
	type established struct {
		stream zuulv1.Session_KeepAliveClient
		id     string
		err    error
	}
	streamCtx, streamCancel := context.WithCancel(sessCtx)
	ch := make(chan established, 1)
	context.Pool(ctx).Submit(ctx, func() {
		st, id, err := c.establish(streamCtx)
		ch <- established{stream: st, id: id, err: err}
	})
	select {
	case r := <-ch:
		if r.err != nil {
			streamCancel()
			cancel()
			_ = conn.Close()
			return nil, fmt.Errorf("zuul.New: establish session: %w", r.err)
		}
		c.clientID = r.id
		c.setStream(r.stream, streamCancel)
		c.renewed()
	case <-ctx.Done():
		streamCancel()
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("zuul.New: establish session: %w", ctx.Err())
	}

	// The keepalive is a recurring background task tied to the session's lifetime.
	if err := context.Tasks(sessCtx).Run(sessCtx, "session-heartbeat", c.heartbeat, boff); err != nil {
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("zuul.New: start heartbeat: %w", err)
	}
	return c, nil
}

// isInsecureCreds reports whether tc is the insecure (no-op) transport credential, which
// must not carry a bearer token. The insecure credential reports SecurityProtocol
// "insecure" in its ProtocolInfo.
func isInsecureCreds(tc credentials.TransportCredentials) bool {
	return tc.Info().SecurityProtocol == "insecure"
}

// establish opens a KeepAlive stream and performs the first request/response,
// claiming (or being assigned) the session identity.
func (c *Client) establish(ctx context.Context) (zuulv1.Session_KeepAliveClient, string, error) {
	stream, err := c.session.KeepAlive(ctx)
	if err != nil {
		return nil, "", err
	}
	if err := stream.Send(&zuulv1.KeepAliveRequest{ClientId: c.clientID, TtlMs: c.ttl.Milliseconds()}); err != nil {
		// Per-message Send/Recv errors bypass the stream interceptor (which only maps the
		// stream open), so classify them here rather than leak a raw transport error.
		return nil, "", errors.FromStatus(ctx, err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, "", errors.FromStatus(ctx, err)
	}
	return stream, resp.GetClientId(), nil
}

// setStream swaps in the active keepalive stream, closing and cancelling the previously
// installed one so a reconnect does not leak the old stream (and its goroutine blocked
// on Recv).
func (c *Client) setStream(s zuulv1.Session_KeepAliveClient, cancel context.CancelFunc) {
	c.mu.Lock()
	old := c.stream
	oldCancel := c.streamCancel
	c.stream = s
	c.streamCancel = cancel
	c.mu.Unlock()
	if old != nil {
		_ = old.CloseSend()
	}
	if oldCancel != nil {
		oldCancel()
	}
}

// renewed records a successful keepalive: the lease is good for another TTL.
func (c *Client) renewed() {
	c.mu.Lock()
	c.deadlineNano = time.Now().Add(c.ttl).UnixNano()
	c.mu.Unlock()
}

// leaseDeadline returns the current best estimate of when the lease lapses.
func (c *Client) leaseDeadline() time.Time {
	c.mu.Lock()
	d := c.deadlineNano
	c.mu.Unlock()
	return time.Unix(0, d)
}

// markLost records that the session was permanently lost and wakes Done. It is
// idempotent: only the first call sets the error and closes the channel.
func (c *Client) markLost(err error) {
	c.mu.Lock()
	first := c.lostErr == nil
	if first {
		c.lostErr = err
		close(c.done)
	}
	c.mu.Unlock()
	if first {
		context.Log(c.sessCtx).Error("zuul: session lost; lease lapsed and could not be re-established before its deadline", "err", err.Error())
	}
}

// pulseInterval is both the keepalive period and the per-pulse round-trip budget.
func (c *Client) pulseInterval() time.Duration {
	if interval := c.ttl / 3; interval > 0 {
		return interval
	}
	return time.Second
}

// Done returns a channel closed when the session is permanently lost — the lease
// lapsed and could not be re-established before its deadline. A clean Close does not
// close it. Use Err for the cause.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err returns the cause of a permanent session loss, or nil while the session is
// healthy (or was cleanly closed).
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lostErr
}

// ClientID returns the session's identity.
func (c *Client) ClientID() string {
	return c.clientID
}

// Close ends the session (releasing everything it held) and closes the connection.
func (c *Client) Close() error {
	c.cancel()
	c.mu.Lock()
	if c.stream != nil {
		_ = c.stream.CloseSend()
	}
	c.mu.Unlock()
	if err := c.conn.Close(); err != nil {
		return errors.E(c.sessCtx, errors.CatInternal, errors.TypeBackend, fmt.Errorf("zuul: closing connection: %w", err))
	}
	return nil
}

// heartbeat renews the lease every TTL/3. If a pulse fails (node death, network
// blip), it re-establishes the session — possibly on a different endpoint — and
// continues, so the lease never lapses while the process is healthy.
// heartbeat is the session keepalive loop, run as a background task. It returns nil to
// stop (no restart): on ctx cancellation (clean Close) or on permanent lease loss, after
// which restarting cannot help. Resilience within a live lease is handled internally by
// reestablish, so it never returns a (restart-triggering) error.
func (c *Client) heartbeat(ctx context.Context) error {
	tick := time.NewTicker(c.pulseInterval())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if err := c.pulse(ctx); err != nil {
				if !c.reestablish(ctx) {
					// The lease has lapsed beyond recovery (or ctx ended); surface it.
					if ctx.Err() == nil {
						c.markLost(err)
					}
					return nil
				}
				continue
			}
			c.renewed()
		}
	}
}

// pulse sends one keepalive on the current stream, bounded by the pulse interval so a
// stalled (half-open) connection cannot park the heartbeat forever and silently let the
// lease lapse. A timeout returns an error, triggering reestablish.
func (c *Client) pulse(ctx context.Context) error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return errors.E(ctx, errors.CatUnavailable, errors.TypeBackend, errNoStream)
	}
	res := make(chan error, 1)
	context.Pool(ctx).Submit(ctx, func() {
		if err := stream.Send(&zuulv1.KeepAliveRequest{}); err != nil {
			res <- err
			return
		}
		_, err := stream.Recv()
		res <- err
	})
	tctx, cancel := context.WithTimeout(ctx, c.pulseInterval())
	defer cancel()
	select {
	case err := <-res:
		// Per-message Send/Recv errors bypass the stream interceptor (as in establish), so
		// classify them here; otherwise the raw transport error reaches the user via Err
		// when the session is lost. FromStatus preserves the original code and returns nil
		// for a nil (successful) err.
		return errors.FromStatus(ctx, err)
	case <-tctx.Done():
		// A half-open connection: the peer did not answer within the pulse budget. Surface a
		// classified Unavailable rather than a bare context.DeadlineExceeded.
		return errors.E(ctx, errors.CatUnavailable, errors.TypeBackend, tctx.Err())
	}
}

// reestablish retries opening a new session stream (with the same client id) with
// exponential backoff until it succeeds, the lease deadline passes, or ctx ends. A
// non-retryable failure (auth rejected) stops the retry immediately rather than
// hammering until the deadline. It reports success.
func (c *Client) reestablish(ctx context.Context) bool {
	deadline := c.leaseDeadline()
	if !time.Now().Before(deadline) {
		// The lease has already lapsed; reconnecting cannot save the locks it held.
		return false
	}
	rctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	err := c.boff.Retry(rctx, func(context.Context, exponential.Record) error {
		// The stream must outlive this attempt, so it is built on its own context
		// derived from the session ctx, not the attempt's; the server echoes the client
		// id, so identity is stable.
		streamCtx, streamCancel := context.WithCancel(c.sessCtx)
		stream, _, err := c.establish(streamCtx)
		if err != nil {
			streamCancel()
			if permanentReconnectErr(err) {
				return errors.Permanent(err)
			}
			return err
		}
		c.setStream(stream, streamCancel)
		c.renewed()
		return nil
	})
	return err == nil
}

// permanentReconnectErr reports whether err is a non-retryable reason to stop
// reconnecting: the server rejected the credentials, so retrying cannot help.
func permanentReconnectErr(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return true
	}
	return false
}

// tokenUnary attaches a bearer token from source to each unary request.
func tokenUnary(source func(ctx context.Context) (string, error)) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		token, err := source(ctx)
		if err != nil {
			return err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// tokenStream attaches a bearer token from source to each new stream.
func tokenStream(source func(ctx context.Context) (string, error)) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		token, err := source(ctx)
		if err != nil {
			return nil, err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// userUnary attaches the asserted principal as the zuul-user header on each request.
func userUnary(user string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, authz.UserHeader, user)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// userStream attaches the asserted principal as the zuul-user header on each stream.
func userStream(user string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, authz.UserHeader, user)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
