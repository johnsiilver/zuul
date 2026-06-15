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
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/authz"
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
	// DialOptions are extra gRPC dial options (transport credentials, etc.). When a
	// bearer token is configured these MUST carry transport credentials.
	DialOptions []grpc.DialOption
}

func (o options) defaults() options {
	o.TTL = 30 * time.Second
	return o
}

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
// A bearer credential must never travel over an unencrypted connection. The caller
// supplies transport credentials via WithDialOptions; we refuse rather than leak the
// token, and we do NOT auto-add insecure credentials when a token is present, so a
// token dial without real transport security fails here instead of silently sending
// the token in clear text. When there is no token to protect and the caller gave no
// transport credentials of their own, insecure credentials are added (the development
// default, or an explicit WithInsecure dev dial).
func (o options) dialOptions(tokenSource func(ctx context.Context) (string, error)) ([]grpc.DialOption, error) {
	credsProvided := hasCreds(o.DialOptions)
	switch {
	case tokenSource != nil && o.Insecure:
		return nil, fmt.Errorf("insecure connection cannot be combined with a bearer token — tokens must not be sent over an unencrypted connection")
	case tokenSource != nil && !credsProvided:
		return nil, fmt.Errorf("a bearer token is configured but no transport credentials were provided — pass WithDialOptions with TLS credentials")
	}

	dialOpts := append([]grpc.DialOption(nil), o.DialOptions...)
	// Outermost interceptor: wrap every RPC error so an unwrapped transport error never
	// reaches the caller (status.Code still works, the original is kept in the chain).
	dialOpts = append(dialOpts,
		grpc.WithChainUnaryInterceptor(errors.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(errors.StreamClientInterceptor()))
	if tokenSource == nil && !credsProvided {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
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

// WithDialOptions adds extra gRPC dial options (transport credentials, etc.). When a
// bearer token is configured these MUST carry transport credentials.
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

	mu     sync.Mutex
	stream zuulv1.Session_KeepAliveClient
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
	}

	// Establish the session, but bound the first round-trip by the caller's ctx so a
	// failed handshake (e.g. wrong credentials) returns promptly rather than hanging
	// on the background stream.
	type established struct {
		stream zuulv1.Session_KeepAliveClient
		id     string
		err    error
	}
	ch := make(chan established, 1)
	context.Pool(ctx).Submit(ctx, func() {
		st, id, err := c.establish(sessCtx)
		ch <- established{stream: st, id: id, err: err}
	})
	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			_ = conn.Close()
			return nil, fmt.Errorf("zuul.New: establish session: %w", r.err)
		}
		c.clientID = r.id
		c.setStream(r.stream)
	case <-ctx.Done():
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("zuul.New: establish session: %w", ctx.Err())
	}

	context.Pool(sessCtx).Submit(sessCtx, func() { c.heartbeat(sessCtx) })
	return c, nil
}

// hasCreds reports whether the dial options already carry transport credentials.
func hasCreds(opts []grpc.DialOption) bool {
	// grpc.DialOption is opaque; callers that set credentials do so via
	// WithTransportCredentials, which we cannot introspect. We treat any explicit
	// options as possibly carrying credentials and only default to insecure when the
	// caller supplied none at all.
	return len(opts) > 0
}

// establish opens a KeepAlive stream and performs the first request/response,
// claiming (or being assigned) the session identity.
func (c *Client) establish(ctx context.Context) (zuulv1.Session_KeepAliveClient, string, error) {
	stream, err := c.session.KeepAlive(ctx)
	if err != nil {
		return nil, "", err
	}
	if err := stream.Send(&zuulv1.KeepAliveRequest{ClientId: c.clientID, TtlMs: c.ttl.Milliseconds()}); err != nil {
		return nil, "", err
	}
	resp, err := stream.Recv()
	if err != nil {
		return nil, "", err
	}
	return stream, resp.GetClientId(), nil
}

// setStream swaps in the active keepalive stream.
func (c *Client) setStream(s zuulv1.Session_KeepAliveClient) {
	c.mu.Lock()
	c.stream = s
	c.mu.Unlock()
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
	return c.conn.Close()
}

// heartbeat renews the lease every TTL/3. If a pulse fails (node death, network
// blip), it re-establishes the session — possibly on a different endpoint — and
// continues, so the lease never lapses while the process is healthy.
func (c *Client) heartbeat(ctx context.Context) {
	interval := c.ttl / 3
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.pulse(); err != nil {
				if !c.reestablish(ctx) {
					return
				}
			}
		}
	}
}

// pulse sends one keepalive on the current stream.
func (c *Client) pulse() error {
	c.mu.Lock()
	stream := c.stream
	c.mu.Unlock()
	if stream == nil {
		return errNoStream
	}
	if err := stream.Send(&zuulv1.KeepAliveRequest{}); err != nil {
		return err
	}
	_, err := stream.Recv()
	return err
}

// reestablish retries opening a new session stream (with the same client id) with
// exponential backoff until it succeeds or ctx ends. It reports success.
func (c *Client) reestablish(ctx context.Context) bool {
	err := c.boff.Retry(ctx, func(context.Context, exponential.Record) error {
		// The stream must outlive this attempt, so it is built on the session ctx,
		// not the attempt's; the server echoes the client id, so identity is stable.
		stream, _, err := c.establish(ctx)
		if err == nil {
			c.setStream(stream)
		}
		return err
	})
	return err == nil
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
