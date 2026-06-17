// Package node assembles a single Zuul node — the consensus host (lock shards +
// meta shard), the forward dispatcher, the session manager, and the gRPC services
// (Locker/Session/Election/Cluster + the internal Forwarder) — behind one type. The
// zuuld binary and the runnable examples both build on it, so the wiring lives in
// exactly one place.
package node

import (
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"sync/atomic"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/retry/exponential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/authz"
	"github.com/johnsiilver/zuul/internal/auth/oidcauth"
	"github.com/johnsiilver/zuul/internal/auth/zuultls"
	"github.com/johnsiilver/zuul/internal/cluster/router"
	"github.com/johnsiilver/zuul/internal/lock/session"
	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/otel/tracing"
	"github.com/johnsiilver/zuul/internal/raft/consensus"
	"github.com/johnsiilver/zuul/internal/raft/forward"
	"github.com/johnsiilver/zuul/internal/server"
	"github.com/johnsiilver/zuul/internal/ui"
)

// Config configures one node.
type Config struct {
	// ReplicaID is this node's replica id (non-zero). Required.
	ReplicaID uint64
	// RaftAddr is the dragonboat Raft transport address. Required.
	RaftAddr string
	// GRPCAddr is the client-facing / forwarding gRPC address. Required.
	GRPCAddr string
	// DataDir names the (in-memory) NodeHost directory. Required.
	DataDir string
	// Shards are the lock shard ids. Required.
	Shards []uint64
	// MetaShardID is the topology shard id, hosted alongside the lock shards.
	MetaShardID uint64
	// Members maps every initial replica id to its Raft address (bootstrap). Not
	// required when Join is set.
	Members map[uint64]string
	// Seed maps every initial replica id to its gRPC address (forwarding bootstrap).
	Seed map[uint64]string
	// Join starts the node in join mode (it must already have been AddNode'd).
	Join bool
	// MutualTLS enables mutual TLS on the Raft, forward, and client planes: every
	// caller, client or node, must present a CA-signed certificate.
	MutualTLS bool
	// ServerTLS enables server-authenticated (one-way) TLS for clients: the channel
	// is encrypted and the server verified, but clients need no certificates — they
	// authenticate with bearer tokens (Tokens / OIDC) instead, or not at all if no
	// auth is configured. Nodes still authenticate each other with their
	// certificates: the Raft plane stays mutual, and the forward plane only accepts
	// callers that presented a CA-verified certificate. Mutually exclusive with
	// MutualTLS; requires the same CAFile/CertFile/KeyFile.
	ServerTLS bool
	// CAFile, CertFile, KeyFile are PEM paths, required when MutualTLS or ServerTLS
	// is set.
	CAFile, CertFile, KeyFile string
	// PeerCAFile, when set, is the CA that signs node (peer) certificates. The
	// inter-node forward plane then requires a caller's certificate to chain to it,
	// so a client certificate issued by a different CA cannot reach it even under
	// MutualTLS. Without it the forward plane accepts any CA-verified certificate
	// (see Tokens) — recommended whenever clients and nodes share a trust root.
	// Revocation (CRL/OCSP) is not checked: a cert is trusted until it expires, so
	// rotate the peer CA / reissue to evict a compromised node.
	PeerCAFile string
	// PeerAllowedCNs, when non-empty, pins the forward plane to peer certificates
	// whose Common Name is in this set — a positive node allowlist on top of the
	// peer-CA chain check, for when the peer CA also signs non-node certificates.
	PeerAllowedCNs []string
	// Gossip enables NodeHostID addressing (dynamic-IP deployments). Requires
	// MutualTLS. In this mode Members targets are NodeHostID strings.
	Gossip bool
	// NodeHostID, GossipBind, GossipSeeds configure gossip (see consensus.Config).
	NodeHostID  uint64
	GossipBind  string
	GossipSeeds []string
	// Now returns the current time in unix nanoseconds; default time.Now().UnixNano.
	Now func() int64
	// ExpiryInterval is how often the leader sweeps for expired leases; default 1s.
	ExpiryInterval time.Duration
	// MaxRecvBytes caps an inbound gRPC message — including a published election
	// value (Campaign/Proclaim) — on both the client and forward planes; default
	// 1 MiB. Set it consistently on every node (a forwarded write must fit too).
	MaxRecvBytes int
	// MaxConcurrentStreams caps concurrent streams per connection; default 4096.
	MaxConcurrentStreams uint32
	// RateLimitPerSec, if > 0, limits client requests per second (token bucket);
	// 0 disables rate limiting.
	RateLimitPerSec float64
	// RateBurst is the rate limiter's burst; default ceil(RateLimitPerSec)+1.
	RateBurst int
	// PerIdentityRateLimitPerSec, if > 0, additionally limits each authenticated
	// identity's requests per second (its own token bucket); 0 disables it.
	PerIdentityRateLimitPerSec float64
	// PerIdentityRateBurst is each identity bucket's burst; default rate+1.
	PerIdentityRateBurst int
	// Tokens enables bearer-token authentication on the client-facing services:
	// a map of token -> identity (see authz.LoadTokens). The health service and the
	// inter-node forward plane are exempt. Nil disables token auth.
	//
	// Requires MutualTLS or ServerTLS: the forward plane accepts raw replicated
	// commands and is exempt from token auth (peers authenticate with their
	// certificates), so on a plaintext listener any client could bypass token auth
	// by calling it directly — New refuses that combination. Note the forward
	// plane trusts ANY certificate signed by the CA; if clients also hold
	// CA-signed certificates and must not reach it, issue node certificates from
	// a separate CA.
	Tokens map[string]string
	// OIDCIssuer enables OIDC bearer-token authentication: tokens are validated
	// against this issuer (discovery + JWKS), e.g.
	// "https://login.microsoftonline.com/<tenant>/v2.0" for Azure Entra ID /
	// Managed Service Identity. Requires OIDCAudience and MutualTLS or ServerTLS.
	OIDCIssuer string
	// OIDCAudience is the audience tokens must carry. Required with OIDCIssuer.
	OIDCAudience string
	// OIDCIdentityClaim names the claim used as the authorization identity.
	// Default "sub"; Azure deployments often want "oid".
	OIDCIdentityClaim string
	// SnapshotEntries is how many Raft log entries trigger a snapshot (compaction);
	// 0 means the default (1000). The log is held in RAM, so this bounds LogDB memory.
	SnapshotEntries uint64
	// CompactionOverhead is how many entries are retained after compaction;
	// 0 means the default (100).
	CompactionOverhead uint64
	// Authorizer gates key operations by mTLS identity; nil means allow all.
	Authorizer authz.Authorizer
	// UIEnable serves the optional read-only browsing web UI on UIBind. Off by default.
	UIEnable bool
	// UIBind is the UI HTTP listener address (e.g. "127.0.0.1:9999"). Required when
	// UIEnable. The UI has no authentication of its own — bind it to localhost or front
	// it with TLS.
	UIBind string
	// UITLS serves the UI over the node's server TLS (reuses CAFile/CertFile/KeyFile).
	// When false (the default) the UI is plaintext.
	UITLS bool
}

// Node is an assembled, ready-to-serve Zuul node.
type Node struct {
	host           *consensus.Host
	disp           *forward.Dispatcher
	srv            *server.Server
	clusterSrv     *server.ClusterServer
	browse         *server.BrowseServer
	ui             *ui.Server // nil when the UI is disabled
	sessions       *session.Manager
	gs             *grpc.Server
	health         *health.Server
	now            func() int64
	expiryInterval time.Duration
	draining       atomic.Bool
	closing        atomic.Bool // guards shutdown so Close/Stop are idempotent in any order

	mu     sync.Mutex
	cancel context.CancelFunc // guarded by mu; stops Start's background tasks, called by Close/Stop
}

// bearerEnabled reports whether any bearer-token authentication is configured.
func (c Config) bearerEnabled() bool {
	return len(c.Tokens) > 0 || c.OIDCIssuer != ""
}

// hasClientIdentity reports whether the node can identify a client: a verified mTLS
// certificate (CN) or a bearer token. It is the single definition of "is there an
// identity source", used by both validation and the forward-plane warning.
func (c Config) hasClientIdentity() bool {
	return c.MutualTLS || c.bearerEnabled()
}

// Validate checks the auth/TLS configuration coupling. The cmd layers do structural
// checks (addresses, peers, gossip) and rely on this for the security-relevant
// rules, so they cannot drift apart.
func (c Config) Validate() error {
	switch {
	case c.MutualTLS && c.ServerTLS:
		return fmt.Errorf("node.Config: MutualTLS and ServerTLS are mutually exclusive — pick one")
	case (c.MutualTLS || c.ServerTLS) && (c.CAFile == "" || c.CertFile == "" || c.KeyFile == ""):
		return fmt.Errorf("node.Config: MutualTLS/ServerTLS require CAFile, CertFile, and KeyFile")
	case (c.OIDCIssuer == "") != (c.OIDCAudience == ""):
		return fmt.Errorf("node.Config: OIDCIssuer and OIDCAudience must be set together")
	case c.bearerEnabled() && !c.MutualTLS && !c.ServerTLS:
		return fmt.Errorf("node.Config: bearer-token auth (Tokens/OIDC) requires MutualTLS or ServerTLS — on a plaintext listener the inter-node forward plane (exempt from token auth) lets any client bypass authentication")
	case c.UIEnable && c.UIBind == "":
		return fmt.Errorf("node.Config: UIEnable requires UIBind")
	case c.UITLS && c.CertFile == "":
		return fmt.Errorf("node.Config: UITLS requires the server TLS certificate (CAFile/CertFile/KeyFile)")
	}
	// A restrictive Authorizer without an authenticating method is permitted: the
	// caller supplies its principal via the unauthenticated zuul-user header. That is
	// advisory (clients can assert any identity), so New logs a warning rather than
	// failing — see [New].
	return nil
}

// New boots the consensus host and assembles every layer, returning a node ready to
// Serve. It does not begin serving or recording itself; call Serve and Start.
func New(ctx context.Context, cfg Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// A restrictive Authorizer with no authenticating method still works — clients
	// name their principal with the zuul-user header — but that principal is
	// client-asserted and therefore advisory. Warn so an operator who meant to lock
	// the cluster down notices the missing mTLS/token/OIDC.
	if authz.NeedsIdentity(cfg.Authorizer) && !cfg.hasClientIdentity() {
		context.Log(ctx).Warn("ACL is configured without an authenticating method (MutualTLS/Tokens/OIDC); the caller principal comes from the unauthenticated zuul-user header and is advisory")
	}

	// Discover the OIDC issuer before booting anything heavier; it is the cheapest
	// thing to fail on.
	var oidcV *oidcauth.Verifier
	if cfg.OIDCIssuer != "" {
		var err error
		oidcV, err = oidcauth.New(ctx, oidcauth.Config{Issuer: cfg.OIDCIssuer, Audience: cfg.OIDCAudience, IdentityClaim: cfg.OIDCIdentityClaim})
		if err != nil {
			return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("node.New: %w", err))
		}
	}

	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixNano() }
	}
	interval := cfg.ExpiryInterval
	if interval <= 0 {
		interval = time.Second
	}

	r, err := router.New(cfg.Shards)
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("node.New: %w", err))
	}

	hub := watch.New()
	host, err := consensus.New(ctx, consensus.Config{
		ReplicaID:   cfg.ReplicaID,
		RaftAddr:    cfg.RaftAddr,
		GRPCAddr:    cfg.GRPCAddr,
		DataDir:     cfg.DataDir,
		Shards:      cfg.Shards,
		MetaShardID: cfg.MetaShardID,
		Members:     cfg.Members,
		Join:        cfg.Join,
		MutualTLS:   cfg.MutualTLS || cfg.ServerTLS, // Raft is node-to-node; nodes always hold certs
		CAFile:      cfg.CAFile,
		CertFile:    cfg.CertFile,
		KeyFile:     cfg.KeyFile,
		Gossip:      cfg.Gossip,
		NodeHostID:  cfg.NodeHostID,
		GossipBind:  cfg.GossipBind,
		GossipSeeds: cfg.GossipSeeds,

		SnapshotEntries:    cfg.SnapshotEntries,
		CompactionOverhead: cfg.CompactionOverhead,

		Notifier: hub,
	})
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConsensus, fmt.Errorf("node.New: consensus host: %w", err))
	}

	serverOpts, dialOpts, err := tlsOptions(cfg)
	if err != nil {
		host.Close()
		return nil, errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("node.New: tls: %w", err))
	}

	// peerCA / peerCNs distinguish node certificates from client certificates on the
	// forward plane. Empty CNs are dropped so they can never match a cert without one.
	var peerCA *x509.CertPool
	if cfg.PeerCAFile != "" {
		peerCA, err = zuultls.LoadCAPool(cfg.PeerCAFile)
		if err != nil {
			host.Close()
			return nil, errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("node.New: peer CA: %w", err))
		}
	}
	var peerCNs map[string]struct{}
	for _, cn := range cfg.PeerAllowedCNs {
		if cn == "" {
			continue
		}
		if peerCNs == nil {
			peerCNs = make(map[string]struct{}, len(cfg.PeerAllowedCNs))
		}
		peerCNs[cn] = struct{}{}
	}
	// Warn when no effective pin exists (the built set, not the raw config, so a
	// config of only empty CNs still warns).
	if cfg.MutualTLS && peerCA == nil && len(peerCNs) == 0 && (cfg.bearerEnabled() || authz.NeedsIdentity(cfg.Authorizer)) {
		context.Log(ctx).Warn("MutualTLS without PeerCAFile/PeerAllowedCNs: the inter-node forward plane accepts any CA-verified certificate, so a client certificate from the same CA can bypass per-key authorization; set PeerCAFile or issue node certificates from a separate CA")
	}

	// Server hardening, auth, and tracing. Tracing is outermost so a span covers an
	// auth or rate-limit rejection; the auth gate runs before the limiters so the
	// per-identity bucket sees the authenticated identity; forwarded writes carry
	// the trace context to the leader node.
	serverOpts = append(serverOpts, baseHardening(cfg)...)
	gate := &authGate{
		bearer:    &bearerAuth{tokens: hashTokens(cfg.Tokens), oidc: oidcV},
		guardPeer: cfg.MutualTLS || cfg.ServerTLS,
		peerCA:    peerCA,
		peerCNs:   peerCNs,
	}
	// errors.*ServerInterceptor is outermost so every error a handler or inner
	// interceptor returns is mapped through the taxonomy to a gRPC status.
	unary := []grpc.UnaryServerInterceptor{errors.UnaryServerInterceptor(), tracing.ServerUnaryInterceptor(), gate.unary()}
	stream := []grpc.StreamServerInterceptor{errors.StreamServerInterceptor(), tracing.ServerStreamInterceptor(), gate.stream()}
	if lim := rateLimiter(cfg); lim != nil {
		unary = append(unary, rateLimitUnary(lim))
		stream = append(stream, rateLimitStream(lim))
	}
	if cfg.PerIdentityRateLimitPerSec > 0 {
		il := newIdentityLimiters(cfg.PerIdentityRateLimitPerSec, cfg.PerIdentityRateBurst)
		unary = append(unary, perIdentityUnary(il))
		stream = append(stream, perIdentityStream(il))
	}
	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
	dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(tracing.ClientUnaryInterceptor()))

	disp, err := forward.NewDispatcher(host, consensus.NewMetaResolver(host, cfg.Seed), dialOpts...)
	if err != nil {
		host.Close()
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("node.New: dispatcher: %w", err))
	}

	sessions := session.New(disp, now)
	srv, err := server.New(server.Config{Router: r, Proposer: disp, Reader: host, Sessions: sessions, Hub: hub, Now: now, Authorizer: cfg.Authorizer})
	if err != nil {
		disp.Close()
		host.Close()
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("node.New: server: %w", err))
	}
	clusterSrv := server.NewClusterServer(server.ClusterConfig{Host: host, Members: disp, Proposer: disp, MetaShardID: cfg.MetaShardID, Authorizer: cfg.Authorizer})

	// Read-only browse API: enumerate locks/elections (fan-out across local shards) and
	// aggregate observers cluster-wide (local hub + peer fan-out over the dispatcher).
	observers := server.NewObserverAggregator(hub, host, disp)
	browseSrv, err := server.NewBrowseServer(server.BrowseConfig{Router: r, Reader: host, Observers: observers, Authorizer: cfg.Authorizer})
	if err != nil {
		disp.Close()
		host.Close()
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("node.New: browse server: %w", err))
	}

	gs := grpc.NewServer(serverOpts...)
	forward.NewServer(host, hub).Register(gs)
	srv.Register(gs)
	clusterSrv.Register(gs)
	browseSrv.Register(gs)

	// Optional embedded web UI, calling the browse handlers in-process.
	var uiSrv *ui.Server
	if cfg.UIEnable {
		uiCfg := ui.Config{Bind: cfg.UIBind, Browser: browseSrv}
		if cfg.UITLS {
			tc, err := zuultls.ServerOneWayConfig(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
			if err != nil {
				disp.Close()
				host.Close()
				return nil, errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("node.New: ui tls: %w", err))
			}
			uiCfg.TLS = tc
		}
		uiSrv, err = ui.New(ctx, uiCfg)
		if err != nil {
			disp.Close()
			host.Close()
			return nil, errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("node.New: ui: %w", err))
		}
	}

	// Standard gRPC health service: NOT_SERVING until Start observes the shards
	// ready, so a k8s gRPC readiness probe gates traffic correctly during rollouts.
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(gs, hs)

	return &Node{host: host, disp: disp, srv: srv, clusterSrv: clusterSrv, browse: browseSrv, ui: uiSrv, sessions: sessions, gs: gs, health: hs, now: now, expiryInterval: interval}, nil
}

// Start records this node in the meta shard (best effort; retried by clients/admin
// if it fails), begins the leader-driven lease-expiry sweep, and starts the health
// watcher that flips the gRPC health status as readiness changes. The background
// tasks run until ctx is cancelled or Close is called, whichever comes first.
func (n *Node) Start(ctx context.Context) error {
	// Derive a node-lifetime context so Close stops the background tasks even when
	// the caller's ctx never cancels (e.g. zuuld passes context.Background()).
	// Without this, announceSelf keeps retrying WriteSelf against the closed
	// dispatcher forever after Close, and the watcher/expiry goroutines leak.
	tctx, cancel := context.WithCancel(ctx)
	n.setCancel(cancel)
	n.announceSelf(tctx)
	steps := []startStep{
		{name: "expiry sweep", run: func(c context.Context) error { return n.host.RunExpiry(c, n.expiryInterval, n.now) }},
		{name: "health watcher", run: n.runHealthWatcher},
		{name: "ui", run: n.startUI},
	}
	return runStartSteps(tctx, cancel, steps)
}

// startUI begins serving the embedded web UI, if it is enabled. It is a no-op when the
// UI is disabled.
func (n *Node) startUI(ctx context.Context) error {
	if n.ui == nil {
		return nil
	}
	return n.ui.Start(ctx)
}

// setCancel stores the cancel for Start's background tasks. Guarded so a concurrent
// Close (which reads it) cannot race the write.
func (n *Node) setCancel(cancel context.CancelFunc) {
	n.mu.Lock()
	n.cancel = cancel
	n.mu.Unlock()
}

// cancelTasks stops the background tasks Start began, if any. Safe to call before Start.
func (n *Node) cancelTasks() {
	n.mu.Lock()
	cancel := n.cancel
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// startStep is one named background task Start brings up.
type startStep struct {
	name string
	run  func(context.Context) error
}

// runStartSteps runs each step under ctx. On the first failure it calls cancel so no
// already-started background work (announceSelf, an earlier step) is left running on a
// context that would otherwise never be cancelled, then returns the wrapped error.
func runStartSteps(ctx context.Context, cancel context.CancelFunc, steps []startStep) error {
	for _, s := range steps {
		if err := s.run(ctx); err != nil {
			cancel()
			return errors.E(ctx, errors.CatInternal, errors.TypeBackend, fmt.Errorf("node.Start: %s: %w", s.name, err))
		}
	}
	return nil
}

// announceSelf records this node in the meta shard, retrying in the background until
// it succeeds (or ctx is cancelled). At cold start the meta-shard leader may not be
// reachable yet (peers are booting in parallel); a single attempt would leave this
// node permanently missing from the cluster topology even though it serves fine.
func (n *Node) announceSelf(ctx context.Context) {
	task := func(ctx context.Context) error {
		boff, err := exponential.New()
		if err != nil {
			return errors.E(ctx, errors.CatInternal, errors.TypeBackend, fmt.Errorf("node: announce backoff init: %w", err))
		}
		return boff.Retry(ctx, func(ctx context.Context, _ exponential.Record) error {
			if err := n.host.WriteSelf(ctx, n.disp); err != nil {
				return errors.E(ctx, errors.CatUnavailable, errors.TypeConsensus, fmt.Errorf("node: recording self in meta shard: %w", err))
			}
			return nil
		})
	}
	// One-shot out-of-band work: Tasks.Once runs it on the background manager and logs a
	// non-nil return, so a persistent failure is visible without a raw goroutine.
	if err := context.Tasks(ctx).Once(ctx, "announce-self", task); err != nil {
		context.Log(ctx).Error("node: scheduling announce-self failed", "err", err.Error())
	}
}

// runHealthWatcher keeps the gRPC health status in sync with the node's readiness
// (every hosted shard has a leader), checking once a second until ctx ends.
func (n *Node) runHealthWatcher(ctx context.Context) error {
	boff, err := exponential.New()
	if err != nil {
		return err
	}
	task := func(ctx context.Context) error {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
				status := healthpb.HealthCheckResponse_NOT_SERVING
				if n.host.Ready() && !n.draining.Load() {
					status = healthpb.HealthCheckResponse_SERVING
				}
				n.health.SetServingStatus("", status)
			}
		}
	}
	return context.Tasks(ctx).Run(ctx, "health-watcher", task, boff)
}

// Serve serves the gRPC services on lis until the node is closed (blocks).
func (n *Node) Serve(lis net.Listener) error {
	return n.gs.Serve(lis)
}

// Host exposes the underlying consensus host (topology/leadership introspection).
func (n *Node) Host() *consensus.Host {
	return n.host
}

// Server exposes the lock/session/election service handler registered on this
// node's gRPC surface. It is the in-process entry point for embedding and tests;
// callers invoking it directly bypass the gRPC interceptor chain (auth, rate
// limiting, tracing) that wraps the wire path.
func (n *Node) Server() *server.Server {
	return n.srv
}

// ClusterServer exposes the topology/membership service handler (see Server).
func (n *Node) ClusterServer() *server.ClusterServer {
	return n.clusterSrv
}

// Browse exposes the read-only browse service handler (see Server).
func (n *Node) Browse() *server.BrowseServer {
	return n.browse
}

// Sessions exposes the node's session (lease) manager; for embedding and tests.
func (n *Node) Sessions() *session.Manager {
	return n.sessions
}

// Drain transfers leadership of this node's shards to other members ahead of a
// graceful shutdown, so they do not stall in an election. It marks the node
// NOT_SERVING first so health-checked load balancers steer new traffic away.
// Best effort, bounded by ctx.
func (n *Node) Drain(ctx context.Context) {
	n.draining.Store(true) // keeps the health watcher from flipping back to SERVING
	n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	n.host.Drain(ctx)
}

// Close gracefully stops serving — waiting up to gracefulStopWindow for in-flight RPCs to
// finish, then forcing the rest closed so long-lived client streams cannot block shutdown
// — and shuts the consensus host down. Idempotent; safe to call after Stop.
func (n *Node) Close() {
	n.shutdown(true)
}

// Stop abruptly stops serving — open streams are cut without waiting — and shuts the
// node down. It models a hard failure (a crash or SIGKILL); graceful shutdown should
// prefer Drain followed by Close. Idempotent; safe to call before Close.
func (n *Node) Stop() {
	n.shutdown(false)
}

// gracefulStopWindow bounds how long a graceful shutdown waits for in-flight RPCs to drain
// before it forces open connections closed. The client keepalive and election-observe
// streams are long-lived and never end on their own, so an unbounded GracefulStop would
// block shutdown forever (the process would only exit on an external SIGKILL). Leadership
// has already moved on via Drain, so this only bounds the wait for client RPCs to finish.
const gracefulStopWindow = 5 * time.Second

// shutdown cancels the background tasks, stops the gRPC server (a bounded graceful stop
// when graceful is set, abruptly otherwise), then closes the dispatcher and host. The
// closing guard makes Close/Stop a no-op once shutdown has begun.
func (n *Node) shutdown(graceful bool) {
	if !n.closing.CompareAndSwap(false, true) {
		return
	}
	n.cancelTasks() // stop announceSelf/expiry/health-watcher before tearing down their deps
	if n.ui != nil {
		uctx, cancel := context.WithTimeout(context.Background(), gracefulStopWindow)
		_ = n.ui.Close(uctx)
		cancel()
	}
	switch {
	case graceful:
		n.gracefulStop(gracefulStopWindow)
	default:
		n.gs.Stop()
	}
	n.disp.Close()
	n.host.Close()
}

// gracefulStop waits up to window for in-flight RPCs to finish, then forces open
// connections closed so long-lived client streams cannot block shutdown forever. gRPC's
// GracefulStop has no timeout of its own; we run it on the worker pool, race it against a
// timer, and call Stop on expiry — Stop makes a pending GracefulStop return.
func (n *Node) gracefulStop(window time.Duration) {
	ctx := context.Background()
	done := make(chan struct{})
	context.Pool(ctx).Submit(ctx, func() {
		n.gs.GracefulStop()
		close(done)
	})
	t := time.NewTimer(window)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		n.gs.Stop() // force in-flight (long-lived client) streams closed
		<-done
	}
}

// tlsOptions builds the gRPC server and peer-dial credentials from cfg.
func tlsOptions(cfg Config) ([]grpc.ServerOption, []grpc.DialOption, error) {
	if !cfg.MutualTLS && !cfg.ServerTLS {
		// Plaintext: the forward dispatcher still needs explicit transport credentials
		// for peer dials. We must set them here rather than rely on the pool's
		// "insecure when no dial options" default, which is defeated by the tracing
		// interceptor node.New appends (a non-credential dial option).
		return nil, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	serverConf := zuultls.ServerConfig
	if cfg.ServerTLS {
		// One-way for clients; a presented certificate (peer nodes) is still verified.
		serverConf = zuultls.ServerOneWayConfig
	}
	sc, err := serverConf(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	cc, err := zuultls.ClientConfig(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(sc))},
		[]grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(cc))},
		nil
}
