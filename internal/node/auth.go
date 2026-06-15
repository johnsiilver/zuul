package node

import (
	"crypto/x509"
	"fmt"
	"strings"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/forward/forwardpb"
	"github.com/johnsiilver/zuul/internal/oidcauth"
)

// bearerPrefix is the (case-insensitive) HTTP bearer auth scheme prefix.
const bearerPrefix = "bearer "

// Method prefixes are derived from the generated service descriptors so a proto
// rename propagates here automatically (rather than drifting from a hardcoded
// string). A full method is "/<service>/<rpc>".
var (
	healthMethodPrefix  = "/" + healthpb.Health_ServiceDesc.ServiceName + "/"
	forwardMethodPrefix = "/" + forwardpb.Forwarder_ServiceDesc.ServiceName + "/"
)

// methodClass is the authentication policy for a gRPC method.
type methodClass int

const (
	// classClient is a client-facing method: bearer auth applies when configured.
	classClient methodClass = iota
	// classPeer is the inter-node forward plane: callers must present a verified
	// peer certificate (chained to the peer CA when one is configured); never bearer.
	classPeer
	// classOpen is unauthenticated (the health service; probes carry no credentials).
	classOpen
)

// classify returns a method's authentication class. It is the single source of
// truth for per-method auth policy, so registering or renaming a service is a
// one-line change here rather than logic spread across interceptors. Default is
// classClient — a new service is authenticated, not exempt (fail closed).
func classify(fullMethod string) methodClass {
	switch {
	case strings.HasPrefix(fullMethod, healthMethodPrefix):
		return classOpen
	case strings.HasPrefix(fullMethod, forwardMethodPrefix):
		return classPeer
	default:
		return classClient
	}
}

// bearerAuth authenticates client-facing calls from a bearer token: a static token
// map (exact match) and/or OIDC JWT validation. Either source yields the identity
// attached to the context for authorization.
type bearerAuth struct {
	tokens map[string]string
	oidc   *oidcauth.Verifier
}

// enabled reports whether any bearer authentication is configured.
func (b *bearerAuth) enabled() bool {
	return len(b.tokens) > 0 || b.oidc != nil
}

// authenticate validates the bearer token in the request metadata and returns ctx
// carrying its identity, or an Unauthenticated error.
func (b *bearerAuth) authenticate(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.E(ctx, errors.CatUnauthenticated, errors.TypeMissingCredentials, fmt.Errorf("missing credentials"))
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil, errors.E(ctx, errors.CatUnauthenticated, errors.TypeMissingCredentials, fmt.Errorf("missing authorization token"))
	}
	token := strings.TrimSpace(vals[0])
	// The auth scheme is case-insensitive per RFC 7235; strip "Bearer " in any case.
	if len(token) >= len(bearerPrefix) && strings.EqualFold(token[:len(bearerPrefix)], bearerPrefix) {
		token = token[len(bearerPrefix):]
	}
	if identity, ok := b.tokens[token]; ok {
		return authz.WithIdentity(ctx, identity), nil
	}
	if b.oidc != nil {
		identity, err := b.oidc.Verify(ctx, token)
		if err == nil {
			return authz.WithIdentity(ctx, identity), nil
		}
	}
	return nil, errors.E(ctx, errors.CatUnauthenticated, errors.TypeInvalidToken, fmt.Errorf("invalid authorization token"))
}

// authGate enforces per-method authentication: bearer auth for client methods and
// peer-certificate identity for the inter-node forward plane.
type authGate struct {
	bearer *bearerAuth
	// guardPeer requires a verified peer certificate on the forward plane (set when
	// TLS is on, so cert-less token clients cannot reach it).
	guardPeer bool
	// peerCA, when non-nil, additionally requires the forward-plane peer certificate
	// to chain to it — the mechanism that distinguishes node certificates from
	// client certificates issued by a different CA.
	peerCA *x509.CertPool
	// peerCNs, when non-empty, pins forward-plane peer certificates to this set of
	// Common Names — a positive node allowlist on top of chain-to-CA.
	peerCNs map[string]struct{}
}

// check authenticates ctx for fullMethod, returning a context carrying the caller's
// identity (for client methods) or an error.
func (g *authGate) check(ctx context.Context, fullMethod string) (context.Context, error) {
	switch classify(fullMethod) {
	case classOpen:
		return ctx, nil
	case classPeer:
		if g.guardPeer {
			if err := g.verifyPeer(ctx); err != nil {
				return nil, err
			}
		}
		return ctx, nil
	default:
		if g.bearer.enabled() {
			authed, err := g.bearer.authenticate(ctx)
			if err != nil {
				return nil, err
			}
			ctx = authed
		}
		return reconcileUser(ctx)
	}
}

// reconcileUser resolves the zuul-user header against any authenticated identity.
// When the caller is authenticated (token/OIDC identity or mTLS certificate CN), a
// provided user must match it — a mismatch is rejected to prevent impersonation.
// When the caller is not authenticated, the header becomes the principal (advisory,
// for deployments with no authenticating method). Absent header, nothing changes.
func reconcileUser(ctx context.Context) (context.Context, error) {
	user := firstMetadata(ctx, authz.UserHeader)
	authID, ok := authz.IdentityFromContext(ctx)
	switch {
	case ok && authID != "":
		if user != "" && user != authID {
			return nil, errors.E(ctx, errors.CatPermission, errors.TypeIdentityMismatch, fmt.Errorf("zuul-user header does not match the authenticated identity"))
		}
		return ctx, nil
	case user != "":
		return authz.WithIdentity(ctx, user), nil
	default:
		return ctx, nil
	}
}

// firstMetadata returns the first value of the incoming metadata key, trimmed, or "".
func firstMetadata(ctx context.Context, key string) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return strings.TrimSpace(vals[0])
}

// verifyPeer requires a CA-verified client certificate on the forward plane, chained
// to the peer CA when one is configured.
func (g *authGate) verifyPeer(ctx context.Context) error {
	cert := peerLeaf(ctx)
	if cert == nil {
		return errors.E(ctx, errors.CatPermission, errors.TypePeerCertRequired, fmt.Errorf("forward plane requires a verified peer certificate"))
	}
	if g.peerCA != nil {
		opts := x509.VerifyOptions{Roots: g.peerCA, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if _, err := cert.Verify(opts); err != nil {
			return errors.E(ctx, errors.CatPermission, errors.TypePeerCertUntrusted, fmt.Errorf("forward plane peer certificate is not issued by the peer CA"))
		}
	}
	if len(g.peerCNs) > 0 {
		if _, ok := g.peerCNs[cert.Subject.CommonName]; !ok {
			return errors.E(ctx, errors.CatPermission, errors.TypePeerCertNotAllowed, fmt.Errorf("forward plane peer certificate Common Name is not an allowed node"))
		}
	}
	return nil
}

// unary returns the unary interceptor enforcing the gate.
func (g *authGate) unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := g.check(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// stream returns the stream interceptor enforcing the gate.
func (g *authGate) stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := g.check(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &identityStream{ServerStream: ss, ctx: ctx})
	}
}

// peerLeaf returns the caller's leaf client certificate, or nil if none was
// presented (or it was not verified by the TLS layer).
func peerLeaf(ctx context.Context) *x509.Certificate {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil
	}
	return tlsInfo.State.PeerCertificates[0]
}

// identityStream carries the identity-bearing context to the stream handler.
type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityStream) Context() context.Context { return s.ctx }

// maxIdentityLimiters caps the per-identity limiter map. Identities come from the
// operator's CA or tokens file, so the cardinality is operator-bounded in practice;
// the cap is a backstop against memory growth if that assumption breaks.
const maxIdentityLimiters = 10_000

// identityLimiters is a per-identity token-bucket set: each authenticated identity
// (or "anonymous") gets its own limiter, so one noisy client cannot starve others.
type identityLimiters struct {
	limit rate.Limit
	burst int

	mu sync.Mutex
	m  map[string]*rate.Limiter
}

func newIdentityLimiters(perSec float64, burst int) *identityLimiters {
	if burst <= 0 {
		burst = int(perSec) + 1
	}
	return &identityLimiters{limit: rate.Limit(perSec), burst: burst, m: map[string]*rate.Limiter{}}
}

// allow charges one request to the identity's bucket. At the cap, the whole map is
// reset rather than grown — momentarily refilling every bucket, which favors
// availability over briefly stricter limiting.
func (l *identityLimiters) allow(identity string) bool {
	if identity == "" {
		identity = "anonymous"
	}
	l.mu.Lock()
	if len(l.m) >= maxIdentityLimiters {
		l.m = map[string]*rate.Limiter{}
	}
	lim := l.m[identity]
	if lim == nil {
		lim = rate.NewLimiter(l.limit, l.burst)
		l.m[identity] = lim
	}
	l.mu.Unlock()
	return lim.Allow()
}

// perIdentityUnary rejects a unary call when the caller's own bucket is drained.
func perIdentityUnary(l *identityLimiters) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		identity, _ := authz.IdentityFromContext(ctx)
		if !l.allow(identity) {
			return nil, errors.E(ctx, errors.CatResourceExhausted, errors.TypeRateLimited, fmt.Errorf("rate limit exceeded for %q", identity))
		}
		return handler(ctx, req)
	}
}

// perIdentityStream rejects a new stream when the caller's bucket is drained.
func perIdentityStream(l *identityLimiters) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		identity, _ := authz.IdentityFromContext(ss.Context())
		if !l.allow(identity) {
			return errors.E(ss.Context(), errors.CatResourceExhausted, errors.TypeRateLimited, fmt.Errorf("rate limit exceeded for %q", identity))
		}
		return handler(srv, ss)
	}
}
