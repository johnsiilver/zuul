package context

import (
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// identityKey carries an authenticated identity attached by an auth interceptor
// (e.g. token auth).
type identityKey struct{}

// WithIdentity returns ctx carrying an authenticated identity. An auth interceptor
// (token auth) calls this after validating the credential; IdentityFromContext
// prefers it over the TLS certificate.
func WithIdentity(ctx Context, identity string) Context {
	return WithValue(ctx, identityKey{}, identity)
}

// IdentityFromContext returns the caller's authenticated identity and whether one
// was presented: an interceptor-attached identity (token auth) if present,
// otherwise the mutual-TLS client certificate Common Name.
func IdentityFromContext(ctx Context) (string, bool) {
	if id, ok := ctx.Value(identityKey{}).(string); ok && id != "" {
		return id, true
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", false
	}
	return tlsInfo.State.PeerCertificates[0].Subject.CommonName, true
}
