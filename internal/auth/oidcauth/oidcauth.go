// Package oidcauth validates OIDC bearer tokens (JWTs) and maps them to an
// authorization identity. It covers any standards-compliant provider — including
// Azure Entra ID / Managed Service Identity, whose tokens are OIDC JWTs issued by
// https://login.microsoftonline.com/<tenant>/v2.0 — via issuer discovery and JWKS
// signature verification (keys are fetched and rotated automatically).
package oidcauth

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/retry/exponential"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
)

// maxTokenCache bounds the validated-token cache. Tokens are operator-issued so the
// live set is small; the cap is a backstop against unbounded growth. At the cap new
// entries are skipped (re-verified each time) rather than the whole cache cleared,
// so there is no re-verification cliff.
const maxTokenCache = 16384

// maxCacheTTL caps how long a validated token is trusted from cache regardless of
// its (possibly long) expiry. It bounds the window in which a token whose signing
// key was rotated or revoked at the issuer is still accepted.
const maxCacheTTL = time.Minute

// Config configures token validation.
type Config struct {
	// Issuer is the token issuer URL (e.g.
	// "https://login.microsoftonline.com/<tenant>/v2.0"). Discovery and JWKS are
	// fetched from it. Required.
	Issuer string
	// Audience is the audience (aud) tokens must carry — the app ID URI or client
	// id this service is registered as. Required.
	Audience string
	// IdentityClaim names the claim used as the authorization identity. Default
	// "sub"; Azure deployments often want "oid" (the object id) or "azp".
	IdentityClaim string
}

func (c Config) validate() error {
	switch {
	case c.Issuer == "":
		return fmt.Errorf("oidcauth.Config: Issuer is required")
	case c.Audience == "":
		return fmt.Errorf("oidcauth.Config: Audience is required")
	}
	u, err := url.Parse(c.Issuer)
	if err != nil {
		return fmt.Errorf("oidcauth.Config: Issuer %q is not a valid URL: %w", c.Issuer, err)
	}
	// Discovery and JWKS are fetched from the issuer; over plaintext HTTP an attacker
	// could swap the signing keys and forge tokens. Allow http only for loopback
	// (local testing).
	if u.Scheme != "https" && !isLoopback(u.Hostname()) {
		return fmt.Errorf("oidcauth.Config: Issuer must use https (got %q); plaintext is allowed only for loopback", c.Issuer)
	}
	return nil
}

// isLoopback reports whether host is a loopback name or address.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Verifier validates bearer tokens against one OIDC issuer, caching results for up
// to maxCacheTTL so repeated calls with the same token skip the signature check (RSA
// verification is far more expensive than a sharded-map lookup). The cache is a
// lock-striped ShardedMap so concurrent client RPCs don't serialize on one mutex.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
	claim    string
	cache    sync.ShardedMap[string, cacheEntry]
	// flight collapses concurrent verifications of the same token so a cache miss runs
	// the expensive signature check (and any JWKS fetch) once, not once per caller.
	flight sync.Flight[string, cacheEntry]
}

// cacheEntry is a previously-validated token's identity and the time it stops being
// trusted from cache (min of the token's expiry and now+maxCacheTTL).
type cacheEntry struct {
	identity string
	exp      time.Time
}

// New discovers the issuer's configuration (retried with backoff — the provider may
// be briefly unreachable at boot) and returns a Verifier.
func New(ctx context.Context, cfg Config) (*Verifier, error) {
	if err := cfg.validate(); err != nil {
		return nil, errors.E(ctx, errors.CatRequest, errors.TypeConfig, err)
	}
	claim := cfg.IdentityClaim
	if claim == "" {
		claim = "sub"
	}

	boff, err := exponential.New()
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeBackend, fmt.Errorf("oidcauth.New: %w", err))
	}
	var provider *oidc.Provider
	err = boff.Retry(ctx, func(ctx context.Context, _ exponential.Record) error {
		var perr error
		provider, perr = oidc.NewProvider(ctx, cfg.Issuer)
		return perr
	})
	if err != nil {
		return nil, fmt.Errorf("oidcauth.New: discover issuer %s: %w", cfg.Issuer, err)
	}
	return &Verifier{
		verifier: provider.Verifier(&oidc.Config{ClientID: cfg.Audience}),
		claim:    claim,
	}, nil
}

// Verify validates raw (signature via the issuer's JWKS, issuer, audience, expiry)
// and returns the identity claim's value. A previously-validated token still within
// its cache window is served without re-verifying; an expired entry is dropped.
func (v *Verifier) Verify(ctx context.Context, raw string) (string, error) {
	now := time.Now()
	if e, ok := v.cache.Get(raw); ok {
		if now.Before(e.exp) {
			return e.identity, nil
		}
		v.cache.Del(raw) // expired: reclaim and re-verify below
	}

	// On a miss, single-flight the verification so N concurrent RPCs bearing the same
	// token run the signature check once and share the result.
	entry, err, _ := v.flight.Do(ctx, raw, func() (cacheEntry, error) {
		return v.verifyAndCache(ctx, raw)
	})
	if err != nil {
		return "", err
	}
	return entry.identity, nil
}

// verifyAndCache verifies raw against the issuer (signature, issuer, audience, expiry),
// extracts the identity claim, and caches the result. It re-checks the cache first, since
// another flight for the same token may have just filled it.
func (v *Verifier) verifyAndCache(ctx context.Context, raw string) (cacheEntry, error) {
	now := time.Now()
	if e, ok := v.cache.Get(raw); ok && now.Before(e.exp) {
		return e, nil
	}
	token, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return cacheEntry{}, errors.E(ctx, errors.CatUnauthenticated, errors.TypeInvalidToken, fmt.Errorf("oidcauth: invalid token: %w", err))
	}
	claims := map[string]any{}
	if err := token.Claims(&claims); err != nil {
		return cacheEntry{}, errors.E(ctx, errors.CatUnauthenticated, errors.TypeInvalidToken, fmt.Errorf("oidcauth: decode claims: %w", err))
	}
	identity, ok := claims[v.claim].(string)
	if !ok || identity == "" {
		return cacheEntry{}, errors.E(ctx, errors.CatUnauthenticated, errors.TypeInvalidToken, fmt.Errorf("oidcauth: token has no usable %q claim", v.claim))
	}
	entry := cacheEntry{identity: identity, exp: cacheUntil(now, token.Expiry)}
	if v.cache.Len() < maxTokenCache {
		v.cache.Set(raw, entry)
	}
	return entry, nil
}

// cacheUntil returns the time a token validated at now should stop being trusted
// from cache: the sooner of its own expiry and now+maxCacheTTL.
func cacheUntil(now, exp time.Time) time.Time {
	cap := now.Add(maxCacheTTL)
	if exp.IsZero() || exp.After(cap) {
		return cap
	}
	return exp
}
