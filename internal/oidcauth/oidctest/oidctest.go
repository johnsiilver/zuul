// Package oidctest is a fake OIDC issuer for tests: it serves the discovery
// document and a JWKS over a local loopback HTTP listener, and signs tokens with its
// own key.
//
// This is test scaffolding (it follows the net/http/httptest pattern of a non-test
// helper package so it can be shared across packages' tests). It MUST NOT be
// imported by non-test code: it stands up an unauthenticated HTTP server and signs
// arbitrary claims. It only ever serves a loopback address, so production OIDC
// config — which requires an https issuer except for loopback — will not accept it
// by accident.
package oidctest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// Provider is a running fake issuer.
type Provider struct {
	// URL is the issuer URL (use it as oidcauth.Config.Issuer).
	URL string

	key *rsa.PrivateKey
	srv *httptest.Server
}

// New starts a fake issuer.
func New() (*Provider, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("oidctest: generate key: %w", err)
	}
	p := &Provider{key: key}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   p.URL,
			"jwks_uri": p.URL + "/keys",
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{
			Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test-key", Algorithm: "RS256", Use: "sig"}},
		})
	})
	p.srv = httptest.NewServer(mux)
	p.URL = p.srv.URL
	return p, nil
}

// Close stops the issuer.
func (p *Provider) Close() {
	p.srv.Close()
}

// Sign issues a signed JWT with the given extra claims on top of iss/iat/exp
// (aud is the caller's to set, e.g. "aud": "zuul").
func (p *Provider) Sign(claims map[string]any) (string, error) {
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test-key"))
	if err != nil {
		return "", fmt.Errorf("oidctest: signer: %w", err)
	}
	all := map[string]any{
		"iss": p.URL,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range claims {
		all[k] = v
	}
	payload, err := json.Marshal(all)
	if err != nil {
		return "", fmt.Errorf("oidctest: marshal claims: %w", err)
	}
	jws, err := signer.Sign(payload)
	if err != nil {
		return "", fmt.Errorf("oidctest: sign: %w", err)
	}
	return jws.CompactSerialize()
}
