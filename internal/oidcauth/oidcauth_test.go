package oidcauth

import (
	"testing"
	"time"

	"github.com/johnsiilver/zuul/internal/oidcauth/oidctest"
)

// TestVerify covers token validation against a live (fake) issuer: signature,
// audience, expiry, and identity-claim extraction.
func TestVerify(t *testing.T) {
	provider, err := oidctest.New()
	if err != nil {
		t.Fatalf("TestVerify: provider: %s", err)
	}
	t.Cleanup(provider.Close)

	tests := []struct {
		name    string
		claim   string // IdentityClaim config ("" = default sub)
		claims  map[string]any
		wantID  string
		wantErr bool
	}{
		{
			name:   "Success: valid token yields sub identity",
			claims: map[string]any{"aud": "zuul", "sub": "orders-svc"},
			wantID: "orders-svc",
		},
		{
			name:   "Success: custom identity claim (Azure oid)",
			claim:  "oid",
			claims: map[string]any{"aud": "zuul", "sub": "ignored", "oid": "11111111-2222-3333-4444-555555555555"},
			wantID: "11111111-2222-3333-4444-555555555555",
		},
		{
			name:    "Error: wrong audience rejected",
			claims:  map[string]any{"aud": "someone-else", "sub": "orders-svc"},
			wantErr: true,
		},
		{
			name:    "Error: expired token rejected",
			claims:  map[string]any{"aud": "zuul", "sub": "orders-svc", "exp": time.Now().Add(-time.Hour).Unix()},
			wantErr: true,
		},
		{
			name:    "Error: missing identity claim rejected",
			claim:   "oid",
			claims:  map[string]any{"aud": "zuul", "sub": "orders-svc"},
			wantErr: true,
		},
	}
	for _, test := range tests {
		v, err := New(t.Context(), Config{Issuer: provider.URL, Audience: "zuul", IdentityClaim: test.claim})
		if err != nil {
			t.Fatalf("TestVerify(%s): New: %s", test.name, err)
		}
		raw, err := provider.Sign(test.claims)
		if err != nil {
			t.Fatalf("TestVerify(%s): Sign: %s", test.name, err)
		}
		id, err := v.Verify(t.Context(), raw)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestVerify(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestVerify(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		if id != test.wantID {
			t.Errorf("TestVerify(%s): identity = %q, want %q", test.name, id, test.wantID)
		}
	}
}

// TestVerifyGarbage proves a non-JWT bearer string is rejected.
func TestVerifyGarbage(t *testing.T) {
	provider, err := oidctest.New()
	if err != nil {
		t.Fatalf("TestVerifyGarbage: provider: %s", err)
	}
	t.Cleanup(provider.Close)
	v, err := New(t.Context(), Config{Issuer: provider.URL, Audience: "zuul"})
	if err != nil {
		t.Fatalf("TestVerifyGarbage: New: %s", err)
	}
	if _, err := v.Verify(t.Context(), "not-a-jwt"); err == nil {
		t.Errorf("TestVerifyGarbage: got err == nil, want err != nil")
	}
}

// TestHTTPSIssuerRequired proves a non-loopback http issuer is rejected while https
// and loopback http are accepted.
func TestHTTPSIssuerRequired(t *testing.T) {
	tests := []struct {
		name    string
		issuer  string
		wantErr bool
	}{
		{name: "Error: plaintext http issuer", issuer: "http://issuer.example.com", wantErr: true},
		{name: "Success: https issuer", issuer: "https://issuer.example.com"},
		{name: "Success: loopback http (testing)", issuer: "http://127.0.0.1:9000"},
		{name: "Success: localhost http (testing)", issuer: "http://localhost:9000"},
	}
	for _, test := range tests {
		err := Config{Issuer: test.issuer, Audience: "zuul"}.validate()
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestHTTPSIssuerRequired(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestHTTPSIssuerRequired(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}
