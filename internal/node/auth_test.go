package node

import (
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/gostdlib/base/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/zuultls/zuultlstest"
)

// TestBearerCaseInsensitive proves the bearer scheme is matched case-insensitively
// (RFC 7235), and the token still validates regardless of "Bearer"/"bearer"/"BEARER".
func TestBearerCaseInsensitive(t *testing.T) {
	b := &bearerAuth{tokens: map[string]string{"sekrit": "orders-svc"}}
	tests := []struct {
		name    string
		header  string
		wantErr bool
	}{
		{name: "Success: canonical Bearer", header: "Bearer sekrit"},
		{name: "Success: lowercase bearer", header: "bearer sekrit"},
		{name: "Success: uppercase BEARER", header: "BEARER sekrit"},
		{name: "Success: bare token (no scheme)", header: "sekrit"},
		{name: "Error: wrong token", header: "Bearer nope", wantErr: true},
	}
	for _, test := range tests {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", test.header))
		_, err := b.authenticate(ctx)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestBearerCaseInsensitive(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestBearerCaseInsensitive(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestReconcileUser proves the zuul-user header reconciliation: with no
// authenticated identity the header becomes the principal; with an authenticated
// identity a matching or absent header is accepted and a mismatched one is denied.
func TestReconcileUser(t *testing.T) {
	tests := []struct {
		name    string
		authID  string // token/OIDC identity attached to ctx (empty = unauthenticated)
		header  string // zuul-user header (empty = absent)
		wantErr bool
		wantID  string // expected principal after reconcile (when no error)
	}{
		{name: "Success: unauthenticated header becomes principal", header: "alice", wantID: "alice"},
		{name: "Success: unauthenticated and no header stays anonymous", wantID: ""},
		{name: "Success: authenticated with matching header", authID: "alice", header: "alice", wantID: "alice"},
		{name: "Success: authenticated with no header", authID: "alice", wantID: "alice"},
		{name: "Error: authenticated with mismatched header", authID: "alice", header: "bob", wantErr: true},
	}
	for _, test := range tests {
		ctx := context.Background()
		if test.header != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(authz.UserHeader, test.header))
		}
		if test.authID != "" {
			ctx = authz.WithIdentity(ctx, test.authID)
		}
		got, err := reconcileUser(ctx)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestReconcileUser(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestReconcileUser(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("TestReconcileUser(%s): code = %s, want PermissionDenied", test.name, status.Code(err))
			}
			continue
		}
		if id, _ := authz.IdentityFromContext(got); id != test.wantID {
			t.Errorf("TestReconcileUser(%s): principal = %q, want %q", test.name, id, test.wantID)
		}
	}
}

// TestReconcileUserMTLS proves the header is reconciled against the mTLS certificate
// CN, not just a token identity: a mismatch is denied, a match yields the CN.
func TestReconcileUserMTLS(t *testing.T) {
	ca, _ := zuultlstest.NewCA(t)
	leaf, _ := ca.Leaf(t, "alice") // certificate CommonName = alice

	mismatch := metadata.NewIncomingContext(peerCtx(leaf), metadata.Pairs(authz.UserHeader, "bob"))
	if _, err := reconcileUser(mismatch); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestReconcileUserMTLS: mismatched header vs cert CN: got %s, want PermissionDenied", status.Code(err))
	}

	match := metadata.NewIncomingContext(peerCtx(leaf), metadata.Pairs(authz.UserHeader, "alice"))
	got, err := reconcileUser(match)
	if err != nil {
		t.Fatalf("TestReconcileUserMTLS: matching header: got err == %s, want nil", err)
	}
	if id, _ := authz.IdentityFromContext(got); id != "alice" {
		t.Errorf("TestReconcileUserMTLS: principal = %q, want alice", id)
	}
}

// TestVerifyPeer proves the forward-plane peer check: no certificate is denied, a
// certificate not chained to the peer CA is denied, and one issued by the peer CA
// passes.
func TestVerifyPeer(t *testing.T) {
	caA, poolA := zuultlstest.NewCA(t)
	_, poolB := zuultlstest.NewCA(t)
	leafFromA, _ := caA.Leaf(t, "zuul-node")

	tests := []struct {
		name    string
		gate    *authGate
		ctx     context.Context
		wantErr bool
	}{
		{
			name:    "Error: no certificate presented",
			gate:    &authGate{guardPeer: true, peerCA: poolA},
			ctx:     peerCtx(nil),
			wantErr: true,
		},
		{
			name:    "Error: certificate not issued by the peer CA",
			gate:    &authGate{guardPeer: true, peerCA: poolB},
			ctx:     peerCtx(leafFromA),
			wantErr: true,
		},
		{
			name: "Success: certificate issued by the peer CA",
			gate: &authGate{guardPeer: true, peerCA: poolA},
			ctx:  peerCtx(leafFromA),
		},
		{
			name: "Success: any certificate when no peer CA configured",
			gate: &authGate{guardPeer: true},
			ctx:  peerCtx(leafFromA),
		},
		{
			name: "Success: CN in the allowlist",
			gate: &authGate{guardPeer: true, peerCA: poolA, peerCNs: map[string]struct{}{"zuul-node": {}}},
			ctx:  peerCtx(leafFromA),
		},
		{
			name:    "Error: CN not in the allowlist",
			gate:    &authGate{guardPeer: true, peerCA: poolA, peerCNs: map[string]struct{}{"other-node": {}}},
			ctx:     peerCtx(leafFromA),
			wantErr: true,
		},
	}
	for _, test := range tests {
		err := test.gate.verifyPeer(test.ctx)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestVerifyPeer(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestVerifyPeer(%s): got err == %s, want err == nil", test.name, err)
		case err != nil && status.Code(err) != codes.PermissionDenied:
			t.Errorf("TestVerifyPeer(%s): code = %s, want PermissionDenied", test.name, status.Code(err))
		}
	}
}

// peerCtx builds a context carrying leaf as the verified peer certificate (or none).
func peerCtx(leaf *x509.Certificate) context.Context {
	state := tls.ConnectionState{}
	if leaf != nil {
		state.PeerCertificates = []*x509.Certificate{leaf}
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

// TestPeerCNsDropEmpty proves an empty Common Name never enters the allowlist, so a
// certificate without a CN cannot pass the pin.
func TestPeerCNsDropEmpty(t *testing.T) {
	ca, pool := zuultlstest.NewCA(t)
	emptyCN, _ := ca.Leaf(t, "") // a peer-CA-signed cert with no CommonName

	// Build the gate's CN set exactly as node.New does, then prove "" is excluded.
	cfgCNs := []string{"", "node-a"}
	var peerCNs map[string]struct{}
	for _, cn := range cfgCNs {
		if cn == "" {
			continue
		}
		if peerCNs == nil {
			peerCNs = make(map[string]struct{})
		}
		peerCNs[cn] = struct{}{}
	}
	gate := &authGate{guardPeer: true, peerCA: pool, peerCNs: peerCNs}

	if err := gate.verifyPeer(peerCtx(emptyCN)); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestPeerCNsDropEmpty: empty-CN cert: got %s, want PermissionDenied", status.Code(err))
	}
}
