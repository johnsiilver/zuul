package integration

import (
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/client"
	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/zuultls"
)

// TestACLPathEnforcement proves path-based ACLs over the insecure transport with the
// principal supplied by the zuul-user header (client.WithUser): home-directory
// auto-grant, cross-user read grants, write denial, invalid-path rejection, and that
// an unidentified caller is denied.
func TestACLPathEnforcement(t *testing.T) {
	policy := authz.HomeDir(authz.Prefix(map[string][]authz.Rule{
		"bob": {{Prefix: "/alice/configs/", Write: false}},
	}))
	c := buildCluster(t, 3, 4, clusterOpts{authorizer: policy})
	ctx := t.Context()

	dialUser := func(n *node, user string) *client.Client {
		opts := []client.Option{client.WithClientID(user), client.WithTTL(30 * time.Second)}
		if user != "" {
			opts = append(opts, client.WithUser(user))
		}
		cl, err := client.New(ctx, client.Endpoints{c.grpcAddrs[n.replicaID]}, opts...)
		if err != nil {
			t.Fatalf("TestACLPathEnforcement: dial %q: %s", user, err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}

	alice := dialUser(c.nodes[0], "alice")
	bob := dialUser(c.nodes[1], "bob")
	anon := dialUser(c.nodes[2], "") // no WithUser: unidentified principal

	// Home directory: a principal may lock its own /<user>/ subtree without a rule.
	if ok, err := alice.NewMutex("/alice/orders").TryLock(ctx); err != nil || !ok {
		t.Fatalf("TestACLPathEnforcement: alice locks own subtree: ok=%v err=%v, want true/nil", ok, err)
	}
	if ok, err := bob.NewMutex("/bob/lock").TryLock(ctx); err != nil || !ok {
		t.Fatalf("TestACLPathEnforcement: bob locks own subtree: ok=%v err=%v, want true/nil", ok, err)
	}

	// bob has no grant on alice's subtree: write is denied.
	if _, err := bob.NewMutex("/alice/orders2").TryLock(ctx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestACLPathEnforcement: bob writes alice's subtree: got %s, want PermissionDenied", status.Code(err))
	}

	// Cross-user read grant: bob may read alice's /alice/configs/ election but not
	// participate in it.
	if err := alice.NewElection("/alice/configs/leader").Campaign(ctx, []byte("alice"), 0); err != nil {
		t.Fatalf("TestACLPathEnforcement: alice campaign: %s", err)
	}
	if _, err := bob.NewElection("/alice/configs/leader").Leader(ctx); err != nil {
		t.Errorf("TestACLPathEnforcement: bob reads granted election: got err == %s, want nil", err)
	}
	if err := bob.NewElection("/alice/configs/leader").Campaign(ctx, []byte("bob"), time.Second); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestACLPathEnforcement: bob campaigns on read-only grant: got %s, want PermissionDenied", status.Code(err))
	}

	// An unidentified caller (no zuul-user header, no auth) is denied everywhere.
	if _, err := anon.NewMutex("/carol/lock").TryLock(ctx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestACLPathEnforcement: anonymous caller: got %s, want PermissionDenied", status.Code(err))
	}

	// A non-path key is rejected before authorization.
	if _, err := alice.NewMutex("bare-key").TryLock(ctx); status.Code(err) != codes.InvalidArgument {
		t.Errorf("TestACLPathEnforcement: non-path key: got %s, want InvalidArgument", status.Code(err))
	}
}

// TestAuthzEnforcement proves per-key authorization keyed off the mTLS client
// certificate identity: the client (cert CN "zuul-node") may lock keys under its
// allowed prefix but is denied elsewhere.
func TestAuthzEnforcement(t *testing.T) {
	certs := genCerts(t) // client + server cert CN == "zuul-node"
	policy := authz.Prefix(map[string][]authz.Rule{
		"zuul-node": {{Prefix: "/allowed/", Write: true}},
	})
	c := newAuthzCluster(t, 3, 4, certs, policy)
	ctx := t.Context()

	clientTLS, err := zuultls.ClientConfig(certs.ca, certs.cert, certs.key)
	if err != nil {
		t.Fatalf("TestAuthzEnforcement: client TLS: %s", err)
	}
	creds := grpc.WithTransportCredentials(credentials.NewTLS(clientTLS))
	cl, err := client.New(ctx, client.Endpoints{c.grpcAddrs[c.nodes[0].replicaID]}, client.WithClientID("alice"), client.WithTTL(30*time.Second), client.WithDialOptions(creds))
	if err != nil {
		t.Fatalf("TestAuthzEnforcement: Dial: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	// A key under the allowed prefix is permitted.
	ok, err := cl.NewMutex("/allowed/resource").TryLock(ctx)
	if err != nil {
		t.Fatalf("TestAuthzEnforcement: allowed TryLock: %s", err)
	}
	if !ok {
		t.Errorf("TestAuthzEnforcement: allowed TryLock: acquired=false, want true")
	}

	// A key outside the allowed prefix is denied.
	_, err = cl.NewMutex("/denied/resource").TryLock(ctx)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestAuthzEnforcement: denied TryLock: got code %s, want PermissionDenied", status.Code(err))
	}
}
