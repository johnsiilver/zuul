package node

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/client"
	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/authz"
	"github.com/johnsiilver/zuul/internal/auth/oidcauth/oidctest"
	"github.com/johnsiilver/zuul/internal/auth/zuultls"
	"github.com/johnsiilver/zuul/internal/auth/zuultls/zuultlstest"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
)

// TestNodeHealth boots a real single node through the full assembly (hardening,
// tracing, health) and proves the gRPC health service reports NOT_SERVING before
// Start's watcher confirms readiness, SERVING once the shards have leaders, and
// NOT_SERVING again once draining.
func TestNodeHealth(t *testing.T) {
	ctx := t.Context()

	raftAddr, err := freePort()
	if err != nil {
		t.Fatalf("TestNodeHealth: freePort: %s", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TestNodeHealth: listen: %s", err)
	}
	grpcAddr := lis.Addr().String()

	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := New(bootCtx, Config{
		ReplicaID:   1,
		RaftAddr:    raftAddr,
		GRPCAddr:    grpcAddr,
		DataDir:     "zuul-health-test",
		Shards:      []uint64{1, 2},
		MetaShardID: 1_000_000,
		Members:     map[uint64]string{1: raftAddr},
		Seed:        map[uint64]string{1: grpcAddr},
	})
	if err != nil {
		t.Fatalf("TestNodeHealth: New: %s", err)
	}
	t.Cleanup(n.Close)
	context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(lis) })

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("TestNodeHealth: dial: %s", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	hc := healthpb.NewHealthClient(conn)

	// Before Start, the status is NOT_SERVING.
	if got := check(t, hc); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Errorf("TestNodeHealth: pre-Start status = %s, want NOT_SERVING", got)
	}

	if err := n.Start(ctx); err != nil {
		t.Fatalf("TestNodeHealth: Start: %s", err)
	}
	if !awaitStatus(t, hc, healthpb.HealthCheckResponse_SERVING) {
		t.Fatalf("TestNodeHealth: never became SERVING")
	}

	// Draining flips it to NOT_SERVING and keeps it there. (On a single node there
	// is nowhere to transfer leadership, so Drain runs out its budget — keep it short.)
	drainCtx, cancelDrain := context.WithTimeout(ctx, 2*time.Second)
	n.Drain(drainCtx)
	cancelDrain()
	if !awaitStatus(t, hc, healthpb.HealthCheckResponse_NOT_SERVING) {
		t.Errorf("TestNodeHealth: not NOT_SERVING after Drain")
	}
}

// check performs one health check.
func check(t *testing.T, hc healthpb.HealthClient) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	resp, err := hc.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check: %s", err)
	}
	return resp.GetStatus()
}

// awaitStatus polls until the health status matches want (or times out).
func awaitStatus(t *testing.T, hc healthpb.HealthClient, want healthpb.HealthCheckResponse_ServingStatus) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check(t, hc) == want {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// freePort returns a free loopback address.
func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port), nil
}

// TestTokenAuth boots a mutual-TLS node with bearer-token authentication and
// proves: a client with a valid token works (and its token identity passes authz),
// a client without one is rejected, the health service stays exempt for probes, and
// token auth without mTLS is refused outright (the forward plane would be a bypass).
func TestTokenAuth(t *testing.T) {
	ctx := t.Context()
	ca, cert, key := zuultlstest.GenCerts(t)

	raftAddr, err := freePort()
	if err != nil {
		t.Fatalf("TestTokenAuth: freePort: %s", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TestTokenAuth: listen: %s", err)
	}
	grpcAddr := lis.Addr().String()

	cfg := Config{
		ReplicaID:   1,
		RaftAddr:    raftAddr,
		GRPCAddr:    grpcAddr,
		DataDir:     "zuul-token-test",
		Shards:      []uint64{1, 2},
		MetaShardID: 1_000_000,
		Members:     map[uint64]string{1: raftAddr},
		Seed:        map[uint64]string{1: grpcAddr},
		MutualTLS:   true,
		CAFile:      ca,
		CertFile:    cert,
		KeyFile:     key,
		Tokens:      map[string]string{"sekrit": "orders-svc"},
		Authorizer:  authz.Prefix(map[string][]authz.Rule{"orders-svc": {{Prefix: "/orders/", Write: true}}}),
	}

	// Token auth without mTLS is a refused configuration.
	insecureCfg := cfg
	insecureCfg.MutualTLS = false
	if _, err := New(ctx, insecureCfg); err == nil {
		t.Fatalf("TestTokenAuth: New with Tokens and no MutualTLS: got err == nil, want err != nil")
	}

	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := New(bootCtx, cfg)
	if err != nil {
		t.Fatalf("TestTokenAuth: New: %s", err)
	}
	t.Cleanup(n.Close)
	context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(lis) })
	if err := n.Start(ctx); err != nil {
		t.Fatalf("TestTokenAuth: Start: %s", err)
	}

	clientTLS, err := zuultls.ClientConfig(ca, cert, key)
	if err != nil {
		t.Fatalf("TestTokenAuth: client TLS: %s", err)
	}
	tc := credentials.NewTLS(clientTLS)
	creds := grpc.WithTransportCredentials(tc)

	// Health stays exempt (probes carry TLS but no token).
	conn, err := grpc.NewClient(grpcAddr, creds)
	if err != nil {
		t.Fatalf("TestTokenAuth: dial: %s", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("TestTokenAuth: health without token: %s, want exempt", err)
	}

	// A tokenless client (valid TLS) cannot open a session.
	dialCtx, cancelDial := context.WithTimeout(ctx, 3*time.Second)
	defer cancelDial()
	if bad, err := client.New(dialCtx, client.Endpoints{grpcAddr}, client.WithClientID("intruder"), client.WithTransportCredentials(tc)); err == nil {
		_ = bad.Close()
		t.Errorf("TestTokenAuth: tokenless client connected, want Unauthenticated")
	}

	// A token-bearing client works, and its token identity drives authz.
	cl, err := client.New(ctx, client.Endpoints{grpcAddr}, client.WithClientID("good"), client.WithAuthToken("sekrit"), client.WithTransportCredentials(tc))
	if err != nil {
		t.Fatalf("TestTokenAuth: token client Dial: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	ok, err := cl.NewMutex("/orders/42").TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TestTokenAuth: allowed TryLock: ok=%v err=%v, want true/nil", ok, err)
	}
	if _, err := cl.NewMutex("/users/7").TryLock(ctx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestTokenAuth: denied TryLock: got %s, want PermissionDenied", status.Code(err))
	}
}

// TestServerTLSOIDC boots a node in server-TLS mode (one-way TLS: encryption +
// server authentication, no client certs) with OIDC bearer authentication, and
// proves: a certificate-less client with a valid OIDC token works and its identity
// claim drives authz; a tokenless client is rejected; and the inter-node forward
// plane refuses certificate-less callers (no bypass around token auth).
func TestServerTLSOIDC(t *testing.T) {
	ctx := t.Context()
	ca, cert, key := zuultlstest.GenCerts(t)

	issuer, err := oidctest.New()
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: issuer: %s", err)
	}
	t.Cleanup(issuer.Close)

	raftAddr, err := freePort()
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: freePort: %s", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: listen: %s", err)
	}
	grpcAddr := lis.Addr().String()

	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := New(bootCtx, Config{
		ReplicaID:    1,
		RaftAddr:     raftAddr,
		GRPCAddr:     grpcAddr,
		DataDir:      "zuul-oidc-test",
		Shards:       []uint64{1, 2},
		MetaShardID:  1_000_000,
		Members:      map[uint64]string{1: raftAddr},
		Seed:         map[uint64]string{1: grpcAddr},
		ServerTLS:    true,
		CAFile:       ca,
		CertFile:     cert,
		KeyFile:      key,
		OIDCIssuer:   issuer.URL,
		OIDCAudience: "api://zuul",
		Authorizer:   authz.Prefix(map[string][]authz.Rule{"orders-svc": {{Prefix: "/orders/", Write: true}}}),
	})
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: New: %s", err)
	}
	t.Cleanup(n.Close)
	context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(lis) })
	if err := n.Start(ctx); err != nil {
		t.Fatalf("TestServerTLSOIDC: Start: %s", err)
	}

	// Certificate-less transport: the client only verifies the server.
	rootsTLS, err := zuultls.ClientRootsConfig(ca)
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: roots config: %s", err)
	}
	tc := credentials.NewTLS(rootsTLS)
	creds := grpc.WithTransportCredentials(tc)

	// A token-bearing client works; its OIDC sub claim is the authz identity.
	source := func(context.Context) (string, error) {
		return issuer.Sign(map[string]any{"aud": "api://zuul", "sub": "orders-svc"})
	}
	cl, err := client.New(ctx, client.Endpoints{grpcAddr}, client.WithClientID("good"), client.WithTokenSource(source), client.WithTransportCredentials(tc))
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: Dial: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	ok, err := cl.NewMutex("/orders/42").TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TestServerTLSOIDC: allowed TryLock: ok=%v err=%v, want true/nil", ok, err)
	}
	if _, err := cl.NewMutex("/users/7").TryLock(ctx); status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestServerTLSOIDC: denied TryLock: got %s, want PermissionDenied", status.Code(err))
	}

	// A tokenless client cannot open a session.
	dialCtx, cancelDial := context.WithTimeout(ctx, 3*time.Second)
	defer cancelDial()
	if bad, err := client.New(dialCtx, client.Endpoints{grpcAddr}, client.WithClientID("intruder"), client.WithTransportCredentials(tc)); err == nil {
		_ = bad.Close()
		t.Errorf("TestServerTLSOIDC: tokenless client connected, want Unauthenticated")
	}

	// The forward plane refuses certificate-less callers — even with a valid token.
	conn, err := grpc.NewClient(grpcAddr, creds)
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: dial forward: %s", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	tok, err := source(ctx)
	if err != nil {
		t.Fatalf("TestServerTLSOIDC: sign: %s", err)
	}
	fctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
	_, err = forwardpb.NewForwarderClient(conn).Propose(fctx, &forwardpb.ProposeRequest{ShardId: 1, Command: []byte("junk")})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("TestServerTLSOIDC: forward plane without peer cert: got %s, want PermissionDenied", status.Code(err))
	}
}

// TestACLWithoutIdentityAllowed proves a restrictive authorizer is permitted without
// an authenticating method: callers name their principal with the unauthenticated
// zuul-user header. Validate accepts it (New logs an advisory warning).
func TestACLWithoutIdentityAllowed(t *testing.T) {
	cfg := Config{
		ReplicaID:   1,
		RaftAddr:    "127.0.0.1:9001",
		GRPCAddr:    "127.0.0.1:8001",
		DataDir:     "zuul-acl-noauth",
		Shards:      []uint64{1},
		MetaShardID: 1_000_000,
		Members:     map[uint64]string{1: "127.0.0.1:9001"},
		Seed:        map[uint64]string{1: "127.0.0.1:8001"},
		Authorizer:  authz.Prefix(map[string][]authz.Rule{"x": {{Prefix: "/x/", Write: true}}}),
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("TestACLWithoutIdentityAllowed: Validate: got err == %s, want err == nil (header supplies the principal)", err)
	}
}

// TestValidateMutualTLSRequiresFiles proves Config.Validate rejects MutualTLS (like
// ServerTLS) without certificate files, rather than failing later in consensus.New.
func TestValidateMutualTLSRequiresFiles(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "Error: MutualTLS without files", cfg: Config{MutualTLS: true}, wantErr: true},
		{name: "Error: ServerTLS without files", cfg: Config{ServerTLS: true}, wantErr: true},
		{name: "Success: MutualTLS with files", cfg: Config{MutualTLS: true, CAFile: "ca", CertFile: "c", KeyFile: "k"}},
	}
	for _, test := range tests {
		err := test.cfg.Validate()
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestValidateMutualTLSRequiresFiles(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestValidateMutualTLSRequiresFiles(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestForwardedWriteSelf is the in-process regression guard for cross-node
// forwarding over a plaintext connection. It builds two real nodes via node.New
// (with the tracing interceptor in the dial chain, like zuuld) and asserts the meta
// membership converges to 2 — which only happens if a non-leader node can forward
// its WriteSelf to the meta-shard leader. The bug it guards: node.New appended the
// tracing interceptor, defeating the pool's "insecure when no dial options" default,
// so plaintext peer dials had no transport credentials and every forward failed.
func TestForwardedWriteSelf(t *testing.T) {
	ctx := t.Context()

	type addr struct {
		raft, grpc string
		lis        net.Listener
	}
	nodes := make([]addr, 2)
	for i := range nodes {
		raft, err := freePort()
		if err != nil {
			t.Fatalf("TestForwardedWriteSelf: freePort: %s", err)
		}
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("TestForwardedWriteSelf: listen: %s", err)
		}
		nodes[i] = addr{raft: raft, grpc: lis.Addr().String(), lis: lis}
	}
	members := map[uint64]string{1: nodes[0].raft, 2: nodes[1].raft}
	seed := map[uint64]string{1: nodes[0].grpc, 2: nodes[1].grpc}

	// New blocks until a quorum elects leaders, so both nodes must boot concurrently.
	built := make([]*Node, 2)
	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	g := context.Pool(ctx).Group()
	for i := range nodes {
		i := i
		g.Go(ctx, func(ctx context.Context) error {
			n, err := New(bootCtx, Config{
				ReplicaID:   uint64(i + 1),
				RaftAddr:    nodes[i].raft,
				GRPCAddr:    nodes[i].grpc,
				DataDir:     fmt.Sprintf("zuul-fwd-%d", i+1),
				Shards:      []uint64{1, 2},
				MetaShardID: 1_000_000,
				Members:     members,
				Seed:        seed,
			})
			if err != nil {
				return err
			}
			built[i] = n
			return nil
		})
	}
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("TestForwardedWriteSelf: New: %s", err)
	}
	for i := range built {
		n, lis := built[i], nodes[i].lis
		t.Cleanup(n.Close)
		context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(lis) })
	}
	for i := range built {
		if err := built[i].Start(ctx); err != nil {
			t.Fatalf("TestForwardedWriteSelf: Start node %d: %s", i+1, err)
		}
	}

	// Both nodes must appear in the meta shard, which requires a forwarded WriteSelf.
	deadline := time.Now().Add(20 * time.Second)
	for {
		m, err := built[0].Host().MetaList(ctx)
		if err == nil && len(m) == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("TestForwardedWriteSelf: meta membership never reached 2 (forwarded WriteSelf broken?): got %d, err=%v", len(m), err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestRunStartSteps is a regression test for the background-task leak: if a later
// startup step fails, the node-lifetime context (which already has announceSelf and any
// earlier step running on it) must be cancelled, not left alive forever. It also checks
// the success path leaves the context running.
func TestRunStartSteps(t *testing.T) {
	tests := []struct {
		name    string
		failAt  int // index of the step that returns an error; -1 means none fail
		wantErr bool
	}{
		{name: "Success: all steps start, context stays alive", failAt: -1},
		{name: "Error: a later step fails, context is cancelled", failAt: 1, wantErr: true},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(t.Context())

		// firstCtx records the context handed to the first (always-succeeding) step,
		// standing in for announceSelf/an earlier task that must be cancelled on failure.
		var firstCtx context.Context
		steps := []startStep{
			{
				name: "first",
				run: func(c context.Context) error {
					firstCtx = c
					return nil
				},
			},
			{
				name: "second",
				run: func(c context.Context) error {
					if test.failAt == 1 {
						return errors.New("boom")
					}
					return nil
				},
			},
		}

		err := runStartSteps(ctx, cancel, steps)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestRunStartSteps(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestRunStartSteps(%s): got err == %s, want err == nil", test.name, err)
		}

		cancelled := firstCtx.Err() != nil
		if cancelled != test.wantErr {
			t.Errorf("TestRunStartSteps(%s): started-task context cancelled = %v, want %v", test.name, cancelled, test.wantErr)
		}
		cancel()
	}
}

// TestNodeUI boots a real node with the embedded web UI enabled, acquires a lock through
// the production client, and confirms the UI renders the namespace listing and the
// record's detail end-to-end (node.New wiring + in-process Browse + HTTP handlers).
func TestNodeUI(t *testing.T) {
	ctx := t.Context()

	raftAddr, err := freePort()
	if err != nil {
		t.Fatalf("TestNodeUI: freePort: %s", err)
	}
	uiAddr, err := freePort()
	if err != nil {
		t.Fatalf("TestNodeUI: freePort (ui): %s", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TestNodeUI: listen: %s", err)
	}
	grpcAddr := lis.Addr().String()

	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := New(bootCtx, Config{
		ReplicaID:   1,
		RaftAddr:    raftAddr,
		GRPCAddr:    grpcAddr,
		DataDir:     "zuul-ui-test",
		Shards:      []uint64{1, 2},
		MetaShardID: 1_000_000,
		Members:     map[uint64]string{1: raftAddr},
		Seed:        map[uint64]string{1: grpcAddr},
		UIEnable:    true,
		UIBind:      uiAddr,
	})
	if err != nil {
		t.Fatalf("TestNodeUI: New: %s", err)
	}
	t.Cleanup(n.Close)
	context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(lis) })
	if err := n.Start(ctx); err != nil {
		t.Fatalf("TestNodeUI: Start: %s", err)
	}

	cl, err := client.New(ctx, client.Endpoints{grpcAddr}, client.WithClientID("c1"))
	if err != nil {
		t.Fatalf("TestNodeUI: client.New: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	// Acquire a lock so a record exists, retrying until the shard has a leader.
	var ok bool
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok, err = cl.NewMutex("/alice/lock").TryLock(ctx)
		if err == nil && ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("TestNodeUI: TryLock never succeeded: ok=%v err=%v", ok, err)
	}

	// The namespace view lists the record.
	if body := uiGet(t, "http://"+uiAddr+"/?ns=/alice"); !strings.Contains(body, "/alice/lock") {
		t.Errorf("TestNodeUI: namespace view missing /alice/lock:\n%s", body)
	}
	// The detail view shows the holder.
	detail := uiGet(t, "http://"+uiAddr+"/?ns=/alice&rec=/alice/lock")
	for _, want := range []string{"/alice/lock", "c1", "Detail"} {
		if !strings.Contains(detail, want) {
			t.Errorf("TestNodeUI: detail view missing %q", want)
		}
	}
}

// uiGet fetches url and returns the body, failing the test on error or non-200.
func uiGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("uiGet(%s): %s", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("uiGet(%s): read: %s", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("uiGet(%s): status %d: %s", url, resp.StatusCode, b)
	}
	return string(b)
}
