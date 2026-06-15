package integration

import (
	"testing"
	"time"

	"github.com/gostdlib/base/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/johnsiilver/zuul/client"
	"github.com/johnsiilver/zuul/internal/zuultls"
	"github.com/johnsiilver/zuul/internal/zuultls/zuultlstest"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// genCerts creates a throwaway CA and one loopback node certificate (used by every
// node and the client), writes them as PEM files, and returns their paths.
func genCerts(t *testing.T) *tlsPaths {
	t.Helper()
	ca, cert, key := zuultlstest.GenCerts(t)
	return &tlsPaths{ca: ca, cert: cert, key: key}
}

// TestMutualTLS proves the cluster runs end-to-end under mutual TLS on all three
// planes (Raft, forward, client), and that an unauthenticated client is rejected.
func TestMutualTLS(t *testing.T) {
	certs := genCerts(t)
	c := newSecureCluster(t, 3, 4, certs)
	ctx := t.Context()

	// A forwarded write succeeds over the mTLS forward plane.
	const key = "/test/tls-forward-key"
	leaderID := c.leaderReplica(t, c.router.Shard(key))
	entry := c.otherNode(leaderID)
	openSession(entry, "c1")
	got, err := entry.srv.TryLock(ctx, &zuulv1.TryLockRequest{Name: key, ClientId: "c1"})
	if err != nil {
		t.Fatalf("TestMutualTLS: forwarded TryLock over mTLS: %s", err)
	}
	if !got.GetAcquired() {
		t.Fatalf("TestMutualTLS: forwarded TryLock: acquired=false, want true")
	}

	// The Go client connects over mTLS and uses a Mutex.
	clientTLS, err := zuultls.ClientConfig(certs.ca, certs.cert, certs.key)
	if err != nil {
		t.Fatalf("TestMutualTLS: client TLS config: %s", err)
	}
	creds := grpc.WithTransportCredentials(credentials.NewTLS(clientTLS))
	cl, err := client.New(ctx, client.Endpoints{c.grpcAddrs[c.nodes[0].replicaID]}, client.WithClientID("alice"), client.WithDialOptions(creds))
	if err != nil {
		t.Fatalf("TestMutualTLS: client Dial over mTLS: %s", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	mu := cl.NewMutex("/test/tls-client-lock")
	ok, err := mu.TryLock(ctx)
	if err != nil {
		t.Fatalf("TestMutualTLS: client TryLock over mTLS: %s", err)
	}
	if !ok {
		t.Errorf("TestMutualTLS: client TryLock: acquired=false, want true")
	}

	// An unauthenticated (insecure) client is rejected by the mTLS server.
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	bad, err := client.New(dialCtx, client.Endpoints{c.grpcAddrs[c.nodes[0].replicaID]}, client.WithClientID("intruder"))
	if err == nil {
		_ = bad.Close()
		t.Errorf("TestMutualTLS: insecure client connected to an mTLS server, want rejection")
	}
}
