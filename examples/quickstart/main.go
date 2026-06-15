// Command quickstart boots a single in-process Zuul node and drives it with the Go
// client: it acquires and releases a lock (showing the fencing token), then runs a
// one-candidate leader election. Run it with:
//
//	go run ./examples/quickstart
//
// In production you would instead run one `zuuld` per machine and point the client
// at any of them; this demo just embeds a node so it runs with a single command.
package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/client"
	"github.com/johnsiilver/zuul/internal/node"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "quickstart:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	// Pick free ports for the Raft transport and the client-facing gRPC server.
	raftAddr, err := freePort()
	if err != nil {
		return err
	}
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	grpcAddr := grpcLis.Addr().String()

	// Assemble a single-node cluster (4 lock shards + the meta shard).
	bootCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := node.New(bootCtx, node.Config{
		ReplicaID:   1,
		RaftAddr:    raftAddr,
		GRPCAddr:    grpcAddr,
		DataDir:     "zuul-quickstart",
		Shards:      []uint64{1, 2, 3, 4},
		MetaShardID: 1_000_000,
		Members:     map[uint64]string{1: raftAddr},
		Seed:        map[uint64]string{1: grpcAddr},
	})
	if err != nil {
		return fmt.Errorf("boot node: %w", err)
	}
	defer n.Close()

	context.Pool(ctx).Submit(ctx, func() { _ = n.Serve(grpcLis) })
	if err := n.Start(ctx); err != nil {
		return fmt.Errorf("start node: %w", err)
	}
	fmt.Printf("zuul node serving on %s\n\n", grpcAddr)

	// Connect a client (this opens a session whose lease is kept alive for you).
	cl, err := client.New(ctx, client.Endpoints{grpcAddr}, client.WithClientID("demo"), client.WithTTL(10*time.Second))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer cl.Close()

	// --- Distributed lock ---
	// Keys are filesystem-like paths: /<user>/<dir.../><name>. The first segment is
	// the owner; "demo" here matches this client's identity.
	mu := cl.NewMutex("/demo/orders/42")
	ok, err := mu.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("trylock: %w", err)
	}
	fmt.Printf("lock /demo/orders/42: acquired=%v fencingToken=%d\n", ok, mu.Token())
	fmt.Println("  -> pass the fencing token to whatever the lock guards; a stale holder is rejected")
	if err := mu.Unlock(ctx); err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	fmt.Println("lock /demo/orders/42: released")

	// --- Leader election ---
	// The leader publishes its own address as an Endpoint, so any observer can dial
	// the elected master by election path with el.Master / el.FollowMaster.
	fmt.Println()
	host, portStr, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return fmt.Errorf("split grpc addr: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("parse grpc port: %w", err)
	}
	endpoint, err := client.MarshalEndpoint(&zuulv1.Endpoint{Host: host, Port: uint32(port)})
	if err != nil {
		return fmt.Errorf("marshal endpoint: %w", err)
	}
	el := cl.NewElection("/demo/workers/leader")
	if err := el.Campaign(ctx, endpoint, 0); err != nil {
		return fmt.Errorf("campaign: %w", err)
	}
	leader, err := el.Leader(ctx)
	if err != nil {
		return fmt.Errorf("leader: %w", err)
	}
	master, ok, err := el.Master(ctx)
	if err != nil {
		return fmt.Errorf("master: %w", err)
	}
	fmt.Printf("election /demo/workers/leader: leader=%q token=%d\n", leader.ID, leader.Token)
	if ok {
		fmt.Printf("  -> resolved master address %s (dial this to reach the elected leader)\n", master.Address())
	}
	if err := el.Resign(ctx); err != nil {
		return fmt.Errorf("resign: %w", err)
	}
	fmt.Println("election /demo/workers/leader: resigned")
	return nil
}

// freePort returns a free loopback address (the port is released for immediate use).
func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
