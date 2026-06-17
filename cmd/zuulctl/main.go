// Command zuulctl is a small admin/introspection CLI for a Zuul cluster. It talks to
// any node's gRPC address and drives the read paths of the Cluster, Locker, and
// Election APIs plus the membership-change admin calls.
//
//	zuulctl --addr 10.0.0.1:8001 members
//	zuulctl --addr 10.0.0.1:8001 status orders/42
//	zuulctl --addr 10.0.0.1:8001 add-node 4 10.0.0.4:9001 10.0.0.4:8001
//
// Add --mutual-tls --tls-ca/-cert/-key to talk to an mTLS cluster.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/zuultls"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

func main() {
	addr := flag.String("addr", "", "a Zuul node's gRPC address host:port (required)")
	mtls := flag.Bool("mutual-tls", false, "use mutual TLS")
	caFile := flag.String("tls-ca", "", "CA certificate PEM (with --mutual-tls)")
	certFile := flag.String("tls-cert", "", "client certificate PEM (with --mutual-tls)")
	keyFile := flag.String("tls-key", "", "client key PEM (with --mutual-tls)")
	flag.Usage = usage
	flag.Parse()

	if *addr == "" {
		fail("--addr is required")
	}
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	dialOpts, err := dialOptions(*mtls, *caFile, *certFile, *keyFile)
	if err != nil {
		fail(err.Error())
	}
	conn, err := grpc.NewClient(*addr, dialOpts...)
	if err != nil {
		fail("dial: " + err.Error())
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, conn, args); err != nil {
		fail(err.Error())
	}
}

// run dispatches the subcommand.
func run(ctx context.Context, conn *grpc.ClientConn, args []string) error {
	cmd, rest := args[0], args[1:]
	cluster := zuulv1.NewClusterClient(conn)
	switch cmd {
	case "members":
		return members(ctx, cluster)
	case "shards":
		return shards(ctx, cluster)
	case "health":
		return health(ctx, cluster)
	case "status":
		if len(rest) != 1 {
			return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("usage: status <key>"))
		}
		return status(ctx, zuulv1.NewLockerClient(conn), rest[0])
	case "leader":
		if len(rest) != 1 {
			return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("usage: leader <name>"))
		}
		return leader(ctx, zuulv1.NewElectionClient(conn), rest[0])
	case "add-node":
		if len(rest) != 3 {
			return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("usage: add-node <replicaID> <raftAddr> <grpcAddr>"))
		}
		return addNode(ctx, cluster, rest[0], rest[1], rest[2])
	case "remove-node":
		if len(rest) != 1 {
			return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("usage: remove-node <replicaID>"))
		}
		return removeNode(ctx, cluster, rest[0])
	default:
		return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("unknown command %q (run with no command for help)", cmd))
	}
}

func members(ctx context.Context, c zuulv1.ClusterClient) error {
	resp, err := c.Members(ctx, &zuulv1.MembersRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "REPLICA\tRAFT\tGRPC\tNODEHOSTID")
	for _, m := range resp.GetMembers() {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", m.GetReplicaId(), m.GetRaftAddress(), m.GetZuulGrpcAddress(), m.GetNodeHostId())
	}
	return tw.Flush()
}

func shards(ctx context.Context, c zuulv1.ClusterClient) error {
	resp, err := c.Shards(ctx, &zuulv1.ShardsRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SHARD\tLEADER")
	for _, s := range resp.GetShards() {
		fmt.Fprintf(tw, "%d\t%d\n", s.GetShardId(), s.GetLeaderReplicaId())
	}
	return tw.Flush()
}

func health(ctx context.Context, c zuulv1.ClusterClient) error {
	resp, err := c.Health(ctx, &zuulv1.HealthRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("healthy=%v shards=%d led=%d\n", resp.GetHealthy(), resp.GetShardCount(), resp.GetLeaderCount())
	return nil
}

func status(ctx context.Context, c zuulv1.LockerClient, key string) error {
	resp, err := c.Status(ctx, &zuulv1.StatusRequest{Name: key})
	if err != nil {
		return err
	}
	fmt.Printf("key=%s held=%v holder=%s fencingToken=%d queueDepth=%d revision=%d\n",
		key, resp.GetHeld(), resp.GetHolderClientId(), resp.GetFencingToken(), resp.GetQueueDepth(), resp.GetRevision())
	return nil
}

func leader(ctx context.Context, c zuulv1.ElectionClient, name string) error {
	resp, err := c.Leader(ctx, &zuulv1.LeaderRequest{Name: name})
	if err != nil {
		return err
	}
	fmt.Printf("election=%s hasLeader=%v leader=%s fencingToken=%d value=%q revision=%d\n",
		name, resp.GetHasLeader(), resp.GetLeaderClientId(), resp.GetFencingToken(), resp.GetValue(), resp.GetRevision())
	return nil
}

func addNode(ctx context.Context, c zuulv1.ClusterClient, idStr, raftAddr, grpcAddr string) error {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("replicaID %q is not a number", idStr))
	}
	if _, err := c.AddNode(ctx, &zuulv1.AddNodeRequest{ReplicaId: id, RaftAddress: raftAddr, ZuulGrpcAddress: grpcAddr}); err != nil {
		return err
	}
	fmt.Printf("added node %d (raft %s, grpc %s); now start it with --join\n", id, raftAddr, grpcAddr)
	return nil
}

func removeNode(ctx context.Context, c zuulv1.ClusterClient, idStr string) error {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return errors.E(ctx, errors.CatRequest, errors.TypeBadRequest, fmt.Errorf("replicaID %q is not a number", idStr))
	}
	if _, err := c.RemoveNode(ctx, &zuulv1.RemoveNodeRequest{ReplicaId: id}); err != nil {
		return err
	}
	fmt.Printf("removed node %d\n", id)
	return nil
}

// dialOptions builds the gRPC dial options (insecure, or mutual TLS).
func dialOptions(mtls bool, caFile, certFile, keyFile string) ([]grpc.DialOption, error) {
	if !mtls {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
	}
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--mutual-tls requires --tls-ca, --tls-cert, and --tls-key")
	}
	cc, err := zuultls.ClientConfig(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(cc))}, nil
}

func usage() {
	fmt.Fprint(os.Stderr, `zuulctl --addr host:port [--mutual-tls --tls-ca/-cert/-key] <command> [args]

commands:
  members                              list cluster members and their addresses
  shards                               per-shard leader map
  health                               this node's health and shards led
  status <key>                         a lock's holder and queue depth
  leader <name>                        an election's current leader and value
  add-node <replicaID> <raft> <grpc>   admit a new node (then start it with --join)
  remove-node <replicaID>              remove a node from the cluster
`)
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "zuulctl:", msg)
	os.Exit(1)
}
