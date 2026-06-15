// Command zuuld runs one Zuul node: an in-memory, multi-master, sharded
// distributed lock + leader-election server. It assembles the node (lock + meta
// shards, forward dispatcher, session manager, gRPC services) and serves until
// interrupted.
//
// Example 3-node bootstrap (each node started with its own --id):
//
//	zuuld --id 1 --raft 10.0.0.1:9001 --grpc 10.0.0.1:8001 \
//	  --peers 1=10.0.0.1:9001=10.0.0.1:8001,2=10.0.0.2:9001=10.0.0.2:8001,3=10.0.0.3:9001=10.0.0.3:8001
//
// A node added to a running cluster (after Cluster.AddNode) is started with --join
// and no --peers. Add --mutual-tls --tls-ca/-cert/-key to encrypt every plane.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/consensus"
	"github.com/johnsiilver/zuul/internal/discovery"
	"github.com/johnsiilver/zuul/internal/node"
)

// metaShard is the reserved Raft group id for the topology (meta) shard, kept clear
// of the lock shard ids (1..shards).
const metaShard = uint64(1_000_000)

// bootTimeout bounds how long the node waits for its shards to elect leaders at
// startup (peers may still be coming up).
const bootTimeout = 60 * time.Second

// drainTimeout bounds graceful leadership transfer at shutdown.
const drainTimeout = 10 * time.Second

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "zuuld: %s\n", err)
		os.Exit(2)
	}

	ctx := context.Background()
	log := context.Log(ctx)

	shards := make([]uint64, cfg.shards)
	for i := range shards {
		shards[i] = uint64(i + 1)
	}

	bootCtx, cancelBoot := context.WithTimeout(ctx, bootTimeout)
	defer cancelBoot()
	n, err := node.New(bootCtx, node.Config{
		ReplicaID:      cfg.id,
		RaftAddr:       cfg.raft,
		GRPCAddr:       cfg.grpc,
		DataDir:        cfg.dataDir,
		Shards:         shards,
		MetaShardID:    metaShard,
		Members:        cfg.members,
		Seed:           cfg.seed,
		Join:           cfg.join,
		MutualTLS:      cfg.mutualTLS,
		ServerTLS:      cfg.serverTLS,
		PeerCAFile:     cfg.peerCAFile,
		PeerAllowedCNs: cfg.peerCNs,
		CAFile:         cfg.caFile,
		CertFile:       cfg.certFile,
		KeyFile:        cfg.keyFile,
		Gossip:         cfg.gossip,
		NodeHostID:     cfg.nodeHostID,
		GossipBind:     cfg.gossipBind,
		GossipSeeds:    cfg.gossipSeeds,
		Authorizer:     cfg.authorizer,
		Tokens:         cfg.tokens,

		OIDCIssuer:        cfg.oidcIssuer,
		OIDCAudience:      cfg.oidcAud,
		OIDCIdentityClaim: cfg.oidcClaim,

		RateLimitPerSec:            cfg.rateLimit,
		RateBurst:                  cfg.rateBurst,
		PerIdentityRateLimitPerSec: cfg.perIDRate,
		PerIdentityRateBurst:       cfg.perIDBurst,
		SnapshotEntries:            cfg.snapEntries,
		CompactionOverhead:         cfg.compaction,
		MaxRecvBytes:               cfg.maxRecvBytes,
	})
	if err != nil {
		fatal(log, "assemble node", err)
	}

	listenAddr := cfg.grpcListen
	if listenAddr == "" {
		listenAddr = cfg.grpc
	}
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fatal(log, "listen", err)
	}
	context.Pool(ctx).Submit(ctx, func() {
		if err := n.Serve(lis); err != nil {
			log.Error("grpc server stopped", "err", err.Error())
		}
	})
	if err := n.Start(ctx); err != nil {
		fatal(log, "start node", err)
	}

	log.Info("zuuld serving", "replica", cfg.id, "grpc", cfg.grpc, "raft", cfg.raft, "shards", cfg.shards, "join", cfg.join, "mutualTLS", cfg.mutualTLS, "serverTLS", cfg.serverTLS, "oidc", cfg.oidcIssuer != "", "gossip", cfg.gossip)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Info("zuuld draining (transferring shard leadership)")
	drainCtx, cancelDrain := context.WithTimeout(ctx, drainTimeout)
	n.Drain(drainCtx)
	cancelDrain()

	log.Info("zuuld shutting down")
	n.Close()
}

// splitNonEmpty splits a comma list, dropping empty fields.
func splitNonEmpty(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// fatal logs and exits non-zero.
func fatal(log interface{ Error(string, ...any) }, what string, err error) {
	log.Error("zuuld: "+what, "err", err.Error())
	os.Exit(1)
}

// config is the parsed flag set.
type config struct {
	id           uint64
	raft         string
	grpc         string // advertised client/forwarding address (recorded in meta)
	grpcListen   string // bind address for the gRPC listener; empty => same as grpc
	dataDir      string
	shards       int
	join         bool
	mutualTLS    bool
	serverTLS    bool
	peerCAFile   string
	peerCNs      []string
	oidcIssuer   string
	oidcAud      string
	oidcClaim    string
	caFile       string
	certFile     string
	keyFile      string
	gossip       bool
	nodeHostID   uint64
	gossipBind   string
	gossipSeeds  []string
	authorizer   authz.Authorizer
	tokens       map[string]string // bearer token -> identity (token auth; nil disables)
	rateLimit    float64
	rateBurst    int
	perIDRate    float64
	perIDBurst   int
	snapEntries  uint64
	compaction   uint64
	maxRecvBytes int               // max inbound gRPC message size (0 = default 1 MiB)
	members      map[uint64]string // replica id -> raft address or nhid target (bootstrap)
	seed         map[uint64]string // replica id -> grpc address (forwarding seed)
}

// parseFlags reads and validates the command-line flags.
func parseFlags() (config, error) {
	var (
		id      = flag.Uint64("id", 0, "this node's replica id (required, non-zero)")
		raft    = flag.String("raft", "", "Raft transport address host:port (required)")
		grpcA   = flag.String("grpc", "", "client/forwarding gRPC address host:port (required)")
		dataDir = flag.String("data-dir", "zuul", "NodeHost dir name (in-memory; identity only)")
		shards  = flag.Int("shards", 16, "number of lock shards")
		join    = flag.Bool("join", false, "join an existing cluster (must be AddNode'd first)")
		peers   = flag.String("peers", "", "comma list of id=raftAddr=grpcAddr for initial members")
		mtls    = flag.Bool("mutual-tls", false, "enable mutual TLS on the Raft, forward, and client planes")
		stls    = flag.Bool("server-tls", false, "server-authenticated TLS for clients (no client certs; pair with token/OIDC auth); nodes still use certs between themselves")
		peerCA  = flag.String("peer-ca", "", "CA that signs node certificates; restricts the inter-node forward plane to certs from this CA (recommended when clients and nodes share a CA)")
		peerCNs = flag.String("peer-allowed-cns", "", "comma list of node certificate Common Names allowed on the forward plane (positive allowlist on top of --peer-ca)")
		oidcIss = flag.String("oidc-issuer", "", "OIDC issuer URL for bearer-token auth, e.g. https://login.microsoftonline.com/<tenant>/v2.0 (Azure Entra/MSI)")
		oidcAud = flag.String("oidc-audience", "", "audience tokens must carry (required with --oidc-issuer)")
		oidcClm = flag.String("oidc-identity-claim", "", `claim used as the authz identity (default "sub"; Azure often "oid")`)
		caFile  = flag.String("tls-ca", "", "CA certificate PEM (required with --mutual-tls)")
		certF   = flag.String("tls-cert", "", "node certificate PEM (required with --mutual-tls)")
		keyF    = flag.String("tls-key", "", "node key PEM (required with --mutual-tls)")
		gossip  = flag.Bool("gossip", false, "address nodes by NodeHostID over gossip (--mutual-tls recommended but optional; --peers ids become id=nodeHostID=grpcAddr)")
		nhid    = flag.Uint64("node-host-id", 0, "this node's stable gossip identity (required with --gossip)")
		gBind   = flag.String("gossip-bind", "", "this node's gossip bind/advertise address (required with --gossip)")
		gSeeds  = flag.String("gossip-seeds", "", "comma list of peer gossip addresses (required with --gossip)")
		disc    = flag.String("discovery", "", `peer discovery mode: "" (use --id/--raft/--grpc/--peers) or "k8s" (StatefulSet DNS)`)
		k8sName = flag.String("k8s-name", "", "StatefulSet name / pod-name prefix (--discovery k8s)")
		k8sSvc  = flag.String("k8s-service", "", "headless Service name (--discovery k8s; default --k8s-name)")
		k8sNS   = flag.String("k8s-namespace", "", "pod namespace (--discovery k8s)")
		k8sDom  = flag.String("k8s-domain", "cluster.local", "cluster DNS domain (--discovery k8s)")
		k8sReps = flag.Int("k8s-replicas", 0, "StatefulSet replica count (--discovery k8s)")
		k8sRaft = flag.Int("k8s-raft-port", 0, "uniform Raft port (--discovery k8s)")
		k8sGRPC = flag.Int("k8s-grpc-port", 0, "uniform gRPC port (--discovery k8s)")
		podName = flag.String("pod-name", podNameDefault(), "this pod's name, e.g. zuul-2 (--discovery k8s; defaults to $HOSTNAME)")
		aclFile = flag.String("acl-file", "", "per-identity key ACL file (requires --mutual-tls or token auth to identify clients); default allow-all")
		tokFile = flag.String("auth-tokens-file", "", "bearer-token auth file of 'token identity' lines; enables token authentication")
		rlRate  = flag.Float64("rate-limit", 0, "global client requests/sec cap (0 = unlimited)")
		rlBurst = flag.Int("rate-burst", 0, "global rate-limit burst (default rate+1)")
		piRate  = flag.Float64("per-identity-rate-limit", 0, "per-identity requests/sec cap (0 = unlimited)")
		piBurst = flag.Int("per-identity-rate-burst", 0, "per-identity burst (default rate+1)")
		snapEnt = flag.Uint64("snapshot-entries", 0, "Raft entries per snapshot/compaction (0 = default 10000)")
		compact = flag.Uint64("compaction-overhead", 0, "Raft entries retained after compaction (0 = default 100)")
		maxRecv = flag.Int("max-recv-bytes", 0, "max inbound gRPC message size in bytes, including a published election value (0 = default 1 MiB)")
		cfgFile = flag.String("config", "", "JSON config file that fully defines the node (other flags are ignored)")
	)
	flag.Parse()

	if *cfgFile != "" {
		return loadConfigFile(*cfgFile)
	}

	cfg := config{
		dataDir:      *dataDir,
		shards:       *shards,
		join:         *join,
		mutualTLS:    *mtls,
		serverTLS:    *stls,
		peerCAFile:   *peerCA,
		peerCNs:      splitNonEmpty(*peerCNs),
		oidcIssuer:   *oidcIss,
		oidcAud:      *oidcAud,
		oidcClaim:    *oidcClm,
		caFile:       *caFile,
		certFile:     *certF,
		keyFile:      *keyF,
		gossip:       *gossip,
		nodeHostID:   *nhid,
		gossipBind:   *gBind,
		rateLimit:    *rlRate,
		rateBurst:    *rlBurst,
		perIDRate:    *piRate,
		perIDBurst:   *piBurst,
		snapEntries:  *snapEnt,
		compaction:   *compact,
		maxRecvBytes: *maxRecv,
		members:      map[uint64]string{},
		seed:         map[uint64]string{},
	}
	cfg.gossipSeeds = splitNonEmpty(*gSeeds)

	// Common validation (applies to every discovery mode).
	switch {
	case cfg.shards < 1:
		return config{}, fmt.Errorf("--shards must be at least 1")
	case (cfg.mutualTLS || cfg.serverTLS) && (cfg.caFile == "" || cfg.certFile == "" || cfg.keyFile == ""):
		return config{}, fmt.Errorf("--mutual-tls/--server-tls require --tls-ca, --tls-cert, and --tls-key")
	}
	// Auth/TLS coupling (mTLS xor serverTLS, OIDC pairing, bearer-needs-TLS,
	// ACL-needs-identity) is validated authoritatively by node.New.

	if *aclFile != "" {
		a, err := authz.LoadFile(*aclFile)
		if err != nil {
			return config{}, err
		}
		cfg.authorizer = a
	}
	if *tokFile != "" {
		tokens, err := authz.LoadTokens(*tokFile)
		if err != nil {
			return config{}, err
		}
		cfg.tokens = tokens
	}

	switch *disc {
	case "k8s":
		if cfg.gossip {
			return config{}, fmt.Errorf("--discovery k8s uses stable DNS and is incompatible with --gossip")
		}
		res, err := discovery.K8s{
			Name:          *k8sName,
			Service:       *k8sSvc,
			Namespace:     *k8sNS,
			ClusterDomain: *k8sDom,
			Replicas:      *k8sReps,
			RaftPort:      *k8sRaft,
			GRPCPort:      *k8sGRPC,
			PodName:       *podName,
		}.Resolve()
		if err != nil {
			return config{}, err
		}
		cfg.id, cfg.raft, cfg.grpc = res.ReplicaID, res.RaftAddr, res.GRPCAddr
		cfg.members, cfg.seed = res.Members, res.Seed
		// Advertise the pod DNS name to peers/clients, but bind the listener on all
		// interfaces: the advertised name resolves to one pod IP, so binding to it
		// would miss localhost (port-forward) and any other interface.
		cfg.grpcListen = fmt.Sprintf(":%d", *k8sGRPC)
	case "":
		cfg.id, cfg.raft, cfg.grpc = *id, *raft, *grpcA
		switch {
		case cfg.id == 0:
			return config{}, fmt.Errorf("--id is required and must be non-zero")
		case cfg.raft == "":
			return config{}, fmt.Errorf("--raft is required")
		case cfg.grpc == "":
			return config{}, fmt.Errorf("--grpc is required")
		case cfg.gossip && (cfg.nodeHostID == 0 || cfg.gossipBind == "" || len(cfg.gossipSeeds) == 0):
			return config{}, fmt.Errorf("--gossip requires --node-host-id, --gossip-bind, and --gossip-seeds")
		}
		if err := parsePeers(*peers, &cfg); err != nil {
			return config{}, err
		}
		if !cfg.join && len(cfg.members) == 0 {
			return config{}, fmt.Errorf("--peers is required unless --join is set")
		}
	default:
		return config{}, fmt.Errorf("unknown --discovery mode %q (use \"\" or \"k8s\")", *disc)
	}
	return cfg, nil
}

// podNameDefault returns the pod name from the environment ($HOSTNAME, then
// $POD_NAME), used as the default for --pod-name in Kubernetes.
func podNameDefault() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	return os.Getenv("POD_NAME")
}

// parsePeers fills cfg.members and cfg.seed from the --peers list. Each entry is
// "id=raftAddr=grpcAddr" in static mode, or "id=nodeHostID=grpcAddr" in gossip mode
// (where the dragonboat member target is the peer's NodeHostID).
func parsePeers(peers string, cfg *config) error {
	if strings.TrimSpace(peers) == "" {
		return nil
	}
	for _, entry := range strings.Split(peers, ",") {
		parts := strings.Split(strings.TrimSpace(entry), "=")
		if len(parts) != 3 {
			return fmt.Errorf("--peers entry %q must be id=%s=grpcAddr", entry, peerTargetLabel(cfg.gossip))
		}
		pid, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || pid == 0 {
			return fmt.Errorf("--peers entry %q has an invalid id", entry)
		}
		if cfg.gossip {
			nhid, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil || nhid == 0 {
				return fmt.Errorf("--peers entry %q has an invalid nodeHostID", entry)
			}
			cfg.members[pid] = consensus.GossipTarget(nhid)
		} else {
			cfg.members[pid] = parts[1]
		}
		cfg.seed[pid] = parts[2]
	}
	return nil
}

// peerTargetLabel names the middle --peers field for error messages.
func peerTargetLabel(gossip bool) string {
	if gossip {
		return "nodeHostID"
	}
	return "raftAddr"
}
