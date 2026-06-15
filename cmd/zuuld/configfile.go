package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/johnsiilver/zuul/internal/authz"
	"github.com/johnsiilver/zuul/internal/consensus"
)

// fileConfig is the JSON shape of --config. When --config is given it fully defines
// the node; the other command-line flags are ignored. Example:
//
//	{
//	  "id": 1,
//	  "raft": "10.0.0.1:9001",
//	  "grpc": "10.0.0.1:8001",
//	  "shards": 16,
//	  "peers": {
//	    "1": {"raft": "10.0.0.1:9001", "grpc": "10.0.0.1:8001"},
//	    "2": {"raft": "10.0.0.2:9001", "grpc": "10.0.0.2:8001"},
//	    "3": {"raft": "10.0.0.3:9001", "grpc": "10.0.0.3:8001"}
//	  },
//	  "mutualTLS": true, "tlsCA": "ca.pem", "tlsCert": "node.pem", "tlsKey": "key.pem",
//	  "aclFile": "/etc/zuul/acl"
//	}
type fileConfig struct {
	ID      uint64 `json:"id"`
	Raft    string `json:"raft"`
	GRPC    string `json:"grpc"`
	DataDir string `json:"dataDir"`
	Shards  int    `json:"shards"`
	Join    bool   `json:"join"`

	MutualTLS      bool     `json:"mutualTLS"`
	ServerTLS      bool     `json:"serverTLS"`
	PeerCAFile     string   `json:"peerCAFile"`
	PeerAllowedCNs []string `json:"peerAllowedCNs"`
	TLSCA          string   `json:"tlsCA"`
	TLSCert        string   `json:"tlsCert"`
	TLSKey         string   `json:"tlsKey"`

	OIDCIssuer        string `json:"oidcIssuer"`
	OIDCAudience      string `json:"oidcAudience"`
	OIDCIdentityClaim string `json:"oidcIdentityClaim"`

	Gossip      bool     `json:"gossip"`
	NodeHostID  uint64   `json:"nodeHostID"`
	GossipBind  string   `json:"gossipBind"`
	GossipSeeds []string `json:"gossipSeeds"`

	SnapshotEntries    uint64 `json:"snapshotEntries"`
	CompactionOverhead uint64 `json:"compactionOverhead"`
	MaxRecvBytes       int    `json:"maxRecvBytes"`

	RateLimitPerSec            float64 `json:"rateLimitPerSec"`
	RateBurst                  int     `json:"rateBurst"`
	PerIdentityRateLimitPerSec float64 `json:"perIdentityRateLimitPerSec"`
	PerIdentityRateBurst       int     `json:"perIdentityRateBurst"`

	ACLFile        string `json:"aclFile"`
	AuthTokensFile string `json:"authTokensFile"`

	// Peers maps replica id (as a JSON string key) to its addresses. In gossip mode
	// "raft" holds the peer's nodeHostID (a number) instead of an address.
	Peers map[string]peerEntry `json:"peers"`
}

// peerEntry is one initial member's addresses.
type peerEntry struct {
	Raft string `json:"raft"`
	GRPC string `json:"grpc"`
}

// loadConfigFile reads a JSON config file into the internal config, applying the
// same validation rules as the flags.
func loadConfigFile(path string) (config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return config{}, fmt.Errorf("--config: %w", err)
	}
	var fc fileConfig
	if err := json.Unmarshal(b, &fc); err != nil {
		return config{}, fmt.Errorf("--config %s: %w", path, err)
	}

	cfg := config{
		id:           fc.ID,
		raft:         fc.Raft,
		grpc:         fc.GRPC,
		dataDir:      fc.DataDir,
		shards:       fc.Shards,
		join:         fc.Join,
		mutualTLS:    fc.MutualTLS,
		serverTLS:    fc.ServerTLS,
		peerCAFile:   fc.PeerCAFile,
		peerCNs:      fc.PeerAllowedCNs,
		oidcIssuer:   fc.OIDCIssuer,
		oidcAud:      fc.OIDCAudience,
		oidcClaim:    fc.OIDCIdentityClaim,
		caFile:       fc.TLSCA,
		certFile:     fc.TLSCert,
		keyFile:      fc.TLSKey,
		gossip:       fc.Gossip,
		nodeHostID:   fc.NodeHostID,
		gossipBind:   fc.GossipBind,
		gossipSeeds:  fc.GossipSeeds,
		rateLimit:    fc.RateLimitPerSec,
		rateBurst:    fc.RateBurst,
		perIDRate:    fc.PerIdentityRateLimitPerSec,
		perIDBurst:   fc.PerIdentityRateBurst,
		snapEntries:  fc.SnapshotEntries,
		compaction:   fc.CompactionOverhead,
		maxRecvBytes: fc.MaxRecvBytes,
		members:      map[uint64]string{},
		seed:         map[uint64]string{},
	}
	if cfg.dataDir == "" {
		cfg.dataDir = "zuul"
	}
	if cfg.shards == 0 {
		cfg.shards = 16
	}

	for idStr, p := range fc.Peers {
		pid, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || pid == 0 {
			return config{}, fmt.Errorf("--config: peers key %q is not a valid replica id", idStr)
		}
		if cfg.gossip {
			nhid, err := strconv.ParseUint(p.Raft, 10, 64)
			if err != nil || nhid == 0 {
				return config{}, fmt.Errorf("--config: peers[%s].raft %q must be a nodeHostID in gossip mode", idStr, p.Raft)
			}
			cfg.members[pid] = consensus.GossipTarget(nhid)
		} else {
			cfg.members[pid] = p.Raft
		}
		cfg.seed[pid] = p.GRPC
	}

	if fc.ACLFile != "" {
		a, err := authz.LoadFile(fc.ACLFile)
		if err != nil {
			return config{}, err
		}
		cfg.authorizer = a
	}
	if fc.AuthTokensFile != "" {
		tokens, err := authz.LoadTokens(fc.AuthTokensFile)
		if err != nil {
			return config{}, err
		}
		cfg.tokens = tokens
	}

	switch {
	case cfg.id == 0:
		return config{}, fmt.Errorf("--config: id is required and must be non-zero")
	case cfg.raft == "":
		return config{}, fmt.Errorf("--config: raft is required")
	case cfg.grpc == "":
		return config{}, fmt.Errorf("--config: grpc is required")
	case cfg.shards < 1:
		return config{}, fmt.Errorf("--config: shards must be at least 1")
	case (cfg.mutualTLS || cfg.serverTLS) && (cfg.caFile == "" || cfg.certFile == "" || cfg.keyFile == ""):
		return config{}, fmt.Errorf("--config: mutualTLS/serverTLS require tlsCA, tlsCert, and tlsKey")
	case cfg.gossip && (cfg.nodeHostID == 0 || cfg.gossipBind == "" || len(cfg.gossipSeeds) == 0):
		return config{}, fmt.Errorf("--config: gossip requires nodeHostID, gossipBind, and gossipSeeds")
	case !cfg.join && len(cfg.members) == 0:
		return config{}, fmt.Errorf("--config: peers is required unless join is set")
	}
	return cfg, nil
}
