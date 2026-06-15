# Zuul

> *"There is no Dana, only Zuul."* — a gatekeeper for your distributed resources.

Zuul is an **in-memory, multi-master, sharded distributed lock and leader-election
service**, in the spirit of etcd but built for speed. Lock state lives only in RAM
(never fsync'd to disk), locks are correct under failure (consensus-backed,
lease-bound, fenced), and clients can read or write through **any** node.

```
client ──gRPC──▶ any node ──┬─ leads the key's shard? apply locally
                            └─ else forward to the shard's leader node
```

## Why it's correct

Locks require **linearizable consensus** — an eventually-consistent "multi-master"
mutex cannot guarantee mutual exclusion. Zuul partitions the keyspace into *M* Raft
groups (shards); each shard elects its own leader and leaders spread across nodes, so
you get the practical benefit of multi-master (no single write bottleneck, every node
leads some shards) while every individual lock's operations stay **single-shard and
linearizable**. This is the Spanner / CockroachDB / AWS-Physalia pattern. A Porcupine
linearizability test proves the headline property: **the lock is never held by two
clients at once.**

## Features

- **Distributed locks** — blocking / bounded-wait / try-lock, FIFO-fair queueing.
- **Fencing tokens** — every acquisition returns a monotonic token; a paused-then-
  resumed client can't act on a lock that has moved on (Kleppmann safety).
- **Leader election** — `Campaign` / `Proclaim` / `Leader` / `Observe` / `Resign`.
- **Lease-bound sessions** — a client keeps one lease alive; on disconnect its locks
  release automatically. Leader-driven, time-flows-through-the-Raft-log expiry.
- **Multi-master** — read or write any key through any node; writes forward to the
  shard leader transparently.
- **Runtime reconfiguration** — grow, shrink, and heal the cluster (`AddNode` /
  `RemoveNode`, amnesiac-restart re-add) with no downtime.
- **In-memory only** — Raft log + snapshots live in RAM; a full-cluster restart is
  intentionally amnesiac (stale locks after a total outage are worse than empty).
- **Mutual TLS** — optional on every plane (Raft, node-to-node forwarding, clients). Off by default;
  enable it when you want it. Recommended (but not forced) with gossip, since gossip is unauthenticated.
- **Path-based keys + ACLs** — keys are filesystem-like paths (`/<user>/<dir.../><name>`);
  a per-identity ACL file grants read or read-write access, with each principal owning
  its own `/<user>/` home subtree. See [Path-based keys and ACLs](#path-based-keys-and-acls).

## Quickstart

Run an embedded single node and drive it with the client in one command:

```bash
go run ./examples/quickstart
```

```
zuul node serving on 127.0.0.1:61988

lock /demo/orders/42: acquired=true fencingToken=1
  -> pass the fencing token to whatever the lock guards; a stale holder is rejected
lock /demo/orders/42: released

election /demo/workers/leader: leader="demo" token=1
  -> resolved master address 127.0.0.1:61988 (dial this to reach the elected leader)
election /demo/workers/leader: resigned
```

## Running a cluster

Each machine runs one `zuuld`. Start the initial members together, each with its own
`--id`, all sharing the same `--peers` list (`id=raftAddr=grpcAddr,...`):

```bash
zuuld --id 1 --raft 10.0.0.1:9001 --grpc 10.0.0.1:8001 \
  --peers 1=10.0.0.1:9001=10.0.0.1:8001,2=10.0.0.2:9001=10.0.0.2:8001,3=10.0.0.3:9001=10.0.0.3:8001

# ...the same --peers on nodes 2 and 3, with --id 2 / --id 3.
```

Grow the cluster at runtime: call `Cluster.AddNode` on any node, then start the new
node with `--join` (and no `--peers`). Add mutual TLS to every plane with
`--mutual-tls --tls-ca ca.pem --tls-cert node.pem --tls-key node-key.pem`.

**On Kubernetes** (StatefulSet behind a headless Service), skip `--id/--raft/--grpc/
--peers` and let each pod discover its peers from stable DNS — no Kubernetes API
access needed:

```bash
zuuld --discovery k8s --k8s-name zuul --k8s-namespace prod --k8s-replicas 3 \
  --k8s-raft-port 9001 --k8s-grpc-port 8001   # pod identity from $HOSTNAME
```

Each pod derives its replica id from its ordinal (`zuul-2` → replica 3) and addresses
peers by name (`zuul-0.zuul.prod.svc.cluster.local:9001`, …); dragonboat and gRPC
re-resolve those names as pods restart. For dynamic-IP fleets without stable DNS, use
gossip instead: `--gossip --node-host-id N --gossip-bind … --gossip-seeds …`.

## Using the Go client

```go
import (
    "context"
    "time"

    "github.com/johnsiilver/zuul/client"
)

cl, err := client.New(context.Background(), client.Endpoints{"10.0.0.1:8001"}, client.WithClientID("worker-1"))
// (the library keeps the session's lease alive in the background until Close)
defer cl.Close()

// Keys are filesystem-like paths: /<user>/<dir.../><name>. The first segment is the
// owner. A principal automatically has read-write on its own /<user>/ subtree; see
// "Path-based keys and ACLs" below.

// --- Distributed lock ---
mu := cl.NewMutex("/worker-1/orders/42")
if err := mu.Lock(ctx, 5*time.Second); err != nil { // blocks up to 5s
    // ErrNotAcquired on timeout
}
defer mu.Unlock(ctx)
useResource(mu.Token()) // pass the fencing token to whatever the lock guards

// --- Leader election ---
el := cl.NewElection("/worker-1/leader")
if err := el.Campaign(ctx, []byte("worker-1"), 0); err != nil { /* ... */ } // blocks until leader
defer el.Resign(ctx)

events, _ := el.Observe(ctx) // current leader, then every change
for ev := range events {
    log.Printf("leader is now %q (value %q)", ev.ID, ev.Value)
}
```

### Dialing the elected master

An election leader is often a "master" that other clients need to reach. Have the
leader publish its address as its election value (an `Endpoint`), and any client can
resolve the current master by election path — and keep following it across failovers:

```go
import zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"

// The master publishes where to reach it when it campaigns (or via Proclaim). Host
// may be an IP literal or a DNS name — e.g. "db.prod.svc.cluster.local".
value, _ := client.MarshalEndpoint(&zuulv1.Endpoint{Host: "10.0.0.4", Port: 8443})
el.Campaign(ctx, value, 0)

// An observer resolves the current master to a dialable host:port...
m, ok, _ := el.Master(ctx) // ok=false while leaderless
if ok {
    conn, _ := grpc.NewClient(m.Address(), /* creds */) // m.Address() == "10.0.0.4:8443"
    _ = conn
}

// ...or follows it: f.Current() always points at the live master, and f.Updates()
// delivers each new address as leadership moves.
f, _ := el.FollowMaster(ctx)
defer f.Close()
for m := range f.Updates() {
    log.Printf("master is now %s", m.Address())
}
```

`Endpoint.host` accepts a DNS name or an IP literal, so a `dns:port` master resolves
and dials the same way as an IP one. `Endpoint.metadata` is a `google.protobuf.Any`,
so a master can attach any typed payload (region, capabilities, a second address)
alongside its `host`/`port`.

### Path-based keys and ACLs

Every lock and election key is a filesystem-like path, `/<user>/<dir.../><name>`;
non-path keys are rejected. The first segment is the **owner**. Authorization has
two access levels — **read** (read values: `Status` / `Leader` / `Observe`) and
**read-write** (participate: `Lock` / `Campaign` / … plus read). There is no
write-only.

The **principal** comes from the authenticating method that exposes it — the mTLS
certificate CN, the OIDC identity claim, or a token's mapped identity. A principal
automatically has read-write on its own `/<principal>/` subtree (its home
directory); the ACL file grants only additional cross-user access:

```
# identity   pathPrefix        mode   (r = read, rw = read-write; rwa adds cluster admin)
bob          /alice/configs/   r
svc-orders   /shared/orders/   rw
```

Load it with `zuuld --acl-file /etc/zuul/acl`. When no authenticating method is
configured (e.g. an insecure/dev deployment), the client names its principal with
`client.WithUser("alice")`, sent as the unauthenticated `zuul-user` header — the ACL
is then advisory. When a method *does* authenticate the caller, a `WithUser` value
must match the authenticated identity or the request is rejected.

For mutual TLS, pass dial credentials via `client.WithDialOptions`.

### Message size limit (election values)

An election value (`Campaign` / `Proclaim`, e.g. a published `Endpoint`) is bounded
by the node's inbound gRPC message size, **1 MiB by default**. The limit covers the
whole request — the value plus the key path, client id, and protobuf framing — so the
usable value is a touch under 1 MiB. Locks store no value. Raise or lower it with:

```bash
zuuld --max-recv-bytes 4194304   # 4 MiB; or "maxRecvBytes": 4194304 in --config JSON
```

The same limit guards the node-to-node forward plane, so set it consistently on every
node. (Dragonboat's Raft entry cap of 64 MiB sits well above this, so gRPC is the
binding limit.)

## Architecture

- **Multi-group sharded Raft** over [`dragonboat`](https://github.com/lni/dragonboat)
  (rehomed into `internal/dragonboat`, audited, patched for IPv6/`net/netip`, cgo
  removed). `shardID = xxhash64(key) % M`; each shard is a Raft group replicated on
  every node.
- **Per-shard FSM** (`internal/fsm`) — pure, deterministic, exhaustively tested: locks,
  FIFO queues, fencing tokens, and lease grant/keepalive/expire. Time is leader-stamped
  into commands so apply stays identical on every replica.
- **Writes** propose on the shard's leader — local if this node leads it, else proxied
  over the node-to-node **forward plane** (`internal/forward`) with exponential-backoff
  retry through leader churn. **Reads** run linearizably (ReadIndex) on any node.
- **Meta shard** (`internal/meta`) — a small replicated topology map (replica id →
  addresses) so any node can resolve a leader's address, including for nodes added at
  runtime. Backs the `Cluster` admin API and dynamic membership.
- **Sessions** (`internal/session`) — per-client, per-shard leases created lazily;
  one heartbeat fans renewals out to the shards a client actually touches.
- **Two protobuf transport planes**: dragonboat's Raft replication (framed TCP, `raftpb`)
  and Zuul's forward plane (gRPC, vtproto-marshaled `fsmpb`/`metapb` bytes). The
  client-facing API is a third gRPC plane (`proto/zuul/v1`). All three support mutual TLS.

## Status

Working and tested end-to-end (in-process multi-node clusters): forwarded writes,
cross-node fencing, **leader failover keeps locks**, dynamic grow/shrink/amnesiac
re-add, leader election with streaming `Observe`, Porcupine linearizability (including
**through a leader failover mid-contention**), mutual TLS (with an unauthenticated
client provably rejected), and both **static and gossip (NodeHostID)** addressing.
Ships a Go client, the `zuuld` server, and a `zuulctl` admin CLI. Single-node
benchmarks (Apple M1 Max): acquire+release ≈ 25 µs/op, linearizable read ≈ 2.7 µs/op.

**Not yet:** OTEL telemetry and an etcd v3 wire-compatibility gateway. See
`PRP/projectplan-zuul.md` for the full design and roadmap, and
`PRP/security-audit-dragonboat.md` for the dependency audit.

## Layout

```
client/                Go client library (Client, Mutex, Election, Master dialing)
cmd/zuuld/             the server binary
examples/quickstart/   embedded single-node demo
proto/zuul/v1/         client-facing gRPC API (Locker/Session/Election/Cluster)
internal/
  fsm/        per-shard replicated state machine (the correctness heart)
  consensus/  dragonboat host: shards, propose/read, membership, expiry
  forward/    node-to-node forward-to-leader plane
  meta/       topology (meta) shard
  router/ session/ watch/ server/ node/ zuultls/   supporting layers
  dragonboat/ rehomed + audited Raft engine
```
