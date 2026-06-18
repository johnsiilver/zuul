---
name: zuul-client-design
description: Design Go client interactions with Zuul for a specific use case — distributed locks, leader election, dialing the elected master, and lease-bound sessions — including path-based keys, the identity/ACL model, and multi-endpoint failover. Use when writing or reviewing code that imports github.com/johnsiilver/zuul/client.
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - Bash(go build*)
  - Bash(go test*)
  - Bash(go vet*)
  - Bash(go doc*)
---

# /zuul-client-design — Designing Zuul client interactions

Use this to turn a requirement ("only one worker may process the queue", "route
traffic to the current primary", "guard a critical section across pods") into correct
code on the Zuul Go client (`github.com/johnsiilver/zuul/client`).

Start by naming the use case, then follow the matching recipe. Always honor the two
cross-cutting rules: **keys are paths** and **the principal is established once**.

## 1. Connect

```go
cl, err := client.New(ctx, client.Endpoints{"node-a:8001", "node-b:8001", "node-c:8001"},
    client.WithClientID("worker-1"),   // omit to let the server assign one
    client.WithTTL(30*time.Second),    // lease duration; library renews it for you
)
defer cl.Close()
```

- **Endpoints** — pass several node addresses; the client fails over between them when
  a node dies, and the background lease keepalive re-establishes the session on a
  survivor before the TTL lapses, so held locks survive node failure.
- One `*client.Client` is safe to share across goroutines for **distinct** keys; drive
  a single key from one goroutine at a time.

### Identity / auth options (pick what the server runs)

| Option | When |
| --- | --- |
| `client.WithUser("alice")` | Server runs no authenticating method (dev / trusted net). Sets the ACL principal via the `zuul-user` header (advisory). If the server *does* authenticate, this must equal the authenticated identity or requests are rejected. |
| `client.WithAuthToken(tok)` | Static bearer token (`zuuld --auth-tokens-file`). Requires TLS transport creds via `WithDialOptions`. |
| `client.WithTokenSource(fn)` / `client.WithAzureMSI(...)` | Rotating / OIDC / Azure MSI tokens. |
| `client.WithDialOptions(grpc.WithTransportCredentials(...))` | mTLS or server-TLS. A bearer token MUST travel over TLS — the client refuses otherwise. |
| `client.WithInsecure()` | Dev only; rejected together with any bearer token. |

The **principal** for authorization comes from the auth method (mTLS cert CN, OIDC
claim, token identity) or, when none is configured, from `WithUser`. It is **not** the
`ClientID` (that is just the session/lease owner and is unauthenticated).

## 2. Keys are paths

Every lock/election key MUST be `/<user>/<dir.../><name>` (segments `[A-Za-z0-9._@-]`,
≥2 segments). Non-path keys fail with `InvalidArgument`. A principal automatically has
read-write under its own `/<principal>/` subtree; cross-user access comes from the ACL
file. So name keys under the owning principal, e.g. `/alice/orders/42`,
`/svc-orders/leader`.

## 3. Recipes by use case

### Mutual exclusion (distributed lock)
One holder at a time; survives client pause via fencing tokens.
```go
mu := cl.NewMutex("/svc/orders/42")
ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // set a deadline to bound the wait
defer cancel()
if err := mu.Lock(ctx); err != nil { // blocks until held or the ctx deadline expires
    // errors.Is(err, client.ErrNotAcquired) on a bounded-wait timeout
    return err
}
defer mu.Unlock(ctx)
useResource(mu.Token()) // PASS the fencing token to the guarded resource; a stale
                        // (paused-then-resumed) holder is then rejected downstream
```
- `TryLock(ctx)` — non-blocking; returns `(false, nil)` if held.
- The fencing token is the safety mechanism (Kleppmann): the lock alone is not enough
  if the guarded resource can be reached by a stalled holder — always thread the token
  through.

### Single active instance (leader election)
```go
el := cl.NewElection("/svc/leader")
if err := el.Campaign(ctx, []byte("worker-1")); err != nil { return err } // blocks until leader (set a ctx deadline to bound the wait)
defer el.Resign(ctx)
// ... do leader-only work; el.Proclaim(ctx, newValue) updates the published value ...

// Observers (any client) watch leadership:
events, _ := el.Observe(ctx) // current leader first, then each change; auto-resumes
for ev := range events {
    log.Printf("leader=%q value=%q", ev.ID, ev.Value)
}
```

### Dial the elected master (service discovery)
The leader publishes where to reach it; clients resolve/follow it by election path.
```go
import zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"

// Master side: publish host:port (host may be an IP or a DNS name).
val, _ := client.MarshalEndpoint(&zuulv1.Endpoint{Host: "primary.svc.cluster.local", Port: 8443})
el.Campaign(ctx, val)               // or el.Proclaim(ctx, val) to update

// Client side: one-shot, or follow across failovers.
m, ok, _ := el.Master(ctx)         // ok=false while leaderless; m.Address() == "host:port"
f, _ := el.FollowMaster(ctx)       // f.Current() always points at the live master,
defer f.Close()                    // f.Updates() delivers each new address
```
`Endpoint.metadata` is a `google.protobuf.Any` for extra typed payload (region, a
second address, capabilities). The published value is capped by the server's
`--max-recv-bytes` (default ~1 MiB).

### Long-lived worker
No extra work: the session lease is kept alive in the background until `Close`. On a
network blip the client re-establishes the session (possibly on another endpoint), so
locks/leadership are retained while the process is healthy.

## 4. Error handling

| Condition | Signal |
| --- | --- |
| Bounded `Lock`/`Campaign` wait expired | `client.ErrNotAcquired` |
| `Unlock`/`Proclaim`/`Resign` without holding | `client.ErrNotHeld` |
| Not authorized for the key | gRPC `codes.PermissionDenied` |
| Key is not a valid path | gRPC `codes.InvalidArgument` |
| Stale fencing token on `Unlock`/`Proclaim`/`Resign` | `codes.FailedPrecondition` |

Inspect gRPC codes with `status.Code(err)` (`google.golang.org/grpc/status`).

## Procedure for this skill

1. Confirm the use case (lock / election / master-dial / combination) and the security
   posture (auth method or `WithUser`).
2. Choose the key path(s) under the owning principal's namespace.
3. Write the smallest correct client code from the recipe; thread fencing tokens for
   locks; use multiple `Endpoints` for failover.
4. `go build` / `go vet` it. If a server is reachable, exercise it; otherwise note the
   expected `status.Code` outcomes.

See also: the `/zuul-k8s-deploy` skill for standing up the cluster the client talks to.
