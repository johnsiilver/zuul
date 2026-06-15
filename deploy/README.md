# Deploying Zuul

## Build the image

```bash
docker build -f deploy/Dockerfile -t <registry>/zuul:latest .
docker push <registry>/zuul:latest
```

The binary is static (`CGO_ENABLED=0`) and runs in a distroless image as non-root.

## Run on Kubernetes

`deploy/k8s/zuul.yaml` is a 3-node cluster: a headless `Service` (per-pod DNS), a
`zuul-client` `Service` (clients dial any node on `:8001`), and a `StatefulSet` that
runs `zuuld --discovery k8s`. Each pod derives its replica id from its ordinal
(`zuul-0` → replica 1) and finds peers by stable DNS — no external coordination.

```bash
# set the image first
kubectl apply -f deploy/k8s/zuul.yaml
```

**Scaling.** Keep `--k8s-replicas` in sync with `.spec.replicas` for the *initial*
size. To grow/shrink a running cluster, use the `Cluster` admin API
(`zuulctl --addr <node>:8001 add-node …` / `remove-node …`) — runtime membership is
dynamic. `terminationGracePeriodSeconds: 30` lets `zuuld` transfer shard leadership
on `SIGTERM` before exiting.

**Why the manifest looks the way it does (required for a quorum cluster):**

- `podManagementPolicy: Parallel` — the default `OrderedReady` starts pods one at a
  time and waits for each to be Ready, but a node only becomes Ready once a quorum
  has elected leaders, so an ordered start **deadlocks at pod 0**. Parallel brings all
  pods up together.
- `publishNotReadyAddresses: true` on the headless Service — during initial formation
  no pod is Ready yet, so without this the peers' DNS records aren't published and the
  nodes can't resolve each other to elect a leader. (This is the standard requirement
  for any StatefulSet whose pods must talk before becoming Ready.)
- A `startupProbe` gives the boot/election window before liveness can act (`zuuld`
  opens its gRPC port only after the cluster forms).

> **⚠ Critical: this cluster is in-memory and does NOT survive individual pod
> restarts.** Lock/lease state lives only in RAM, and a restarted pod comes back
> amnesiac with the same replica id — which a Raft member cannot do (the leader's log
> is ahead of the restarted node's empty one), so the pod crash-loops. This means
> **rolling updates and single-pod restarts (image change, eviction, node drain) will
> break the restarted pod.** Treat the cluster as recreate-only: roll the whole
> StatefulSet at once (`kubectl delete statefulset zuul` then re-apply), or, to replace
> one node in place, first evict it via the admin API
> (`zuulctl remove-node <id>`) and re-add it (`add-node …`) as a fresh replica. A
> durable-storage mode (so a restarted node recovers its log) is the sketched but
> unbuilt alternative if you need rolling restarts.

## Mutual TLS (recommended for production)

mTLS is off by default. To enable it, mount a CA + per-pod cert/key (e.g. via
cert-manager) and add to the StatefulSet `args`:

```yaml
- "--mutual-tls"
- "--tls-ca=/tls/ca.pem"
- "--tls-cert=/tls/tls.crt"
- "--tls-key=/tls/tls.key"
```

The certificate SANs must cover the pod DNS names
(`zuul-0.zuul.<ns>.svc.cluster.local`, …) and `zuul-client.<ns>.svc.cluster.local`.

## Per-key authorization (optional)

With mTLS on, `--acl-file=/etc/zuul/acl` enforces per-identity key access. Each line
is `identity prefix mode`, where `mode` is `r` plus optional `w` (write) and/or `a`
(cluster administration — AddNode/RemoveNode; **never implied by `w`**, so a
wildcard lock grant cannot accidentally hand out membership control), and `*` means
all keys. Identity is the client certificate Common Name (or the token identity
with token auth):

```
# identity     prefix     mode
orders-svc     orders/    rw
dashboard      orders/    r
operator       *          rwa
```

Authorization is per-key, not per-lock-owner: any identity authorized to write a
key may operate on locks under it (the request's client id + fencing token select
which lock is acted on — they are not credentials). Give clients distinct key
prefixes when they must not touch each other's locks.

## Bearer-token authentication (static, OIDC, Azure MSI)

Instead of per-client certificates, clients can authenticate with bearer tokens.
Three sources, all feeding the same identity → ACL pipeline:

- **Static tokens**: `--auth-tokens-file` (lines of `token identity`).
- **OIDC**: `--oidc-issuer <url> --oidc-audience <aud>` — tokens are validated as
  JWTs against the issuer (discovery + JWKS, keys rotate automatically); the
  identity is the `sub` claim, or `--oidc-identity-claim` to pick another.
- **Azure Managed Service Identity**: MSI tokens are Entra ID JWTs, so configure
  OIDC with the Entra issuer:

  ```
  --oidc-issuer https://login.microsoftonline.com/<tenant>/v2.0   --oidc-audience api://zuul --oidc-identity-claim oid
  ```

  The Go client fetches MSI tokens itself:
  `client.WithAzureMSI(&client.AzureMSI{Resource: "api://zuul"})` (set `ClientID`
  for a user-assigned identity). Any other rotating credential plugs in via
  `client.WithTokenSource`.

Bearer auth requires TLS — `--mutual-tls`, or **`--server-tls`** when clients
should not need certificates of their own: the channel is encrypted and
server-authenticated (clients verify the CA only), token auth identifies clients,
and the nodes still authenticate each other with their certificates (the Raft
plane stays mutual; the inter-node forward plane rejects any caller that did not
present a CA-verified certificate). On a plaintext listener bearer auth is refused
outright — the forward plane would be an authentication bypass. The OIDC issuer must
be `https` (loopback `http` is allowed only for testing).

By default the forward plane trusts any CA-signed certificate. If clients also hold
certificates from the same CA, set **`--peer-ca <node-ca.pem>`** (the CA that signs
node certificates): the forward plane then requires the caller's certificate to
chain to that CA, so a client certificate cannot reach it even under `--mutual-tls`.
For a positive node allowlist on top of that, add **`--peer-allowed-cns node-a,node-b`**
to pin the accepted certificate Common Names. (Revocation is not checked — rotate the
peer CA / reissue to evict a compromised node.) Without `--peer-ca`, issue node
certificates from a separate CA. The Go client refuses to send a token over a
connection without TLS credentials (`Options.Insecure` is for unencrypted dev dials
only and cannot be combined with a token).

## Connecting

Point the Go client (or any client) at `zuul-client.<namespace>:8001`:

```go
cl, _ := client.New(ctx, client.Endpoints{"zuul-client.default:8001"}, client.WithClientID("worker-1"))
```
