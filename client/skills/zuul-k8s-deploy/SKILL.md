---
name: zuul-k8s-deploy
description: Stand up a Zuul cluster on Kubernetes (StatefulSet + headless-Service DNS discovery) and layer on a chosen security posture — plaintext/dev, server-TLS with token or OIDC auth, mutual TLS, gossip for dynamic IPs, and path-based ACLs. Use when deploying, hardening, or troubleshooting Zuul on K8s.
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Edit
  - Grep
  - Glob
  - Bash(kubectl*)
  - Bash(kustomize*)
  - Bash(go build*)
  - Bash(docker build*)
  - Bash(mage*)
---

# /zuul-k8s-deploy — Zuul on Kubernetes with security options

Deploy and secure a Zuul cluster on Kubernetes. The repo ships a working baseline at
`deploy/k8s/zuul.yaml` (a 3-node StatefulSet using `zuuld --discovery k8s` DNS
discovery) and a kind lifecycle in `magefiles/k8s.go` (`mage k8sDeploy` / `k8sE2E`).
Start from the baseline, then apply exactly one **security tier** plus optional ACLs.

## Baseline (read first)

`deploy/k8s/zuul.yaml` already encodes the non-obvious requirements — keep them:
- **Headless `Service` (`clusterIP: None`) with `publishNotReadyAddresses: true`** —
  peers resolve each other by stable DNS *before* any pod is Ready (Ready needs a
  quorum, so without this it deadlocks).
- **`podManagementPolicy: Parallel`** — quorum needs all pods up together; the default
  OrderedReady deadlocks at pod 0.
- **gRPC health probes** — readiness is SERVING only once every shard has a leader, and
  flips to NOT_SERVING while draining; a `startupProbe` covers the ~60s bootstrap.
- A second `Service` (`zuul-client`) is the client entry point on `:8001`.
- Each pod derives its replica id from its ordinal (`$HOSTNAME`, e.g. `zuul-2` → id 3).

Build/push the image from `deploy/Dockerfile`. Locally: `mage k8sDeploy` (kind).

## Choose a security tier

Decide based on who the clients are and the trust boundary. **Pick one.**

### Tier 0 — Plaintext (dev / fully trusted network)
The baseline as-is. No encryption, no client auth. Combine with ACLs only in
*advisory* mode: clients assert identity via `client.WithUser("alice")` (the
unauthenticated `zuul-user` header). Never use across a trust boundary.

### Tier 1 — Server-TLS + bearer/OIDC auth (most common)
One-way TLS (encryption + server identity); clients carry **tokens**, no client certs.
Nodes still authenticate each other with certificates.

Add to the container `args`:
```yaml
- "--server-tls"
- "--tls-ca=/tls/ca.pem"
- "--tls-cert=/tls/tls.crt"
- "--tls-key=/tls/tls.key"
# then EITHER static tokens:
- "--auth-tokens-file=/auth/tokens"
# OR OIDC (e.g. Azure Entra):
- "--oidc-issuer=https://login.microsoftonline.com/<tenant>/v2.0"
- "--oidc-audience=api://zuul"
- "--oidc-identity-claim=oid"   # default "sub"
```
- Mount the cert as a TLS `Secret` at `/tls`, tokens as a `Secret` at `/auth`.
- The **authorization principal** is the token's mapped identity or the OIDC claim.
- Client side: `client.WithAuthToken(tok)` or `client.WithTokenSource(fn)` **plus**
  `client.WithDialOptions(grpc.WithTransportCredentials(creds))` — a token without TLS
  transport creds is refused.

### Tier 2 — Mutual TLS (every caller presents a cert)
Strongest. Every client and node presents a CA-signed certificate; the cert **CN is
the authorization identity**.
```yaml
- "--mutual-tls"
- "--tls-ca=/tls/ca.pem"
- "--tls-cert=/tls/tls.crt"
- "--tls-key=/tls/tls.key"
# optional: separate node trust from client trust on the forward plane
- "--peer-ca=/tls/peer-ca.pem"
- "--peer-allowed-cns=zuul-0,zuul-1,zuul-2"
```
- If clients and nodes share a CA, set `--peer-ca` (and/or `--peer-allowed-cns`) so a
  client cert cannot reach the inter-node forward plane.
- Client side: `client.WithDialOptions(grpc.WithTransportCredentials(mtlsCreds))`.

### Tier 3 — Gossip (dynamic IPs, no stable pod DNS)
When DNS discovery does not fit (e.g. non-StatefulSet, churny IPs), address nodes by
NodeHostID over gossip. **Requires mutual TLS.**
```yaml
- "--gossip"
- "--node-host-id=<stable-id>"
- "--gossip-bind=$(POD_IP):7946"
- "--gossip-seeds=seed-a:7946,seed-b:7946"
- "--mutual-tls"  # + tls flags as Tier 2
```

## Certificates (Tiers 1–3)

Server cert **SANs must cover every name clients and peers dial**:
- the client `Service` DNS (`zuul-client.<ns>.svc.cluster.local`),
- each pod's stable DNS (`zuul-0.zuul.<ns>.svc.cluster.local`, …) for the Raft/forward
  planes.

The Go client pins the TLS ServerName to the **host of the endpoint it dials**, so the
cert SAN must match that host (the Service name or pod name), not the pod IP. Issue
certs with cert-manager (a `Certificate` with those DNS names) or mount a prepared TLS
`Secret`. Rotate by reissuing — revocation (CRL/OCSP) is not checked.

## Path ACLs (any tier)

Keys are paths `/<user>/<dir.../><name>`; a principal owns read-write on its own
`/<principal>/` subtree, and an ACL file grants cross-user access:
```
# identity   pathPrefix        mode   (r=read, rw=read-write; rwa adds cluster admin)
dashboard    /orders/          r
svc-orders   /orders/          rw
operator     *                 rwa
```
Mount it via ConfigMap and add `--acl-file=/etc/zuul/acl`. The identity it matches is
the authenticated principal (cert CN / OIDC claim / token identity); with no auth
method it is the advisory `zuul-user` header.

## Other production flags

- `--max-recv-bytes <n>` — raise the ~1 MiB cap if election values are large (set the
  same on every node; forwarded writes must fit too).
- `--rate-limit` / `--per-identity-rate-limit` — request-rate caps.
- `--shards <n>` — keep consistent across the StatefulSet.
- `--ui-enable` + `--ui-bind 127.0.0.1:9999` — optional **read-only** browsing web UI.
  It has **no auth**, so bind it to localhost (and reach it via `kubectl port-forward`)
  or add `--ui-tls` to serve it over the node's server TLS (reuses `--tls-ca/-cert/-key`).
  Never expose it on a Service across a trust boundary.
- Prefer a single `--config` JSON (ConfigMap) over many flags for complex setups;
  see `cmd/zuuld/configfile.go` for the schema (incl. `maxRecvBytes`, `aclFile`,
  `authTokensFile`, `uiEnable`/`uiBind`/`uiTLS`).

## Procedure for this skill

1. Confirm the **security tier** and whether ACLs are needed; identify the clients and
   trust boundary.
2. Copy `deploy/k8s/zuul.yaml`, keep the baseline invariants, and add the tier's flags
   + Secret/ConfigMap mounts. (Consider a kustomize overlay like
   `deploy/k8s/overlays/kind`.)
3. For TLS tiers, produce certs whose SANs cover the client Service and pod DNS names.
4. Validate: `kubectl apply`, watch rollout (Ready == every shard has a leader), then
   verify membership/health — `mage k8sVerify` does this over a port-forward with
   `zuulctl`.
5. Hand the client team the matching dial setup — use the `/zuul-client-design` skill.

## Troubleshooting

- **Pods never Ready / no leader** — missing `publishNotReadyAddresses` or
  `OrderedReady` policy; pods cannot reach quorum.
- **TLS `certificate is valid for X, not Y`** — cert SAN doesn't include the host the
  client/peer dials (Service or pod DNS name).
- **`PermissionDenied`** — ACL has no rule and the key is outside the principal's home
  subtree; or a `zuul-user` header that doesn't match the authenticated identity.
- **`InvalidArgument: invalid key path`** — a key that isn't `/<user>/.../<name>`.
- **Token dial fails fast client-side** — bearer token without TLS transport creds; add
  `client.WithDialOptions(...)`.
