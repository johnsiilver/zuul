// Package auth groups zuul's AAA (authentication, authorization, accounting) wiring. It
// has no code of its own; its subpackages hold the service's identity and access control:
//
//   - authz:    authorization — per-identity key ACLs, plus loading of ACL and bearer-token files.
//   - oidcauth: authentication — OIDC / bearer-token (JWT) verification and identity mapping.
//   - zuultls:  authentication — mutual-TLS configs for the gRPC planes, whose client-cert
//     Common Name becomes the caller's identity (see context.IdentityFromContext).
//   - keypath:  the resource key-path scheme, whose leading segment is the owning identity
//     that authz reasons about for ownership-based access.
//
// This grouping is purely organizational.
package auth
