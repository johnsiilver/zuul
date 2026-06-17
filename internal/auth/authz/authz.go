// Package authz authorizes key operations by the caller's authenticated identity.
// Identity comes from the mutual-TLS client certificate (its Common Name), so it
// cannot be spoofed by the request — and authorization therefore only has teeth when
// mutual TLS is enabled. The default authorizer permits everything (authz is opt-in);
// a prefix policy grants each identity a set of key prefixes, read-only or read-write.
package authz

import (
	"github.com/johnsiilver/zuul/errors"
	"github.com/johnsiilver/zuul/internal/auth/keypath"
)

// Op is the kind of access being requested.
type Op int

const (
	// UnknownOp is the unset value; it is never authorized.
	UnknownOp Op = iota
	// Read is a non-mutating operation (Status, Leader, Observe).
	Read
	// Write is a mutating operation (Lock, Unlock, Campaign, Proclaim, Resign).
	Write
	// Admin is a cluster-administration operation (AddNode, RemoveNode). It is a
	// distinct right — a wildcard read-write grant does NOT confer it — so a broad
	// lock-key grant cannot accidentally hand out membership control.
	Admin
)

// ErrDenied is returned by an Authorizer when access is refused.
var ErrDenied = errors.New("authz: access denied")

// UserHeader is the gRPC metadata key by which a client may assert its principal
// when no authenticating method exposes one. It is unauthenticated: the server
// honors it only in the absence of a real identity (mTLS/OIDC/token), and otherwise
// requires it to match the authenticated identity (see the auth gate).
const UserHeader = "zuul-user"

// Authorizer decides whether identity may perform op on key.
type Authorizer interface {
	Authorize(identity, key string, op Op) error
}

// allowAll permits every operation.
type allowAll struct{}

// AllowAll returns an Authorizer that permits everything (the default).
func AllowAll() Authorizer { return allowAll{} }

func (allowAll) Authorize(string, string, Op) error { return nil }

// NeedsIdentity reports whether a is a restrictive policy that decides on the
// caller's identity (so it is useless — denies everyone — without an identity
// source). AllowAll and a nil authorizer do not need one.
func NeedsIdentity(a Authorizer) bool {
	if a == nil {
		return false
	}
	_, allow := a.(allowAll)
	return !allow
}

// Rule grants an identity access to keys under Prefix. A matching rule always
// allows Read; Write and Admin are granted separately.
type Rule struct {
	// Prefix is the key prefix this rule covers; "" matches every key.
	Prefix string
	// Write allows mutating operations (else the rule is read-only).
	Write bool
	// Admin allows cluster-administration operations. Never implied by Write.
	Admin bool
}

// prefixPolicy authorizes by per-identity prefix rules.
type prefixPolicy struct {
	rules map[string][]Rule
}

// Prefix returns an Authorizer driven by per-identity prefix rules. An identity with
// no matching rule is denied.
func Prefix(rules map[string][]Rule) Authorizer {
	cp := make(map[string][]Rule, len(rules))
	for id, rs := range rules {
		cp[id] = append([]Rule(nil), rs...)
	}
	return &prefixPolicy{rules: cp}
}

// Authorize permits op on key if some rule for identity covers the key and grants
// the requested right: any matching rule allows Read, Write needs the rule's Write,
// Admin needs the rule's Admin.
func (p *prefixPolicy) Authorize(identity, key string, op Op) error {
	for _, r := range p.rules[identity] {
		if !prefixCovers(r.Prefix, key) {
			continue
		}
		switch op {
		case Read:
			return nil
		case Write:
			if r.Write {
				return nil
			}
		case Admin:
			if r.Admin {
				return nil
			}
		}
	}
	return ErrDenied
}

// prefixCovers reports whether a rule for prefix covers key, matching on a path
// boundary (see keypath.HasPrefix): an empty prefix is a wildcard, and "/a/configs"
// covers "/a/configs" and "/a/configs/..." but not "/a/configs-secret".
func prefixCovers(prefix, key string) bool {
	return keypath.HasPrefix(prefix, key)
}

// homeDir grants an identity read-write under its own /<identity>/ path subtree
// (its "home directory"), delegating every other decision to inner. The grant
// never includes Admin. Keys that are not canonical resource paths (e.g. the
// internal "cluster/" admin key) are always delegated.
type homeDir struct {
	inner Authorizer
}

// HomeDir wraps inner so that a principal automatically has read-write access to
// keys it owns — those whose first path segment equals the principal's identity.
// All other access (including cross-user grants) is decided by inner.
func HomeDir(inner Authorizer) Authorizer {
	return &homeDir{inner: inner}
}

// Authorize grants Read/Write when identity owns key, otherwise delegates to inner.
func (h *homeDir) Authorize(identity, key string, op Op) error {
	if identity != "" && (op == Read || op == Write) {
		if owner, err := keypath.Owner(key); err == nil && owner == identity {
			return nil
		}
	}
	return h.inner.Authorize(identity, key, op)
}
