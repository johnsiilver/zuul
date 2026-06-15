/*
Package errors is zuul's service error package, built on github.com/gostdlib/base/errors.
It is the single errors package for the service: import it as "errors" everywhere (it is a
drop-in superset of the standard library's errors package, see stdlib.go) and reach the
whole API through it — E, the Category/Type taxonomy, Is/As/New/Join, ErrPermanent, and the
gRPC mapping helpers in grpc.go.

Every error returned across a package or transport boundary should be stamped with a
Category and Type via E(). Cross-package errors share the canonical Category/Type defined
here, so errors.Is links a locally produced error back to the canonical one (Error.Is
compares Category and Type). Errors that must not be retried are marked with Permanent().
*/
package errors

import (
	"fmt"
	"regexp"

	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/errors"
	"github.com/gostdlib/base/retry/exponential"

	"google.golang.org/grpc/codes"
)

//go:generate go tool github.com/johnsiilver/stringer -type=Category -linecomment

// Category is the broad class of an error. It maps to a gRPC status code via Code().
// The zero value is UnknownCategory so an unclassified (buggy) error is detectable.
type Category uint32

// Category implements the base/errors.Category interface.
func (c Category) Category() string {
	return c.String()
}

// Code returns the gRPC status code for the category.
func (c Category) Code() codes.Code {
	if code, ok := catToCode[c]; ok {
		return code
	}
	return codes.Unknown
}

const (
	UnknownCategory      Category = iota // Unknown
	CatRequest                           // Request
	CatNotFound                          // NotFound
	CatUnauthenticated                   // Unauthenticated
	CatPermission                        // Permission
	CatPrecondition                      // Precondition
	CatResourceExhausted                 // ResourceExhausted
	CatUnavailable                       // Unavailable
	CatUnimplemented                     // Unimplemented
	CatInternal                          // Internal
)

// catToCode maps a Category to its gRPC status code. Keep in sync with the constants.
var catToCode = map[Category]codes.Code{
	UnknownCategory:      codes.Unknown,
	CatRequest:           codes.InvalidArgument,
	CatNotFound:          codes.NotFound,
	CatUnauthenticated:   codes.Unauthenticated,
	CatPermission:        codes.PermissionDenied,
	CatPrecondition:      codes.FailedPrecondition,
	CatResourceExhausted: codes.ResourceExhausted,
	CatUnavailable:       codes.Unavailable,
	CatUnimplemented:     codes.Unimplemented,
	CatInternal:          codes.Internal,
}

func init() {
	if len(catToCode) != int(CatInternal)+1 {
		panic("errors: catToCode is missing a Category")
	}
}

//go:generate go tool github.com/johnsiilver/stringer -type=Type -linecomment

// Type is a subcategory within a Category. The zero value is UnknownType.
type Type uint16

// Type implements the base/errors.Type interface.
func (t Type) Type() string {
	return t.String()
}

const (
	UnknownType Type = iota // Unknown

	// Request errors (CatRequest).
	TypeBadRequest      // BadRequest
	TypeInvalidKeyPath  // InvalidKeyPath
	TypeMissingClientID // MissingClientID

	// Authentication errors (CatUnauthenticated).
	TypeMissingCredentials // MissingCredentials
	TypeInvalidToken       // InvalidToken

	// Permission errors (CatPermission).
	TypeIdentityMismatch    // IdentityMismatch
	TypePeerCertRequired    // PeerCertRequired
	TypePeerCertUntrusted   // PeerCertUntrusted
	TypePeerCertNotAllowed  // PeerCertNotAllowed
	TypeUnauthorizedKey     // UnauthorizedKey
	TypeUnauthorizedCluster // UnauthorizedCluster

	// Precondition errors (CatPrecondition).
	TypeNoLiveLease       // NoLiveLease
	TypeStaleFencingToken // StaleFencingToken
	TypeNotLockHolder     // NotLockHolder
	TypeNotLeader         // NotLeader
	TypeNoSession         // NoSession

	// Resource errors (CatResourceExhausted).
	TypeRateLimited // RateLimited

	// Lifecycle errors.
	TypeNotFound    // NotFound
	TypeNotAcquired // NotAcquired

	// Unimplemented errors (CatUnimplemented).
	TypeMembershipDisabled // MembershipDisabled

	// Internal errors (CatInternal).
	TypeUnexpectedOutcome // UnexpectedOutcome
	TypeBackend           // Backend
	TypeMarshal           // Marshal
	TypeConsensus         // Consensus

	// Configuration / startup errors (CatRequest for bad config, CatInternal for
	// failed wiring).
	TypeConfig // Config

	// Discovery / membership infrastructure errors (CatUnavailable).
	TypeDiscovery // Discovery
)

// Error is zuul's error type. It is base/errors.Error, carrying a Category and Type.
// Functions should return the error interface, never this concrete type.
type Error = errors.Error

// EOption is an optional argument for E.
type EOption = errors.EOption

// Re-exported base EOption constructors so callers need only this package.
var (
	// WithLogLevel sets the log level for the error, e.g. slog.LevelWarn for a retryable error.
	WithLogLevel = errors.WithLogLevel
	// WithAttrs adds slog attributes to the error for logging.
	WithAttrs = errors.WithAttrs
	// WithStackTrace attaches a stack trace to the error.
	WithStackTrace = errors.WithStackTrace
	// WithSuppressTraceErr records the trace without an error status (for retried errors).
	WithSuppressTraceErr = errors.WithSuppressTraceErr
	// WithCallNum adjusts the stack frame used for file/line, for wrappers around E.
	WithCallNum = errors.WithCallNum
)

// secretRE matches actual secret *values* (a bearer token, a JWT, a "password=" or
// "api_key=" pair) so a leaked credential is redacted. It deliberately does not match the
// bare words key/token/code/cert/credential, which are first-class non-secret concepts in
// zuul (key paths, fencing tokens, transport credentials) and would otherwise redact
// legitimate operational errors. Tighten or loosen here as needed.
var secretRE = regexp.MustCompile(`(?i)(bearer\s+[A-Za-z0-9._\-]{8,}|eyJ[A-Za-z0-9._\-]{10,}|(password|passwd|api[_\- ]?key|secret)\s*[:=]\s*\S+)`)

// E stamps msg with the given Category and Type via base/errors and returns the Error.
// If msg is already an Error it is returned unchanged. If the message looks like it
// contains a secret it is redacted. file/line are recorded for E's caller.
func E(ctx context.Context, c Category, t Type, msg error, options ...EOption) Error {
	opts := append([]EOption{WithCallNum(2)}, options...)
	e := errors.E(ctx, c, t, msg, opts...)
	if msg != nil && e.MsgOverride == "" && secretRE.MatchString(msg.Error()) {
		e.MsgOverride = "[redacted for security]"
	}
	return e
}

// ErrPermanent marks an error as non-retryable so retry/exponential stops. Do not return
// it bare — wrap a real error with Permanent() or fmt.Errorf("%w: %w", err, ErrPermanent).
// Detect with Is(err, ErrPermanent).
var ErrPermanent = exponential.ErrPermanent

// Permanent wraps err to mark it non-retryable. It returns nil if err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", err, ErrPermanent)
}
