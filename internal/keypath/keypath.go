// Package keypath validates and parses Zuul resource paths. A path is a
// filesystem-like, slash-separated key identifying a lock or election:
//
//	/<user>/<dir.../><name>
//
// The first segment is the owner (the user namespace); the remaining segments
// name a resource within it. Paths are case-sensitive. Every lock and election
// key is required to be a canonical path so authorization can reason about
// ownership (see internal/authz HomeDir).
package keypath

import (
	"fmt"
	"strings"
)

const (
	// maxSegmentLen bounds a single path segment.
	maxSegmentLen = 255
	// maxPathLen bounds the whole path.
	maxPathLen = 1024
)

// Validate reports whether key is a canonical Zuul resource path: a leading
// "/", at least two segments (/<user>/<name>), each non-empty, neither "." nor
// "..", matching [A-Za-z0-9._@-], with no trailing slash and within length
// bounds.
func Validate(key string) error {
	switch {
	case key == "":
		return fmt.Errorf("keypath: empty key")
	case len(key) > maxPathLen:
		return fmt.Errorf("keypath: key exceeds %d bytes", maxPathLen)
	case key[0] != '/':
		return fmt.Errorf("keypath: %q must begin with '/'", key)
	case strings.HasSuffix(key, "/"):
		return fmt.Errorf("keypath: %q must not end with '/'", key)
	}
	segs := strings.Split(key[1:], "/")
	if len(segs) < 2 {
		return fmt.Errorf("keypath: %q must have at least two segments (/<user>/<name>)", key)
	}
	for _, s := range segs {
		if err := validateSegment(s); err != nil {
			return fmt.Errorf("keypath: %q: %w", key, err)
		}
	}
	return nil
}

// validateSegment reports whether s is a legal path segment.
func validateSegment(s string) error {
	switch {
	case s == "":
		return fmt.Errorf("empty segment")
	case s == "." || s == "..":
		return fmt.Errorf("segment %q is not allowed", s)
	case len(s) > maxSegmentLen:
		return fmt.Errorf("segment exceeds %d bytes", maxSegmentLen)
	}
	for _, r := range s {
		if !isSegmentRune(r) {
			return fmt.Errorf("segment %q contains invalid character %q", s, string(r))
		}
	}
	return nil
}

// isSegmentRune reports whether r is allowed in a path segment.
func isSegmentRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r >= '0' && r <= '9':
		return true
	}
	switch r {
	case '.', '_', '@', '-':
		return true
	}
	return false
}

// Owner returns the first segment (the user namespace) of a canonical path. It
// returns an error if key is not a canonical path.
func Owner(key string) (string, error) {
	if err := Validate(key); err != nil {
		return "", err
	}
	rest := key[1:]
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i], nil
	}
	return rest, nil
}
