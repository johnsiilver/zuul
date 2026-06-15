package authz

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzLoadFile feeds arbitrary bytes to the ACL-file parser; it must never panic
// (malformed input must surface as an error, not a crash).
func FuzzLoadFile(f *testing.F) {
	f.Add("orders-svc orders/ rw\n")
	f.Add("# comment\nadmin * rwa\n")
	f.Add("a b\nc d e f\n\t\n  *  r ")
	f.Add("x * rww\ny  ra\n")
	f.Fuzz(func(t *testing.T, content string) {
		path := filepath.Join(t.TempDir(), "acl")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Skip()
		}
		a, err := LoadFile(path)
		if err == nil && a == nil {
			t.Fatalf("FuzzLoadFile: nil authorizer with nil error for %q", content)
		}
	})
}

// FuzzLoadTokens feeds arbitrary bytes to the token-file parser; it must never panic.
func FuzzLoadTokens(f *testing.F) {
	f.Add("tok identity\n")
	f.Add("# c\na b\n  \nx\ty\n")
	f.Add("onlyone\nthree fields here\n")
	f.Fuzz(func(t *testing.T, content string) {
		path := filepath.Join(t.TempDir(), "tok")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Skip()
		}
		_, _ = LoadTokens(path)
	})
}
