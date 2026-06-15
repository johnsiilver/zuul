package authz

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestPrefixAuthorize covers the prefix policy's allow/deny decisions.
func TestPrefixAuthorize(t *testing.T) {
	a := Prefix(map[string][]Rule{
		"orders-svc": {{Prefix: "orders/", Write: true}},
		"dashboard":  {{Prefix: "orders/", Write: false}},
		"admin":      {{Prefix: "", Write: true}},
		"operator":   {{Prefix: "", Write: true, Admin: true}},
	})

	tests := []struct {
		name     string
		identity string
		key      string
		op       Op
		wantErr  bool
	}{
		{name: "Success: owner writes its prefix", identity: "orders-svc", key: "orders/42", op: Write},
		{name: "Success: owner reads its prefix", identity: "orders-svc", key: "orders/42", op: Read},
		{name: "Error: owner outside its prefix", identity: "orders-svc", key: "users/7", op: Write, wantErr: true},
		{name: "Success: read-only reads", identity: "dashboard", key: "orders/42", op: Read},
		{name: "Error: read-only cannot write", identity: "dashboard", key: "orders/42", op: Write, wantErr: true},
		{name: "Success: admin writes anything", identity: "admin", key: "anything/goes", op: Write},
		{name: "Error: unknown identity denied", identity: "nobody", key: "orders/42", op: Read, wantErr: true},
		{name: "Error: empty identity denied", identity: "", key: "orders/42", op: Read, wantErr: true},
		{name: "Error: wildcard read-write does not confer admin", identity: "admin", key: "cluster/", op: Admin, wantErr: true},
		{name: "Success: explicit admin grant confers admin", identity: "operator", key: "cluster/", op: Admin},
		{name: "Error: unknown op denied even with full grant", identity: "operator", key: "cluster/", op: UnknownOp, wantErr: true},
	}
	for _, test := range tests {
		err := a.Authorize(test.identity, test.key, test.op)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestPrefixAuthorize(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestPrefixAuthorize(%s): got err == %s, want err == nil", test.name, err)
		case err != nil && !errors.Is(err, ErrDenied):
			t.Errorf("TestPrefixAuthorize(%s): got err == %s, want ErrDenied", test.name, err)
		}
	}
}

// TestHomeDir proves a principal owns read-write on its own /<identity>/ subtree,
// that ownership never confers Admin, and that all other access (cross-user grants,
// non-path keys) is delegated to the inner authorizer.
func TestHomeDir(t *testing.T) {
	a := HomeDir(Prefix(map[string][]Rule{
		"bob":      {{Prefix: "/alice/configs/", Write: false}},
		"operator": {{Prefix: "", Write: true, Admin: true}},
	}))

	tests := []struct {
		name     string
		identity string
		key      string
		op       Op
		wantErr  bool
	}{
		{name: "Success: owner writes own subtree without a rule", identity: "alice", key: "/alice/orders/42", op: Write},
		{name: "Success: owner reads own subtree without a rule", identity: "alice", key: "/alice/orders/42", op: Read},
		{name: "Error: ownership does not confer admin", identity: "alice", key: "/alice/orders/42", op: Admin, wantErr: true},
		{name: "Error: non-owner denied without a grant", identity: "bob", key: "/alice/secret", op: Read, wantErr: true},
		{name: "Success: cross-user read grant", identity: "bob", key: "/alice/configs/db", op: Read},
		{name: "Error: cross-user read grant does not allow write", identity: "bob", key: "/alice/configs/db", op: Write, wantErr: true},
		{name: "Success: bob owns its own subtree", identity: "bob", key: "/bob/lock", op: Write},
		{name: "Error: empty identity has no home", identity: "", key: "/alice/x", op: Read, wantErr: true},
		{name: "Success: non-path key delegates to admin grant", identity: "operator", key: "cluster/", op: Admin},
		{name: "Error: non-path key not owned", identity: "alice", key: "cluster/", op: Read, wantErr: true},
	}
	for _, test := range tests {
		err := a.Authorize(test.identity, test.key, test.op)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestHomeDir(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestHomeDir(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

// TestAllowAll proves the default authorizer permits everything.
func TestAllowAll(t *testing.T) {
	a := AllowAll()
	if err := a.Authorize("", "anything", Write); err != nil {
		t.Errorf("TestAllowAll: got err == %s, want nil", err)
	}
}

// TestLoadFile proves the ACL file parses into a working policy.
func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "acl")
	content := "# comment\norders-svc orders/ rw\ndashboard orders/ r\nadmin * rw\noperator * rwa\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("TestLoadFile: write: %s", err)
	}
	a, err := LoadFile(path)
	if err != nil {
		t.Fatalf("TestLoadFile: LoadFile: %s", err)
	}
	if err := a.Authorize("orders-svc", "orders/1", Write); err != nil {
		t.Errorf("TestLoadFile: orders-svc write: %s", err)
	}
	if err := a.Authorize("dashboard", "orders/1", Write); !errors.Is(err, ErrDenied) {
		t.Errorf("TestLoadFile: dashboard write: got %v, want ErrDenied", err)
	}
	if err := a.Authorize("admin", "x/y", Write); err != nil {
		t.Errorf("TestLoadFile: admin write: %s", err)
	}
	if err := a.Authorize("admin", "cluster/", Admin); !errors.Is(err, ErrDenied) {
		t.Errorf("TestLoadFile: rw mode admin op: got %v, want ErrDenied", err)
	}
	if err := a.Authorize("operator", "cluster/", Admin); err != nil {
		t.Errorf("TestLoadFile: rwa mode admin op: %s", err)
	}
}

// TestLoadFileBadLine proves a malformed line is rejected.
func TestLoadFileBadLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "acl")
	if err := os.WriteFile(path, []byte("only two fields\n"), 0o600); err != nil {
		t.Fatalf("TestLoadFileBadLine: write: %s", err)
	}
	if _, err := LoadFile(path); err == nil {
		t.Errorf("TestLoadFileBadLine: got err == nil, want a parse error")
	}
}
