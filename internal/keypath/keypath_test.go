package keypath

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "Success: two segments", key: "/alice/lock"},
		{name: "Success: three segments with dir", key: "/alice/orders/42"},
		{name: "Success: deep path", key: "/alice/a/b/c/d"},
		{name: "Success: email owner", key: "/alice@corp/lock"},
		{name: "Success: dotted and dashed names", key: "/svc-orders/order.42_v2"},
		{name: "Error: empty", key: "", wantErr: true},
		{name: "Error: no leading slash", key: "alice/lock", wantErr: true},
		{name: "Error: single segment", key: "/alice", wantErr: true},
		{name: "Error: trailing slash", key: "/alice/lock/", wantErr: true},
		{name: "Error: empty middle segment", key: "/alice//lock", wantErr: true},
		{name: "Error: dot segment", key: "/alice/./lock", wantErr: true},
		{name: "Error: dotdot segment", key: "/alice/../lock", wantErr: true},
		{name: "Error: space in segment", key: "/alice/my lock", wantErr: true},
		{name: "Error: slash-only", key: "/", wantErr: true},
		{name: "Error: bad char", key: "/alice/lock!", wantErr: true},
		{name: "Error: overlong path", key: "/alice/" + strings.Repeat("a", 1100), wantErr: true},
	}

	for _, test := range tests {
		err := Validate(test.key)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestValidate(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestValidate(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}

func TestOwner(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{name: "Success: two segments", key: "/alice/lock", want: "alice"},
		{name: "Success: three segments", key: "/bob/orders/42", want: "bob"},
		{name: "Success: email owner", key: "/alice@corp/lock", want: "alice@corp"},
		{name: "Error: not a path", key: "orders/42", wantErr: true},
		{name: "Error: single segment", key: "/alice", wantErr: true},
	}

	for _, test := range tests {
		got, err := Owner(test.key)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestOwner(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestOwner(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		if got != test.want {
			t.Errorf("TestOwner(%s): got %q, want %q", test.name, got, test.want)
		}
	}
}
