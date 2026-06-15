package errors

import (
	"testing"

	"google.golang.org/grpc/codes"
)

func TestCategoryCode(t *testing.T) {
	tests := []struct {
		name string
		cat  Category
		want codes.Code
	}{
		{name: "Success: request maps to InvalidArgument", cat: CatRequest, want: codes.InvalidArgument},
		{name: "Success: precondition maps to FailedPrecondition", cat: CatPrecondition, want: codes.FailedPrecondition},
		{name: "Success: permission maps to PermissionDenied", cat: CatPermission, want: codes.PermissionDenied},
		{name: "Success: internal maps to Internal", cat: CatInternal, want: codes.Internal},
		{name: "Success: unknown category maps to Unknown", cat: UnknownCategory, want: codes.Unknown},
		{name: "Success: out-of-range category maps to Unknown", cat: Category(9999), want: codes.Unknown},
	}

	for _, test := range tests {
		got := test.cat.Code()
		if got != test.want {
			t.Errorf("TestCategoryCode(%s): got code == %s, want code == %s", test.name, got, test.want)
		}
	}
}

func TestErrorIsLinksBackByCategoryAndType(t *testing.T) {
	ctx := t.Context()

	// An error produced anywhere with CatNotFound/TypeNotFound is equal to the canonical
	// one, even though the underlying messages differ. This is the cross-package link-back.
	local := E(ctx, CatNotFound, TypeNotFound, New("row 42 missing"))
	canonical := E(ctx, CatNotFound, TypeNotFound, New("not found"))
	other := E(ctx, CatPrecondition, TypeStaleFencingToken, New("stale token"))

	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{name: "Success: same category and type are equal", err: local, target: canonical, want: true},
		{name: "Error: different category and type are not equal", err: local, target: other, want: false},
	}

	for _, test := range tests {
		got := Is(test.err, test.target)
		if got != test.want {
			t.Errorf("TestErrorIsLinksBackByCategoryAndType(%s): got Is == %v, want %v", test.name, got, test.want)
		}
	}
}

func TestPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "Success: nil stays nil and is not permanent", err: nil, want: false},
		{name: "Success: wrapped error is permanent", err: New("boom"), want: true},
	}

	for _, test := range tests {
		got := Permanent(test.err)
		switch {
		case test.err == nil && got != nil:
			t.Errorf("TestPermanent(%s): got non-nil, want nil", test.name)
		case test.want && !Is(got, ErrPermanent):
			t.Errorf("TestPermanent(%s): got Is(err, ErrPermanent) == false, want true", test.name)
		case !test.want && got != nil && Is(got, ErrPermanent):
			t.Errorf("TestPermanent(%s): got Is(err, ErrPermanent) == true, want false", test.name)
		}
	}
}

func TestESecretRedaction(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name         string
		msg          string
		wantRedacted bool
	}{
		{name: "Success: key path is not redacted", msg: "invalid key path: /alice/lock", wantRedacted: false},
		{name: "Success: fencing token wording is not redacted", msg: "stale fencing token", wantRedacted: false},
		{name: "Success: transport credentials wording is not redacted", msg: "no transport credentials provided", wantRedacted: false},
		{name: "Success: leaked bearer token is redacted", msg: "auth failed for Bearer abcDEF123456789", wantRedacted: true},
		{name: "Success: leaked jwt is redacted", msg: "bad token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", wantRedacted: true},
		{name: "Success: password assignment is redacted", msg: "connect failed password=hunter2", wantRedacted: true},
	}

	for _, test := range tests {
		e := E(ctx, CatInternal, TypeBackend, New(test.msg))
		redacted := e.Error() == "[redacted for security]"
		if redacted != test.wantRedacted {
			t.Errorf("TestESecretRedaction(%s): got redacted == %v (message %q), want %v", test.name, redacted, e.Error(), test.wantRedacted)
		}
	}
}
