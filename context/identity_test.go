package context

import "testing"

func TestIdentityFromContext(t *testing.T) {
	// An interceptor-attached identity round-trips back out.
	switch id, ok := IdentityFromContext(WithIdentity(Background(), "alice")); {
	case !ok:
		t.Error("TestIdentityFromContext: got ok == false for an attached identity, want true")
	case id != "alice":
		t.Errorf("TestIdentityFromContext: got identity == %q, want \"alice\"", id)
	}

	// A bare context (no attached identity, no TLS peer) presents nothing.
	if id, ok := IdentityFromContext(Background()); ok || id != "" {
		t.Errorf("TestIdentityFromContext: got (%q, %t) for a bare context, want (\"\", false)", id, ok)
	}
}
