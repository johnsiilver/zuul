package client

import (
	"testing"
)

// TestDialTokenRequiresTLS proves Dial refuses to send a bearer token over an
// insecure connection (no transport credentials provided).
func TestDialTokenRequiresTLS(t *testing.T) {
	_, err := New(t.Context(), Endpoints{"127.0.0.1:1"}, WithClientID("c"), WithAuthToken("tok"))
	if err == nil {
		t.Errorf("TestDialTokenRequiresTLS: AuthToken without DialOptions: got err == nil, want err != nil")
	}
	_, err = New(t.Context(), Endpoints{"127.0.0.1:1"}, WithClientID("c"), WithAzureMSI(&AzureMSI{Resource: "api://zuul"}))
	if err == nil {
		t.Errorf("TestDialTokenRequiresTLS: AzureMSI without DialOptions: got err == nil, want err != nil")
	}
}
