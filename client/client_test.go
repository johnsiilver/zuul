package client

import (
	"testing"

	"google.golang.org/grpc/credentials/insecure"
)

// TestDialTokenRequiresTLS proves New refuses to send a bearer token over an insecure
// connection: with no transport credentials at all, and — the regression case — with
// explicitly insecure credentials, which must not be mistaken for real transport
// security (the previous gate inferred "secure" from any dial option being present).
func TestDialTokenRequiresTLS(t *testing.T) {
	_, err := New(t.Context(), Endpoints{"127.0.0.1:1"}, WithClientID("c"), WithAuthToken("tok"))
	if err == nil {
		t.Errorf("TestDialTokenRequiresTLS: AuthToken without credentials: got err == nil, want err != nil")
	}
	_, err = New(t.Context(), Endpoints{"127.0.0.1:1"}, WithClientID("c"), WithAzureMSI(&AzureMSI{Resource: "api://zuul"}))
	if err == nil {
		t.Errorf("TestDialTokenRequiresTLS: AzureMSI without credentials: got err == nil, want err != nil")
	}
	_, err = New(t.Context(), Endpoints{"127.0.0.1:1"}, WithClientID("c"), WithAuthToken("tok"), WithTransportCredentials(insecure.NewCredentials()))
	if err == nil {
		t.Errorf("TestDialTokenRequiresTLS: AuthToken with insecure credentials: got err == nil, want err != nil")
	}
}
