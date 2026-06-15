package client

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gostdlib/base/context"

	"github.com/johnsiilver/zuul/errors"
)

// TestMSITokenSource proves the MSI source fetches from IMDS with the right shape
// (Metadata header, resource/client_id params), caches the token until near expiry,
// and surfaces IMDS failures.
func TestMSITokenSource(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Metadata") != "true" {
			http.Error(w, "missing Metadata header", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("resource") != "api://zuul" {
			http.Error(w, "wrong resource", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("client_id") != "my-identity" {
			http.Error(w, "wrong client_id", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, `{"access_token":"tok-%d","expires_on":"%d"}`, calls, time.Now().Add(time.Hour).Unix())
	}))
	t.Cleanup(srv.Close)

	source, err := AzureMSI{Resource: "api://zuul", ClientID: "my-identity", Endpoint: srv.URL}.tokenSource()
	if err != nil {
		t.Fatalf("TestMSITokenSource: tokenSource: %s", err)
	}

	tok, err := source(t.Context())
	if err != nil {
		t.Fatalf("TestMSITokenSource: first get: %s", err)
	}
	if tok != "tok-1" {
		t.Errorf("TestMSITokenSource: token = %q, want tok-1", tok)
	}

	// Within the expiry window, the cached token is reused — no second IMDS call.
	tok2, err := source(t.Context())
	if err != nil {
		t.Fatalf("TestMSITokenSource: second get: %s", err)
	}
	if tok2 != "tok-1" || calls != 1 {
		t.Errorf("TestMSITokenSource: token = %q calls = %d, want tok-1 and 1 (cached)", tok2, calls)
	}
}

// TestMSITokenSourceErrors covers config and IMDS failure paths.
func TestMSITokenSourceErrors(t *testing.T) {
	if _, err := (AzureMSI{}).tokenSource(); err == nil {
		t.Errorf("TestMSITokenSourceErrors: empty Resource: got err == nil, want err != nil")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no identity", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	source, err := AzureMSI{Resource: "api://zuul", Endpoint: srv.URL}.tokenSource()
	if err != nil {
		t.Fatalf("TestMSITokenSourceErrors: tokenSource: %s", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = source(ctx)
	switch {
	case err == nil:
		t.Errorf("TestMSITokenSourceErrors: IMDS 400: got err == nil, want err != nil")
	case !errors.Is(err, errors.ErrPermanent):
		t.Errorf("TestMSITokenSourceErrors: IMDS 400: got non-permanent err %q, want permanent (retrying cannot help)", err)
	}
}
