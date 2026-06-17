package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/retry/exponential"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
)

// imdsEndpoint is the Azure Instance Metadata Service token endpoint, reachable
// from any Azure VM/pod with a managed identity.
const imdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token"

// refreshSkew renews a cached MSI token this long before it expires.
const refreshSkew = 2 * time.Minute

// AzureMSI fetches bearer tokens from Azure Managed Service Identity (the IMDS
// endpoint). The resulting tokens are Entra ID JWTs that a Zuul server validates
// with its OIDC configuration (issuer
// "https://login.microsoftonline.com/<tenant>/v2.0").
type AzureMSI struct {
	// Resource is the audience to request a token for — the server's app ID URI
	// (the zuuld --oidc-audience value). Required.
	Resource string
	// ClientID selects a user-assigned managed identity; empty uses the
	// system-assigned one.
	ClientID string
	// Endpoint overrides the IMDS endpoint (tests); default the Azure IMDS address.
	Endpoint string
}

// imdsToken is the IMDS response shape. expires_on arrives as a string on most
// API versions.
type imdsToken struct {
	AccessToken string `json:"access_token"`
	ExpiresOn   string `json:"expires_on"`
}

// tokenState is the cached MSI token. It is read-mostly (written only on refresh),
// so it lives behind a WProtect: the hot read path is a lock-free atomic load.
type tokenState struct {
	token  string
	expiry time.Time
}

// Copy implements sync.Copier for WProtect.
func (t *tokenState) Copy() *tokenState {
	c := *t
	return &c
}

// msiSource caches the current token and refreshes it through IMDS near expiry.
type msiSource struct {
	cfg  AzureMSI
	boff *exponential.Backoff
	hc   *http.Client

	// state is the current token, read lock-free on every RPC.
	state sync.WProtect[tokenState, *tokenState]
	// refreshing single-flights the background renewal (CAS-guarded).
	refreshing atomic.Bool
	// refreshMu single-flights the actual IMDS request (held across the network, but
	// only by the one goroutine doing a refresh — readers never take it).
	refreshMu sync.Mutex
}

// tokenSource returns a TokenSource backed by this managed identity.
func (a AzureMSI) tokenSource() (func(ctx context.Context) (string, error), error) {
	if a.Resource == "" {
		return nil, fmt.Errorf("zuul: AzureMSI.Resource is required")
	}
	if a.Endpoint == "" {
		a.Endpoint = imdsEndpoint
	}
	boff, err := exponential.New()
	if err != nil {
		return nil, err
	}
	s := &msiSource{cfg: a, boff: boff, hc: &http.Client{Timeout: 5 * time.Second}}
	return s.get, nil
}

// get returns a live token. A still-valid token is served immediately (no lock held
// over the network); within the refresh skew it is renewed in the background so RPCs
// never block on a slow IMDS while a usable token exists. Only a hard-expired (or
// absent) token blocks on a synchronous refresh.
func (s *msiSource) get(ctx context.Context) (string, error) {
	st := s.state.Get()
	now := time.Now()
	switch {
	case st != nil && now.Before(st.expiry.Add(-refreshSkew)):
		return st.token, nil // fresh (lock-free)
	case st != nil && now.Before(st.expiry):
		if s.refreshing.CompareAndSwap(false, true) {
			s.backgroundRefresh()
		}
		return st.token, nil // valid but stale: renew in the background, use it now
	default:
		return s.syncRefresh(ctx) // expired or absent: must refresh now
	}
}

// backgroundRefresh renews the token off the request path, on a detached context so
// it is not cancelled when the triggering RPC ends. The caller has already won the
// refreshing CAS; this clears it when done. The detached context is never cancelled, so
// Submit always enqueues the closure (see worker.Pool.Submit) and the deferred reset
// always runs — the refreshing flag cannot stick.
func (s *msiSource) backgroundRefresh() {
	bg := context.Background()
	context.Pool(bg).Submit(bg, func() {
		defer s.refreshing.Store(false)
		ctx, cancel := context.WithTimeout(bg, 30*time.Second)
		defer cancel()
		s.refreshMu.Lock()
		err := s.boff.Retry(ctx, func(ctx context.Context, _ exponential.Record) error { return s.refresh(ctx) })
		s.refreshMu.Unlock()
		if err != nil {
			// A still-valid token is being served meanwhile; surface the failure so a
			// persistent IMDS problem is visible before the token hard-expires.
			context.Log(ctx).Warn("zuul: MSI background token refresh failed", "err", err.Error())
		}
	})
}

// syncRefresh refreshes the token, single-flighted: concurrent callers wait on
// refreshMu, then return the token the first caller fetched.
func (s *msiSource) syncRefresh(ctx context.Context) (string, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	// Another caller may have refreshed while we waited.
	if st := s.state.Get(); st != nil && time.Now().Before(st.expiry.Add(-refreshSkew)) {
		return st.token, nil
	}

	if err := s.boff.Retry(ctx, func(ctx context.Context, _ exponential.Record) error { return s.refresh(ctx) }); err != nil {
		return "", fmt.Errorf("zuul: MSI token: %w", err)
	}
	st := s.state.Get()
	if st == nil {
		return "", fmt.Errorf("zuul: MSI token: no token after refresh")
	}
	return st.token, nil
}

// refresh performs one IMDS token request, storing the result via WProtect.
func (s *msiSource) refresh(ctx context.Context) error {
	q := url.Values{}
	q.Set("api-version", "2018-02-01")
	q.Set("resource", s.cfg.Resource)
	if s.cfg.ClientID != "" {
		q.Set("client_id", s.cfg.ClientID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.Endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Metadata", "true")

	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("IMDS status %d: %s", resp.StatusCode, body)
		// A 400 is a malformed request (bad Resource/ClientID): retrying cannot help.
		// Other statuses (404 at boot, 429 throttle, 5xx) are transient and retried.
		if resp.StatusCode == http.StatusBadRequest {
			return errors.Permanent(err)
		}
		return err
	}
	var tok imdsToken
	if err := json.Unmarshal(body, &tok); err != nil {
		return errors.E(ctx, errors.CatInternal, errors.TypeMarshal, fmt.Errorf("IMDS response: %w", err))
	}
	if tok.AccessToken == "" {
		return errors.E(ctx, errors.CatInternal, errors.TypeBackend, errors.New("IMDS response has no access_token"))
	}
	expiresOn, err := strconv.ParseInt(tok.ExpiresOn, 10, 64)
	if err != nil {
		return errors.E(ctx, errors.CatInternal, errors.TypeMarshal, fmt.Errorf("IMDS expires_on %q: %w", tok.ExpiresOn, err))
	}
	return s.state.Set(&tokenState{token: tok.AccessToken, expiry: time.Unix(expiresOn, 0)})
}
