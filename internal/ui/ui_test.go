package ui

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// fakeBrowser serves canned Browse responses for the handler tests.
type fakeBrowser struct {
	list map[string]*zuulv1.ListRecordsResponse // keyed by request prefix
	get  map[string]*zuulv1.GetRecordResponse   // keyed by record key
}

func (f fakeBrowser) ListRecords(ctx context.Context, req *zuulv1.ListRecordsRequest) (*zuulv1.ListRecordsResponse, error) {
	if r, ok := f.list[req.GetPrefix()]; ok {
		return r, nil
	}
	return &zuulv1.ListRecordsResponse{}, nil
}

func (f fakeBrowser) GetRecord(ctx context.Context, req *zuulv1.GetRecordRequest) (*zuulv1.GetRecordResponse, error) {
	if r, ok := f.get[req.GetKey()]; ok {
		return r, nil
	}
	return &zuulv1.GetRecordResponse{Record: &zuulv1.Record{Key: req.GetKey()}}, nil
}

func testServer(t *testing.T) *Server {
	t.Helper()
	br := fakeBrowser{
		list: map[string]*zuulv1.ListRecordsResponse{
			"": {Namespaces: []string{"alice", "bob"}},
			"/alice": {Records: []*zuulv1.Record{
				{Key: "/alice/lock1", Kind: zuulv1.RecordKind_RECORD_KIND_LOCK, Held: true, HolderClientId: "c1", FencingToken: 1},
			}},
		},
		get: map[string]*zuulv1.GetRecordResponse{
			"/alice/lock1": {
				Record:     &zuulv1.Record{Key: "/alice/lock1", Kind: zuulv1.RecordKind_RECORD_KIND_LOCK, Held: true, HolderClientId: "c1", FencingToken: 1},
				Contenders: []*zuulv1.Contender{{ClientId: "c2", Position: 1}},
				Observers:  []*zuulv1.Observer{{Identity: "watcher", ReplicaId: 2}},
			},
		},
	}
	s, err := New(t.Context(), Config{Bind: "127.0.0.1:0", Browser: br})
	if err != nil {
		t.Fatalf("testServer: New: %s", err)
	}
	return s
}

// TestIndexRendersSelection proves the handler returns 200 and renders the selected
// namespace's records and the selected record's detail (holder, contender, observer),
// i.e. the ?ns=&rec= selection round-trips into the HTML.
func TestIndexRendersSelection(t *testing.T) {
	s := testServer(t)

	req := httptest.NewRequest(http.MethodGet, "/?ns=/alice&rec=/alice/lock1", nil)
	rec := httptest.NewRecorder()
	s.index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("TestIndexRendersSelection: status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"/alice/lock1", "c1", "c2", "watcher", "Namespaces", "Detail"} {
		if !strings.Contains(body, want) {
			t.Errorf("TestIndexRendersSelection: body missing %q", want)
		}
	}
}

// TestIndexNamespaceList proves the default view lists the namespaces with no selection.
func TestIndexNamespaceList(t *testing.T) {
	s := testServer(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	s.index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("TestIndexNamespaceList: status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"/alice", "/bob", "Select a namespace"} {
		if !strings.Contains(body, want) {
			t.Errorf("TestIndexNamespaceList: body missing %q", want)
		}
	}
}

// TestFragments proves each fragment endpoint returns only its own pane (no page shell),
// so the client can update one pane in place without a reload.
func TestFragments(t *testing.T) {
	s := testServer(t)

	tests := []struct {
		name    string
		path    string
		handler http.HandlerFunc
		want    []string
		absent  []string
	}{
		{
			name:    "records fragment for a namespace",
			path:    "/frag/records?ns=/alice",
			handler: s.fragRecords,
			want:    []string{"/alice/lock1", "Records"},
			absent:  []string{"<html", "Namespaces", "Detail"},
		},
		{
			name:    "detail fragment for a record",
			path:    "/frag/detail?rec=/alice/lock1",
			handler: s.fragDetail,
			want:    []string{"/alice/lock1", "c2", "watcher", "Detail"},
			absent:  []string{"<html", "Namespaces", "Records"},
		},
		{
			name:    "namespaces fragment",
			path:    "/frag/namespaces",
			handler: s.fragNamespaces,
			want:    []string{"/alice", "/bob", "Namespaces"},
			absent:  []string{"<html", "Detail"},
		},
	}

	for _, test := range tests {
		req := httptest.NewRequest(http.MethodGet, test.path, nil)
		rec := httptest.NewRecorder()
		test.handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("TestFragments(%s): status = %d, want 200", test.name, rec.Code)
			continue
		}
		body := rec.Body.String()
		for _, w := range test.want {
			if !strings.Contains(body, w) {
				t.Errorf("TestFragments(%s): body missing %q", test.name, w)
			}
		}
		for _, a := range test.absent {
			if strings.Contains(body, a) {
				t.Errorf("TestFragments(%s): body unexpectedly contains %q (should be a bare fragment)", test.name, a)
			}
		}
	}
}

// TestDisplayValue proves the detail-pane value renderer: short UTF-8 passes through,
// non-UTF-8 is hex-encoded (never dumped as mojibake), and an oversized value is capped
// with a byte-count suffix rather than rendered whole into the page.
func TestDisplayValue(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want string
	}{
		{name: "empty value renders empty", in: nil, want: ""},
		{name: "short utf-8 value passes through verbatim", in: []byte("worker-1"), want: "worker-1"},
		{name: "binary (non-utf8) value is hex-encoded", in: []byte{0xff, 0xfe, 0x00, 0x01}, want: "fffe0001"},
		{name: "oversized value is capped with a byte-count suffix", in: bytes.Repeat([]byte("a"), 600), want: strings.Repeat("a", 512) + " … (600 bytes total)"},
	}
	for _, test := range tests {
		if got := displayValue(test.in); got != test.want {
			t.Errorf("TestDisplayValue(%s): got %q, want %q", test.name, got, test.want)
		}
	}
}

// TestHTTPStatus proves a backend error's category maps to the right HTTP status, and an
// unclassified error falls back to 502.
func TestHTTPStatus(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "request error maps to 400", err: errors.E(ctx, errors.CatRequest, errors.TypeBackend, fmt.Errorf("bad")), want: http.StatusBadRequest},
		{name: "not-found maps to 404", err: errors.E(ctx, errors.CatNotFound, errors.TypeBackend, fmt.Errorf("missing")), want: http.StatusNotFound},
		{name: "permission maps to 403", err: errors.E(ctx, errors.CatPermission, errors.TypeBackend, fmt.Errorf("denied")), want: http.StatusForbidden},
		{name: "unavailable maps to 503", err: errors.E(ctx, errors.CatUnavailable, errors.TypeBackend, fmt.Errorf("down")), want: http.StatusServiceUnavailable},
		{name: "unclassified error falls back to 502", err: fmt.Errorf("plain"), want: http.StatusBadGateway},
	}
	for _, test := range tests {
		if got := httpStatus(test.err); got != test.want {
			t.Errorf("TestHTTPStatus(%s): got %d, want %d", test.name, got, test.want)
		}
	}
}

// TestFailDoesNotLeakBackendError proves fail() sends a generic message with the
// category-mapped status and never echoes the backend error detail to the client.
func TestFailDoesNotLeakBackendError(t *testing.T) {
	const secret = "client alice not authorized to browse /bob"
	err := errors.E(t.Context(), errors.CatPermission, errors.TypeBackend, fmt.Errorf("%s", secret))

	rec := httptest.NewRecorder()
	(&Server{}).fail(t.Context(), rec, err)

	if rec.Code != http.StatusForbidden {
		t.Errorf("TestFailDoesNotLeakBackendError: status = %d, want 403", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "alice") || strings.Contains(body, "not authorized") {
		t.Errorf("TestFailDoesNotLeakBackendError: response leaked backend detail: %q", body)
	}
	if !strings.Contains(body, "browse backend error") {
		t.Errorf("TestFailDoesNotLeakBackendError: missing generic message, body = %q", body)
	}
}
