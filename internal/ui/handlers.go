package ui

import (
	"bytes"
	"fmt"
	"net/http"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/johnsiilver/zuul/context"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// pageVM is the whole page: the namespace list, the records under the selected
// namespace, and the selected record's detail. NS and Rec come from the URL so a reload
// (or in-place refresh) restores the selection.
type pageVM struct {
	NS         string     // selected namespace / search prefix (a path like "/alice")
	Namespaces []string   // owner segments present cluster-wide
	Records    []recordVM // records under NS
	Rec        string     // selected record key
	Detail     *detailVM  // detail of Rec, when selected
}

type recordVM struct {
	Key        string
	Kind       string
	Holder     string
	Token      uint64
	QueueDepth uint32
	Selected   bool
}

type detailVM struct {
	Key        string
	Kind       string
	Held       bool
	Holder     string
	Token      uint64
	Value      string
	Contenders []contenderVM
	Observers  []observerVM
	Partial    bool
}

type contenderVM struct {
	ClientID string
	Position uint32
}

type observerVM struct {
	Identity  string
	ReplicaID uint64
}

// index renders the full three-pane browser for the ?ns= and ?rec= selection. It is the
// entry point and deep-link/reload target; panes thereafter update via the fragment
// endpoints (see fragment) without a full reload.
func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	ctx := context.Attach(r.Context())
	ns := r.URL.Query().Get("ns")
	rec := r.URL.Query().Get("rec")

	vm := pageVM{NS: ns, Rec: rec}
	var err error
	if vm.Namespaces, err = s.namespaces(ctx); err != nil {
		s.fail(ctx, w, err)
		return
	}
	if ns != "" {
		if vm.Records, err = s.records(ctx, ns, rec); err != nil {
			s.fail(ctx, w, err)
			return
		}
	}
	if rec != "" {
		if vm.Detail, err = s.detail(ctx, rec); err != nil {
			s.fail(ctx, w, err)
			return
		}
	}
	s.render(ctx, w, "index.gohtml", vm)
}

// fragNamespaces renders just the namespaces pane (used by Refresh).
func (s *Server) fragNamespaces(w http.ResponseWriter, r *http.Request) {
	ctx := context.Attach(r.Context())
	ns := r.URL.Query().Get("ns")
	names, err := s.namespaces(ctx)
	if err != nil {
		s.fail(ctx, w, err)
		return
	}
	s.render(ctx, w, "ns-pane", pageVM{NS: ns, Namespaces: names})
}

// fragRecords renders just the records pane for the selected namespace (used when a
// namespace is clicked and by Refresh).
func (s *Server) fragRecords(w http.ResponseWriter, r *http.Request) {
	ctx := context.Attach(r.Context())
	ns := r.URL.Query().Get("ns")
	rec := r.URL.Query().Get("rec")
	recs, err := s.records(ctx, ns, rec)
	if err != nil {
		s.fail(ctx, w, err)
		return
	}
	s.render(ctx, w, "records-pane", pageVM{NS: ns, Rec: rec, Records: recs})
}

// fragDetail renders just the detail pane for the selected record (used when a record is
// clicked and by Refresh).
func (s *Server) fragDetail(w http.ResponseWriter, r *http.Request) {
	ctx := context.Attach(r.Context())
	rec := r.URL.Query().Get("rec")
	d, err := s.detail(ctx, rec)
	if err != nil {
		s.fail(ctx, w, err)
		return
	}
	s.render(ctx, w, "detail-pane", pageVM{Rec: rec, Detail: d})
}

// namespaces returns the distinct namespaces present cluster-wide.
func (s *Server) namespaces(ctx context.Context) ([]string, error) {
	all, err := s.cfg.Browser.ListRecords(ctx, &zuulv1.ListRecordsRequest{})
	if err != nil {
		return nil, err
	}
	return all.GetNamespaces(), nil
}

// records returns the records under ns, marking the one matching rec as selected.
func (s *Server) records(ctx context.Context, ns, rec string) ([]recordVM, error) {
	if ns == "" {
		return nil, nil
	}
	resp, err := s.cfg.Browser.ListRecords(ctx, &zuulv1.ListRecordsRequest{Prefix: ns})
	if err != nil {
		return nil, err
	}
	var out []recordVM
	for _, rc := range resp.GetRecords() {
		out = append(out, recordVM{
			Key:        rc.GetKey(),
			Kind:       kindStr(rc.GetKind()),
			Holder:     rc.GetHolderClientId(),
			Token:      rc.GetFencingToken(),
			QueueDepth: rc.GetQueueDepth(),
			Selected:   rc.GetKey() == rec,
		})
	}
	return out, nil
}

// detail returns the full detail of rec, or nil when rec is empty.
func (s *Server) detail(ctx context.Context, rec string) (*detailVM, error) {
	if rec == "" {
		return nil, nil
	}
	d, err := s.cfg.Browser.GetRecord(ctx, &zuulv1.GetRecordRequest{Key: rec})
	if err != nil {
		return nil, err
	}
	dv := &detailVM{
		Key:     d.GetRecord().GetKey(),
		Kind:    kindStr(d.GetRecord().GetKind()),
		Held:    d.GetRecord().GetHeld(),
		Holder:  d.GetRecord().GetHolderClientId(),
		Token:   d.GetRecord().GetFencingToken(),
		Value:   displayValue(d.GetValue()),
		Partial: d.GetPartial(),
	}
	for _, c := range d.GetContenders() {
		dv.Contenders = append(dv.Contenders, contenderVM{ClientID: c.GetClientId(), Position: c.GetPosition()})
	}
	for _, o := range d.GetObservers() {
		dv.Observers = append(dv.Observers, observerVM{Identity: o.GetIdentity(), ReplicaID: o.GetReplicaId()})
	}
	return dv, nil
}

// render executes a named template into a buffer first, then writes it. Buffering means a
// mid-execution template error becomes a clean 500 (nothing has been sent yet) instead of a
// 200 with a half-rendered page.
func (s *Server) render(ctx context.Context, w http.ResponseWriter, name string, vm pageVM) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, vm); err != nil {
		context.Log(ctx).Error("ui: render "+name, "err", err.Error())
		http.Error(w, "render error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// fail logs a Browse-backend error in full and returns a generic message with a
// status mapped from the error's category. It deliberately does NOT echo err.Error() to the
// client: the UI is a client of the in-process Browse API and its errors carry internal
// detail (authorization, shard/transport state) that must not leak to the browser.
func (s *Server) fail(ctx context.Context, w http.ResponseWriter, err error) {
	context.Log(ctx).Error("ui: browse backend", "err", err.Error())
	http.Error(w, "browse backend error", httpStatus(err))
}

// httpStatus maps a zuul error's gRPC code (Browse errors implement GRPCStatus) to an HTTP
// status; anything unrecognized is a 502 (the backend call failed from the UI's view).
func httpStatus(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.Unimplemented:
		return http.StatusNotImplemented
	default:
		return http.StatusBadGateway
	}
}

// displayValue renders an election's published value for the detail pane. The value is
// arbitrary bytes up to the server's max message size, so it is length-capped and shown as
// hex when it is not valid UTF-8, rather than dumped verbatim into the page.
func displayValue(b []byte) string {
	const max = 512
	suffix := ""
	if len(b) > max {
		suffix = fmt.Sprintf(" … (%d bytes total)", len(b))
		b = b[:max]
	}
	if utf8.Valid(b) {
		return string(b) + suffix
	}
	return fmt.Sprintf("%x%s", b, suffix)
}

// kindStr renders a RecordKind for display.
func kindStr(k zuulv1.RecordKind) string {
	switch k {
	case zuulv1.RecordKind_RECORD_KIND_LOCK:
		return "lock"
	case zuulv1.RecordKind_RECORD_KIND_ELECTION:
		return "election"
	default:
		return "unknown"
	}
}
