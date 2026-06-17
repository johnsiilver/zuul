// Package ui is an optional, read-only web UI embedded in zuuld. It renders a
// three-pane browser — namespaces, the locks/elections under a selected namespace, and a
// selected record's detail (holder/leader, fencing token, contenders, observers) — over
// the in-process Browse API. It never mutates state. The selection lives in the URL
// (?ns=&rec=), and a manual Refresh re-fetches in place, preserving scroll.
//
// The UI applies no authentication of its own: serve it on a localhost/operator-only
// bind, or front it with TLS. It calls the Browse handlers directly, bypassing the gRPC
// interceptor chain (auth/rate-limit), the same tradeoff Node.Server() documents.
package ui

import (
	"crypto/tls"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net"
	"net/http"
	"time"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/errors"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

//go:embed templates/*.gohtml static/*
var assets embed.FS

// Browser is the read-only Browse API the UI renders. *server.BrowseServer satisfies it.
type Browser interface {
	ListRecords(ctx context.Context, req *zuulv1.ListRecordsRequest) (*zuulv1.ListRecordsResponse, error)
	GetRecord(ctx context.Context, req *zuulv1.GetRecordRequest) (*zuulv1.GetRecordResponse, error)
}

// Config configures a UI Server.
type Config struct {
	// Bind is the HTTP listen address (e.g. "127.0.0.1:9999"). Required.
	Bind string
	// Browser is the in-process Browse API. Required.
	Browser Browser
	// TLS, when non-nil, serves the UI over TLS.
	TLS *tls.Config
}

func (c Config) validate(ctx context.Context) error {
	switch {
	case c.Bind == "":
		return errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("ui.Config: Bind is required"))
	case c.Browser == nil:
		return errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("ui.Config: Browser is required"))
	}
	return nil
}

// Server is the embedded UI HTTP server.
type Server struct {
	cfg  Config
	tmpl *template.Template
	http *http.Server
	lis  net.Listener
}

// New returns a UI Server. It parses the embedded templates and wires the routes; call
// Start to begin serving.
func New(ctx context.Context, cfg Config) (*Server, error) {
	if err := cfg.validate(ctx); err != nil {
		return nil, err
	}
	tmpl, err := template.ParseFS(assets, "templates/*.gohtml")
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("ui.New: parse templates: %w", err))
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, errors.E(ctx, errors.CatInternal, errors.TypeConfig, fmt.Errorf("ui.New: static assets: %w", err))
	}
	s := &Server{cfg: cfg, tmpl: tmpl}
	mux := http.NewServeMux()
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/frag/namespaces", s.fragNamespaces)
	mux.HandleFunc("/frag/records", s.fragRecords)
	mux.HandleFunc("/frag/detail", s.fragDetail)
	mux.HandleFunc("/", s.index)
	// Timeouts: the UI has no auth and may face slow/hostile clients, so bound every phase
	// of a connection rather than leaving the zero-value (unbounded) http.Server.
	s.http = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64 KiB
	}
	return s, nil
}

// Start binds the listener and serves in the background until Close. Bind errors surface
// synchronously; the serve loop runs as a background task.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.Bind)
	if err != nil {
		return errors.E(ctx, errors.CatRequest, errors.TypeConfig, fmt.Errorf("ui: listen on %s: %w", s.cfg.Bind, err))
	}
	if s.cfg.TLS != nil {
		lis = tls.NewListener(lis, s.cfg.TLS)
	}
	s.lis = lis
	serve := func(ctx context.Context) error {
		if err := s.http.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
	if err := context.Tasks(ctx).Once(ctx, "ui-http", serve); err != nil {
		_ = lis.Close()
		return errors.E(ctx, errors.CatInternal, errors.TypeBackend, fmt.Errorf("ui: start: %w", err))
	}
	return nil
}

// Close gracefully stops the UI server, waiting for in-flight requests (bounded by ctx).
func (s *Server) Close(ctx context.Context) error {
	if s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}

// Addr returns the address the UI is listening on (useful when Bind uses port 0).
func (s *Server) Addr() string {
	if s.lis != nil {
		return s.lis.Addr().String()
	}
	return s.cfg.Bind
}
