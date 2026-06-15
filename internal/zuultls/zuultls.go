// Package zuultls builds mutual-TLS configurations for Zuul's gRPC planes (the
// node-to-node forward plane and the client-facing API) from the same CA + node
// certificate + key files that dragonboat's MutualTLS uses for the Raft plane. So
// one set of credentials secures all three planes.
package zuultls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerConfig returns a TLS config for a gRPC server that requires and verifies a
// client certificate signed by the CA (full mutual TLS).
func ServerConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cert, pool, err := load(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ServerOneWayConfig returns a TLS config for server-TLS mode: clients get an
// encrypted, server-authenticated channel without needing certificates of their own
// (they authenticate with bearer tokens instead), while a client certificate — when
// one IS presented, as peer nodes do on the forward plane — is still verified
// against the CA so the node can recognize peers.
func ServerOneWayConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cert, pool, err := load(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientRootsConfig returns a TLS config for a certificate-less client of a
// server-TLS node: it verifies the server against the CA and presents nothing.
func ClientRootsConfig(caFile string) (*tls.Config, error) {
	pool, err := LoadCAPool(caFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}, nil
}

// LoadCAPool reads a PEM CA file into a certificate pool.
func LoadCAPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("zuultls: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("zuultls: no certificates found in CA file %q", caFile)
	}
	return pool, nil
}

// ClientConfig returns a TLS config for a gRPC client that presents its certificate
// and verifies the server against the CA.
func ClientConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	cert, pool, err := load(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// load reads the node key pair and the CA pool.
func load(caFile, certFile, keyFile string) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("zuultls: load key pair: %w", err)
	}
	pool, err := LoadCAPool(caFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return cert, pool, nil
}
