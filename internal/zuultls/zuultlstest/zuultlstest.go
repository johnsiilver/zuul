// Package zuultlstest mints throwaway certificates for tests: a CA plus leaf
// certificates usable as both server and client certs on loopback. It follows the
// net/http/httptest convention of a non-test helper package so the same minting
// logic is shared across packages' tests rather than duplicated. It MUST NOT be
// imported by non-test code.
package zuultlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a self-signed certificate authority that mints leaf certificates.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	// serial is incremented for each minted leaf so certificates differ.
	serial int64
}

// NewCA returns a fresh in-memory CA and a pool trusting it.
func NewCA(t *testing.T) (*CA, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("zuultlstest.NewCA: key: %s", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "zuul-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("zuultlstest.NewCA: cert: %s", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("zuultlstest.NewCA: parse: %s", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{Cert: cert, Key: key, serial: 1}, pool
}

// Leaf returns an in-memory leaf certificate with the given Common Name, valid for
// loopback and usable as both a server and client certificate.
func (c *CA) Leaf(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("zuultlstest.Leaf: key: %s", err)
	}
	c.serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(c.serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.Cert, &key.PublicKey, c.Key)
	if err != nil {
		t.Fatalf("zuultlstest.Leaf: cert: %s", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("zuultlstest.Leaf: parse: %s", err)
	}
	return cert, key
}

// Files writes the CA and a leaf certificate (CN "zuul-node") to dir as PEM files
// and returns their paths (caFile, certFile, keyFile).
func (c *CA) Files(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()
	leaf, key := c.Leaf(t, "zuul-node")
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("zuultlstest.Files: marshal key: %s", err)
	}
	caFile = writePEM(t, dir, "ca.pem", "CERTIFICATE", c.Cert.Raw)
	certFile = writePEM(t, dir, "node.pem", "CERTIFICATE", leaf.Raw)
	keyFile = writePEM(t, dir, "node-key.pem", "PRIVATE KEY", keyDER)
	return caFile, certFile, keyFile
}

// GenCerts writes a throwaway CA + loopback node certificate to a temp dir and
// returns their PEM paths. Convenience for the common single-CA case.
func GenCerts(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	ca, _ := NewCA(t)
	return ca.Files(t, t.TempDir())
}

// writePEM writes one PEM block to dir/name and returns the path.
func writePEM(t *testing.T, dir, name, blockType string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("zuultlstest.writePEM %s: %s", path, err)
	}
	return path
}
