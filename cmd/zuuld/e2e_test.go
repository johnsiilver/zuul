//go:build e2e

// This e2e test drives the real Kubernetes deploy: it shells out to the mage script
// (`go tool mage k8sE2E`), which builds the image, spins up a local kind cluster,
// side-loads the image, applies the kind overlay, waits for rollout, verifies a
// 3-node cluster formed via DNS discovery, and tears the cluster down.
//
// It is gated behind `//go:build e2e` so the default `go test ./...` never spins up
// kind. Run it with:
//
//	go test -tags e2e -timeout 15m ./cmd/zuuld
//
// It requires docker, kind, and kubectl on PATH (and a running Docker daemon); it
// skips otherwise. All the orchestration lives in the mage script, not here — this
// test is a thin, CI-friendly driver that asserts the deploy flow still works.
package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestK8sDeployE2E runs the full kind deploy lifecycle via mage and fails (with the
// mage output) if it does not succeed.
func TestK8sDeployE2E(t *testing.T) {
	for _, bin := range []string{"docker", "kind", "kubectl", "go"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("TestK8sDeployE2E: requires %q on PATH: %s", bin, err)
		}
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Skipf("TestK8sDeployE2E: docker daemon not available: %s\n%s", err, out)
	}

	root := repoRoot(t)
	cmd := exec.Command("go", "tool", "mage", "k8sE2E")
	cmd.Dir = root // mage runs from the repo root so it finds the vendored tool + deploy/ paths
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("TestK8sDeployE2E: `go tool mage k8sE2E` failed: %s\n%s", err, out)
	}
	t.Logf("TestK8sDeployE2E: deploy verified\n%s", out)
}

// repoRoot returns the module root, derived from this file's location
// (<root>/cmd/zuuld/e2e_test.go).
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("repoRoot: runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}
