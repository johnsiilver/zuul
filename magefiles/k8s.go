//go:build mage

// The k8s deploy lifecycle, encoding the previously-manual kind flow: build the
// image, spin up a local kind cluster, side-load the image, apply the kind overlay,
// wait for rollout, verify a 3-node cluster formed via DNS discovery, and tear down.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	kindCluster = "zuul-test"
	kindCtx     = "kind-zuul-test"
	image       = "zuul:test"
)

// Image builds the zuul:test container image from deploy/Dockerfile.
func Image() error {
	return run("docker", "build", "-f", "deploy/Dockerfile", "-t", image, ".")
}

// K8sUp creates the kind cluster, or does nothing if it already exists.
func K8sUp() error {
	if kindClusterExists() {
		fmt.Fprintf(os.Stderr, "kind cluster %q already exists\n", kindCluster)
		return nil
	}
	return run("kind", "create", "cluster", "--name", kindCluster, "--wait", "90s")
}

// K8sDown deletes the kind cluster (no-op if absent).
func K8sDown() error {
	if !kindClusterExists() {
		return nil
	}
	return run("kind", "delete", "cluster", "--name", kindCluster)
}

// K8sLoad builds the image and side-loads it into the kind node (no registry).
func K8sLoad() error {
	if err := Image(); err != nil {
		return err
	}
	return run("kind", "load", "docker-image", image, "--name", kindCluster)
}

// K8sDeploy brings the cluster up, loads the image, applies the kind overlay, and
// waits for the StatefulSet to roll out (readiness == every shard has a leader).
func K8sDeploy() error {
	if err := K8sUp(); err != nil {
		return err
	}
	if err := K8sLoad(); err != nil {
		return err
	}
	// Render the kind overlay and apply it. The overlay reads zuul.yaml one directory
	// up, which kustomize's default restrictor forbids; LoadRestrictionsNone allows it
	// (we trust our own files). Render+pipe rather than `apply -k` so the flag applies.
	manifest, err := output("kubectl", "kustomize", "--load-restrictor", "LoadRestrictionsNone", "deploy/k8s/overlays/kind")
	if err != nil {
		return fmt.Errorf("kustomize render: %w", err)
	}
	if err := applyStdin(manifest); err != nil {
		return err
	}
	return run("kubectl", "--context", kindCtx, "rollout", "status", "statefulset/zuul", "--timeout=120s")
}

// applyStdin pipes a rendered manifest into `kubectl apply -f -`.
func applyStdin(manifest string) error {
	fmt.Fprintln(os.Stderr, "+ kubectl apply -f - (rendered kind overlay)")
	cmd := exec.Command("kubectl", "--context", kindCtx, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// K8sVerify deploys, then proves a healthy 3-node cluster via zuulctl over a
// port-forward.
func K8sVerify() error {
	if err := K8sDeploy(); err != nil {
		return err
	}
	if err := Build(); err != nil { // provides ./bin/zuulctl
		return err
	}
	return verify()
}

// K8sE2E runs the full lifecycle and ALWAYS tears the cluster down, even on failure.
func K8sE2E() (err error) {
	defer func() {
		if derr := K8sDown(); derr != nil && err == nil {
			err = derr
		}
	}()
	return K8sVerify()
}

// kindClusterExists reports whether the kind cluster is already running.
func kindClusterExists() bool {
	out, err := output("kind", "get", "clusters")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == kindCluster {
			return true
		}
	}
	return false
}

// verify proves the deployed cluster serves and has 3 members, via a port-forward to
// the client Service (falling back to a pod if the Service forward flakes).
func verify() error {
	var lastErr error
	for _, target := range []string{"service/zuul-client", "pod/zuul-0"} {
		if err := verifyVia(target); err == nil {
			return nil
		} else {
			fmt.Fprintf(os.Stderr, "verify via %s failed: %v\n", target, err)
			lastErr = err
		}
	}
	return fmt.Errorf("k8s verify failed: %w", lastErr)
}

// verifyVia opens a port-forward to target, then polls zuulctl until the cluster is
// serving (health) AND all 3 nodes have announced themselves into the meta shard
// (members). Membership is eventually consistent — nodes announce in the background
// after boot — so this retries within the deadline rather than checking once.
func verifyVia(target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	pf := exec.CommandContext(ctx, "kubectl", "--context", kindCtx, "port-forward", target, "18001:8001")
	pf.Stderr = os.Stderr
	if err := pf.Start(); err != nil {
		return fmt.Errorf("port-forward %s: %w", target, err)
	}
	defer func() { _ = pf.Process.Kill() }()

	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		health, herr := output("./bin/zuulctl", "--addr", "127.0.0.1:18001", "health")
		if herr != nil {
			lastErr = herr
			time.Sleep(time.Second)
			continue
		}
		members, merr := output("./bin/zuulctl", "--addr", "127.0.0.1:18001", "members")
		if merr != nil {
			lastErr = merr
			time.Sleep(time.Second)
			continue
		}
		if n := countMembers(members); n != 3 {
			lastErr = fmt.Errorf("3 members not yet announced (have %d)", n)
			time.Sleep(time.Second)
			continue
		}
		fmt.Fprint(os.Stderr, "health: ", health)
		fmt.Fprintln(os.Stderr, "members: 3 (cluster formed via DNS discovery)")
		return nil
	}
	return fmt.Errorf("cluster not fully formed in time: %w", lastErr)
}

// countMembers counts data rows in `zuulctl members` output (rows whose first field
// is a numeric replica id), ignoring the header.
func countMembers(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if _, err := strconv.Atoi(fields[0]); err == nil {
			n++
		}
	}
	return n
}
