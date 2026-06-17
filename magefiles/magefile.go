//go:build mage

// Command mage is the build script for Zuul (run via 'go tool mage <target>'). It
// is stdlib-only on purpose: importing nothing from the mage module keeps it off the
// default build graph (the //go:build mage tag) while guaranteeing it compiles in
// vendor mode — only the mage tool binary itself needs to resolve, which the go.mod
// 'tool' directive anchors.
//
// Dev targets live here; the k8s deploy lifecycle lives in k8s.go.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// run executes a command, streaming its stdout/stderr, and returns its error.
func run(name string, args ...string) error {
	return runIn("", name, args...)
}

// runIn is run with an explicit working directory ("" means the current one).
func runIn(dir, name string, args ...string) error {
	fmt.Fprintf(os.Stderr, "+ %s %s\n", name, strings.Join(args, " "))
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// output captures a command's stdout (stderr is streamed) and returns it.
func output(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	return string(out), err
}

// Gen regenerates the project's protobuf/gRPC code via bufme (one proto per dir,
// generated code colocated). Requires bufme on PATH. bufme also emits grpc-gateway
// (*.pb.gw.go) and OpenAPI (*.swagger.json) byproducts the project does not keep;
// Gen removes them so only *.pb.go and *_vtproto.pb.go remain.
func Gen() error {
	dirs := []string{
		"proto/zuul/v1",
		"internal/raft/forward/forwardpb",
		"internal/raft/meta/metapb",
		"internal/raft/fsm/fsmpb",
	}
	for _, d := range dirs {
		if err := runIn(d, "bufme"); err != nil {
			return fmt.Errorf("gen %s: %w", d, err)
		}
		if err := cleanGenByproducts(d); err != nil {
			return fmt.Errorf("gen %s: %w", d, err)
		}
	}
	return nil
}

// cleanGenByproducts removes the grpc-gateway and OpenAPI files bufme emits but the
// project does not keep (only *.pb.go and *_vtproto.pb.go are tracked).
func cleanGenByproducts(dir string) error {
	for _, pat := range []string{"*.pb.gw.go", "*.swagger.json"} {
		matches, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return err
		}
		for _, m := range matches {
			if err := os.Remove(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// Build compiles zuuld and zuulctl into ./bin (vendored, static-friendly).
func Build() error {
	for _, c := range []string{"zuuld", "zuulctl"} {
		if err := run("go", "build", "-mod=vendor", "-o", "bin/"+c, "./cmd/"+c); err != nil {
			return err
		}
	}
	return nil
}

// Test runs the unit + (in-process) integration test suite.
func Test() error {
	return run("go", "test", "./...")
}

// Race runs the suite under the race detector.
func Race() error {
	return run("go", "test", "-race", "./...")
}

// Integration runs only the in-process integration tests.
func Integration() error {
	return run("go", "test", "./internal/integration/...")
}
