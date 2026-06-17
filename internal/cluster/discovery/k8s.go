// Package discovery derives a Zuul cluster's member list from a deployment topology
// instead of a hand-written --peers list.
//
// The Kubernetes path uses StatefulSet + headless-Service DNS: pods get stable,
// ordinal-indexed names (zuul-0.zuul.ns.svc.cluster.local, zuul-1, ...). Those names
// are the Raft and gRPC addresses; dragonboat and gRPC both re-resolve a name to the
// current pod IP on each reconnect, so pod restarts and IP churn need no extra
// handling. This is the static addressing path with the members generated from the
// StatefulSet shape, so it needs no Kubernetes API access — just DNS.
package discovery

import (
	"fmt"
	"strconv"
	"strings"
)

// K8s describes a Zuul cluster deployed as a Kubernetes StatefulSet behind a
// headless Service.
type K8s struct {
	// Name is the StatefulSet name (and the pod-name prefix). Required.
	Name string
	// Service is the governing headless Service name (spec.serviceName). Defaults to
	// Name when empty.
	Service string
	// Namespace is the pods' namespace. Required.
	Namespace string
	// ClusterDomain is the cluster DNS domain. Defaults to "cluster.local".
	ClusterDomain string
	// Replicas is the StatefulSet's replica count. Required, >= 1.
	Replicas int
	// RaftPort and GRPCPort are the (uniform) Raft and gRPC ports. Required.
	RaftPort, GRPCPort int
	// PodName is this pod's name, e.g. "zuul-2" (from the $HOSTNAME env). Required.
	PodName string
}

// Result is a resolved cluster topology: this node's identity plus the full member
// and forwarding-seed maps, all addressed by stable DNS names.
type Result struct {
	// ReplicaID is this node's replica id (ordinal + 1, so it is non-zero).
	ReplicaID uint64
	// RaftAddr and GRPCAddr are this node's own addresses.
	RaftAddr, GRPCAddr string
	// Members maps every replica id to its Raft DNS address (bootstrap).
	Members map[uint64]string
	// Seed maps every replica id to its gRPC DNS address (forwarding seed).
	Seed map[uint64]string
}

// Resolve builds the cluster topology, computing this node's replica id from its pod
// ordinal and every peer's address from the StatefulSet's DNS naming.
func (k K8s) Resolve() (Result, error) {
	if err := k.validate(); err != nil {
		return Result{}, err
	}
	service := k.Service
	if service == "" {
		service = k.Name
	}
	domain := k.ClusterDomain
	if domain == "" {
		domain = "cluster.local"
	}

	ordinal, err := k.ordinal()
	if err != nil {
		return Result{}, err
	}

	members := make(map[uint64]string, k.Replicas)
	seed := make(map[uint64]string, k.Replicas)
	for i := 0; i < k.Replicas; i++ {
		host := fmt.Sprintf("%s-%d.%s.%s.svc.%s", k.Name, i, service, k.Namespace, domain)
		rid := uint64(i + 1)
		members[rid] = fmt.Sprintf("%s:%d", host, k.RaftPort)
		seed[rid] = fmt.Sprintf("%s:%d", host, k.GRPCPort)
	}

	rid := uint64(ordinal + 1)
	return Result{
		ReplicaID: rid,
		RaftAddr:  members[rid],
		GRPCAddr:  seed[rid],
		Members:   members,
		Seed:      seed,
	}, nil
}

// ordinal parses this pod's StatefulSet ordinal from PodName ("<Name>-<ordinal>").
func (k K8s) ordinal() (int, error) {
	prefix := k.Name + "-"
	if !strings.HasPrefix(k.PodName, prefix) {
		return 0, fmt.Errorf("discovery: pod name %q does not start with %q", k.PodName, prefix)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(k.PodName, prefix))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("discovery: pod name %q has no valid ordinal", k.PodName)
	}
	if n >= k.Replicas {
		return 0, fmt.Errorf("discovery: pod ordinal %d is out of range for %d replicas", n, k.Replicas)
	}
	return n, nil
}

func (k K8s) validate() error {
	switch {
	case k.Name == "":
		return fmt.Errorf("discovery.K8s: Name is required")
	case k.Namespace == "":
		return fmt.Errorf("discovery.K8s: Namespace is required")
	case k.Replicas < 1:
		return fmt.Errorf("discovery.K8s: Replicas must be at least 1")
	case k.RaftPort < 1 || k.GRPCPort < 1:
		return fmt.Errorf("discovery.K8s: RaftPort and GRPCPort are required")
	case k.PodName == "":
		return fmt.Errorf("discovery.K8s: PodName is required (set it from the $HOSTNAME env)")
	}
	return nil
}
