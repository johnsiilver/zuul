package discovery

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

// TestK8sResolve covers ordinal→identity mapping and the generated DNS member list.
func TestK8sResolve(t *testing.T) {
	tests := []struct {
		name    string
		k8s     K8s
		want    Result
		wantErr bool
	}{
		{
			name: "Success: pod zuul-1 of 3 derives replica 2 and all members",
			k8s: K8s{
				Name:      "zuul",
				Namespace: "prod",
				Replicas:  3,
				RaftPort:  9001,
				GRPCPort:  8001,
				PodName:   "zuul-1",
			},
			want: Result{
				ReplicaID: 2,
				RaftAddr:  "zuul-1.zuul.prod.svc.cluster.local:9001",
				GRPCAddr:  "zuul-1.zuul.prod.svc.cluster.local:8001",
				Members: map[uint64]string{
					1: "zuul-0.zuul.prod.svc.cluster.local:9001",
					2: "zuul-1.zuul.prod.svc.cluster.local:9001",
					3: "zuul-2.zuul.prod.svc.cluster.local:9001",
				},
				Seed: map[uint64]string{
					1: "zuul-0.zuul.prod.svc.cluster.local:8001",
					2: "zuul-1.zuul.prod.svc.cluster.local:8001",
					3: "zuul-2.zuul.prod.svc.cluster.local:8001",
				},
			},
		},
		{
			name: "Success: distinct headless service and custom cluster domain",
			k8s: K8s{
				Name:          "zuul",
				Service:       "zuul-headless",
				Namespace:     "default",
				ClusterDomain: "k8s.internal",
				Replicas:      1,
				RaftPort:      9001,
				GRPCPort:      8001,
				PodName:       "zuul-0",
			},
			want: Result{
				ReplicaID: 1,
				RaftAddr:  "zuul-0.zuul-headless.default.svc.k8s.internal:9001",
				GRPCAddr:  "zuul-0.zuul-headless.default.svc.k8s.internal:8001",
				Members:   map[uint64]string{1: "zuul-0.zuul-headless.default.svc.k8s.internal:9001"},
				Seed:      map[uint64]string{1: "zuul-0.zuul-headless.default.svc.k8s.internal:8001"},
			},
		},
		{
			name:    "Error: pod name does not match the statefulset name",
			k8s:     K8s{Name: "zuul", Namespace: "p", Replicas: 3, RaftPort: 1, GRPCPort: 2, PodName: "other-0"},
			wantErr: true,
		},
		{
			name:    "Error: ordinal out of range",
			k8s:     K8s{Name: "zuul", Namespace: "p", Replicas: 2, RaftPort: 1, GRPCPort: 2, PodName: "zuul-5"},
			wantErr: true,
		},
		{
			name:    "Error: missing namespace",
			k8s:     K8s{Name: "zuul", Replicas: 1, RaftPort: 1, GRPCPort: 2, PodName: "zuul-0"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		got, err := test.k8s.Resolve()
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestK8sResolve(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestK8sResolve(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestK8sResolve(%s): -want +got:\n%s", test.name, diff)
		}
	}
}
