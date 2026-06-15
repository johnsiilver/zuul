package discovery

import "testing"

// FuzzOrdinal fuzzes the StatefulSet pod-name ordinal parser (input from $HOSTNAME)
// with arbitrary names; it must never panic.
func FuzzOrdinal(f *testing.F) {
	f.Add("zuul", "zuul-0", 3)
	f.Add("zuul", "zuul-", 3)
	f.Add("zuul", "zuul--5", 3)
	f.Add("zuul", "zuul-99999999999999999999999999", 3)
	f.Add("", "-0", 1)
	f.Fuzz(func(t *testing.T, name, podName string, replicas int) {
		k := K8s{Name: name, PodName: podName, Replicas: replicas}
		_, _ = k.ordinal()
	})
}
