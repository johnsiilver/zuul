package client

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"

	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

func TestMarshalEndpoint(t *testing.T) {
	meta, err := anypb.New(&zuulv1.Member{ReplicaId: 7, ZuulGrpcAddress: "10.0.0.7:8001"})
	if err != nil {
		t.Fatalf("TestMarshalEndpoint: building metadata Any: got err == %s, want err == nil", err)
	}

	tests := []struct {
		name    string
		ep      *zuulv1.Endpoint
		wantErr bool
	}{
		{name: "Error: nil endpoint", ep: nil, wantErr: true},
		{name: "Error: empty ip", ep: &zuulv1.Endpoint{Port: 8443}, wantErr: true},
		{name: "Error: zero port", ep: &zuulv1.Endpoint{Host: "10.0.0.4"}, wantErr: true},
		{name: "Error: port above range", ep: &zuulv1.Endpoint{Host: "10.0.0.4", Port: 70000}, wantErr: true},
		{name: "Success: ipv4 endpoint", ep: &zuulv1.Endpoint{Host: "10.0.0.4", Port: 8443}},
		{name: "Success: ipv6 endpoint", ep: &zuulv1.Endpoint{Host: "2001:db8::1", Port: 8443}},
		{name: "Success: endpoint with metadata", ep: &zuulv1.Endpoint{Host: "10.0.0.4", Port: 8443, Metadata: meta}},
	}

	for _, test := range tests {
		b, err := MarshalEndpoint(test.ep)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestMarshalEndpoint(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestMarshalEndpoint(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}

		got := &zuulv1.Endpoint{}
		if err := got.UnmarshalVT(b); err != nil {
			t.Errorf("TestMarshalEndpoint(%s): decoding marshaled bytes: got err == %s, want err == nil", test.name, err)
			continue
		}
		if diff := cmp.Diff(test.ep, got, protocmp.Transform()); diff != "" {
			t.Errorf("TestMarshalEndpoint(%s): round-trip mismatch (-want +got):\n%s", test.name, diff)
		}
	}
}

func TestDecodeEndpoint(t *testing.T) {
	mustMarshal := func(ep *zuulv1.Endpoint) []byte {
		b, err := ep.MarshalVT()
		if err != nil {
			t.Fatalf("TestDecodeEndpoint: marshaling fixture: got err == %s, want err == nil", err)
		}
		return b
	}

	tests := []struct {
		name     string
		value    []byte
		wantAddr string
		wantErr  bool
	}{
		{name: "Error: empty value is not a dialable endpoint", value: nil, wantErr: true},
		{name: "Error: garbage bytes", value: []byte{0xff, 0xff, 0xff, 0xff}, wantErr: true},
		{name: "Error: endpoint missing port", value: mustMarshal(&zuulv1.Endpoint{Host: "10.0.0.4"}), wantErr: true},
		{name: "Success: ipv4 resolves to host:port", value: mustMarshal(&zuulv1.Endpoint{Host: "10.0.0.4", Port: 8443}), wantAddr: "10.0.0.4:8443"},
		{name: "Success: ipv6 resolves to bracketed host:port", value: mustMarshal(&zuulv1.Endpoint{Host: "2001:db8::1", Port: 8443}), wantAddr: "[2001:db8::1]:8443"},
		{name: "Success: dns host resolves to host:port", value: mustMarshal(&zuulv1.Endpoint{Host: "db.prod.svc.cluster.local", Port: 8443}), wantAddr: "db.prod.svc.cluster.local:8443"},
	}

	for _, test := range tests {
		ep, err := decodeEndpoint(test.value)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestDecodeEndpoint(%s): got err == nil, want err != nil", test.name)
			continue
		case err != nil && !test.wantErr:
			t.Errorf("TestDecodeEndpoint(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
		if addr := (Master{Endpoint: ep}).Address(); addr != test.wantAddr {
			t.Errorf("TestDecodeEndpoint(%s): got addr == %q, want %q", test.name, addr, test.wantAddr)
		}
	}
}

func TestMasterFromInfo(t *testing.T) {
	ipv4 := mustEndpoint(t, &zuulv1.Endpoint{Host: "10.0.0.4", Port: 8443})

	tests := []struct {
		name        string
		info        Leader
		wantDeliver bool
		wantAddr    string
	}{
		{
			name:        "Error: leaderless yields no master",
			info:        Leader{Has: false},
			wantDeliver: false,
		},
		{
			name:        "Error: leader with non-endpoint value yields no master",
			info:        Leader{Has: true, Value: []byte("not-a-proto-endpoint")},
			wantDeliver: false,
		},
		{
			name:        "Success: leader with valid endpoint resolves a master",
			info:        Leader{Has: true, ID: "node-a", Token: 3, Revision: 11, Value: ipv4},
			wantDeliver: true,
			wantAddr:    "10.0.0.4:8443",
		},
	}

	for _, test := range tests {
		m, deliver := masterFromInfo(test.info)
		if deliver != test.wantDeliver {
			t.Errorf("TestMasterFromInfo(%s): got deliver == %v, want %v", test.name, deliver, test.wantDeliver)
			continue
		}
		if !deliver {
			continue
		}
		if m.Address() != test.wantAddr {
			t.Errorf("TestMasterFromInfo(%s): got Address == %q, want %q", test.name, m.Address(), test.wantAddr)
		}
		if m.LeaderID != test.info.ID || m.Token != test.info.Token || m.Revision != test.info.Revision {
			t.Errorf("TestMasterFromInfo(%s): got LeaderID/Token/Revision == %q/%d/%d, want %q/%d/%d", test.name, m.LeaderID, m.Token, m.Revision, test.info.ID, test.info.Token, test.info.Revision)
		}
	}
}

// mustEndpoint marshals ep with MarshalEndpoint, failing the test on error.
func mustEndpoint(t *testing.T, ep *zuulv1.Endpoint) []byte {
	t.Helper()
	b, err := MarshalEndpoint(ep)
	if err != nil {
		t.Fatalf("mustEndpoint: got err == %s, want err == nil", err)
	}
	return b
}
