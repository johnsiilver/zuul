package consensus

import "testing"

// TestConfigValidate covers the required-field checks, including GRPCAddr (recorded
// in the meta shard for forwarding, so an empty value makes the node unroutable).
func TestConfigValidate(t *testing.T) {
	base := func() Config {
		return Config{
			ReplicaID: 1,
			RaftAddr:  "127.0.0.1:9001",
			GRPCAddr:  "127.0.0.1:8001",
			DataDir:   "zuul",
			Shards:    []uint64{1},
			Members:   map[uint64]string{1: "127.0.0.1:9001"},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "Success: complete config", mutate: func(*Config) {}},
		{name: "Error: missing ReplicaID", mutate: func(c *Config) { c.ReplicaID = 0 }, wantErr: true},
		{name: "Error: missing RaftAddr", mutate: func(c *Config) { c.RaftAddr = "" }, wantErr: true},
		{name: "Error: missing GRPCAddr", mutate: func(c *Config) { c.GRPCAddr = "" }, wantErr: true},
		{name: "Error: missing DataDir", mutate: func(c *Config) { c.DataDir = "" }, wantErr: true},
		{name: "Error: no shards", mutate: func(c *Config) { c.Shards = nil }, wantErr: true},
	}
	for _, test := range tests {
		c := base()
		test.mutate(&c)
		err := c.validate()
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestConfigValidate(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestConfigValidate(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}
