package router

import "testing"

// TestShardStableAndInRange proves routing is deterministic and always lands on a
// configured shard.
func TestShardStableAndInRange(t *testing.T) {
	shards := []uint64{10, 20, 30, 40}
	r, err := New(shards)
	if err != nil {
		t.Fatalf("TestShardStableAndInRange: New: got err == %s, want err == nil", err)
	}

	valid := map[uint64]bool{}
	for _, s := range shards {
		valid[s] = true
	}

	keys := []string{"", "a", "lock/one", "there-is-no-dana", "only-zuul", "k0", "k1", "k2"}
	for _, k := range keys {
		got := r.Shard(k)
		if !valid[got] {
			t.Errorf("TestShardStableAndInRange(%q): got shard %d, not in %v", k, got, shards)
		}
		if again := r.Shard(k); again != got {
			t.Errorf("TestShardStableAndInRange(%q): got %d then %d, want stable", k, got, again)
		}
	}
}

// TestNewRejectsEmpty proves New requires at least one shard.
func TestNewRejectsEmpty(t *testing.T) {
	tests := []struct {
		name    string
		shards  []uint64
		wantErr bool
	}{
		{name: "Error: no shards", shards: nil, wantErr: true},
		{name: "Success: one shard", shards: []uint64{1}, wantErr: false},
	}
	for _, test := range tests {
		_, err := New(test.shards)
		switch {
		case err == nil && test.wantErr:
			t.Errorf("TestNewRejectsEmpty(%s): got err == nil, want err != nil", test.name)
		case err != nil && !test.wantErr:
			t.Errorf("TestNewRejectsEmpty(%s): got err == %s, want err == nil", test.name, err)
		}
	}
}
