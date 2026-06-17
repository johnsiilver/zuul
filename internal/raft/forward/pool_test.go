package forward

import (
	"testing"
)

// TestPoolEvict proves evict drops the cached connection so a later request re-dials a
// fresh one (reclaiming a connection to a departed peer), and that evicting an unknown
// address is a harmless no-op.
func TestPoolEvict(t *testing.T) {
	const addr = "127.0.0.1:1"
	p := newPool(nil) // insecure transport credentials
	defer p.close()

	if _, err := p.client(addr); err != nil {
		t.Fatalf("TestPoolEvict: client: %s", err)
	}
	cc := p.conns[addr]
	if cc == nil {
		t.Fatalf("TestPoolEvict: connection was not cached")
	}

	p.evict(addr)
	if _, ok := p.conns[addr]; ok {
		t.Fatalf("TestPoolEvict: connection still cached after evict")
	}

	if _, err := p.client(addr); err != nil {
		t.Fatalf("TestPoolEvict: re-dial: %s", err)
	}
	if p.conns[addr] == cc {
		t.Errorf("TestPoolEvict: re-dial reused the evicted connection, want a fresh one")
	}

	p.evict("missing:1") // unknown address: must not panic
}
