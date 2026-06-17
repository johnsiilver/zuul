package forward

import (
	"slices"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/johnsiilver/zuul/internal/lock/watch"
	"github.com/johnsiilver/zuul/internal/raft/forward/forwardpb"
)

// TestServerListObservers proves the Forwarder reports the local hub's tracked observers
// for a key, stamped with this node's replica id, and omits untracked contenders.
func TestServerListObservers(t *testing.T) {
	hub := watch.New()
	a := hub.Subscribe(watch.SubArgs{Key: "/a/lock", Identity: "alice", Track: true})
	b := hub.Subscribe(watch.SubArgs{Key: "/a/lock", Identity: "bob", Track: true})
	c := hub.Subscribe(watch.SubArgs{Key: "/a/lock", Identity: "x"}) // contender, untracked
	defer a.Close()
	defer b.Close()
	defer c.Close()

	srv := NewServer(fakeLocal{}, hub)
	resp, err := srv.ListObservers(t.Context(), &forwardpb.ListObserversRequest{Key: "/a/lock"})
	if err != nil {
		t.Fatalf("TestServerListObservers: got err == %s, want err == nil", err)
	}
	if resp.GetReplicaId() != 1 {
		t.Errorf("TestServerListObservers: replica id = %d, want 1", resp.GetReplicaId())
	}
	var got []string
	for _, e := range resp.GetObservers() {
		got = append(got, e.GetIdentity())
	}
	slices.Sort(got)
	if diff := pretty.Compare([]string{"alice", "bob"}, got); diff != "" {
		t.Errorf("TestServerListObservers: identities -want +got:\n%s", diff)
	}
}
