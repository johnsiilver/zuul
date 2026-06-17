package meta

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/johnsiilver/zuul/internal/raft/meta/metapb"
)

func put(id uint64, grpc string) *metapb.MetaCommand {
	return &metapb.MetaCommand{Cmd: &metapb.MetaCommand_Put{Put: &metapb.Member{ReplicaId: id, ZuulGrpcAddress: grpc}}}
}

func del(id uint64) *metapb.MetaCommand {
	return &metapb.MetaCommand{Cmd: &metapb.MetaCommand_Delete{Delete: id}}
}

// TestStoreApplyAndQuery covers put/update/delete and the member/list reads.
func TestStoreApplyAndQuery(t *testing.T) {
	s := NewStore()
	for _, c := range []*metapb.MetaCommand{put(1, "a:1"), put(2, "b:2"), put(1, "a:11"), del(2)} {
		if _, err := s.Apply(c); err != nil {
			t.Fatalf("TestStoreApplyAndQuery: Apply: got err == %s, want err == nil", err)
		}
	}

	got, err := s.Query(MemberQuery{ReplicaID: 1})
	if err != nil {
		t.Fatalf("TestStoreApplyAndQuery: MemberQuery: got err == %s, want err == nil", err)
	}
	want := &metapb.Member{ReplicaId: 1, ZuulGrpcAddress: "a:11"} // updated value
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("TestStoreApplyAndQuery: member 1: -want +got:\n%s", diff)
	}

	gone, _ := s.Query(MemberQuery{ReplicaID: 2})
	if gone.(*metapb.Member) != nil {
		t.Errorf("TestStoreApplyAndQuery: member 2: got %v, want nil (deleted)", gone)
	}

	list, _ := s.Query(ListQuery{})
	wantList := []*metapb.Member{{ReplicaId: 1, ZuulGrpcAddress: "a:11"}}
	if diff := cmp.Diff(wantList, list, protocmp.Transform()); diff != "" {
		t.Errorf("TestStoreApplyAndQuery: list: -want +got:\n%s", diff)
	}
}

// TestStoreSnapshotRoundTrip proves snapshot -> restore reproduces the state.
func TestStoreSnapshotRoundTrip(t *testing.T) {
	s := NewStore()
	for _, c := range []*metapb.MetaCommand{put(1, "a:1"), put(3, "c:3"), put(2, "b:2")} {
		if _, err := s.Apply(c); err != nil {
			t.Fatalf("TestStoreSnapshotRoundTrip: Apply: got err == %s, want err == nil", err)
		}
	}
	snap := s.Snapshot()

	restored := NewStore()
	restored.Restore(snap)
	if diff := cmp.Diff(snap, restored.Snapshot(), protocmp.Transform()); diff != "" {
		t.Errorf("TestStoreSnapshotRoundTrip: -want +got:\n%s", diff)
	}
}
