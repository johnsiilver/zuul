package server

import (
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/johnsiilver/zuul/context"

	"github.com/johnsiilver/zuul/internal/cluster/router"
	"github.com/johnsiilver/zuul/internal/raft/fsm"
	zuulv1 "github.com/johnsiilver/zuul/proto/zuul/v1"
)

// fakeBrowseReader serves ListLocksQuery from a per-shard table and WaitersQuery from a
// per-key table, standing in for the local node's FSM read path.
type fakeBrowseReader struct {
	listByShard map[uint64]fsm.LockSummaries
	waitByKey   map[string]fsm.Waiters
}

func (f fakeBrowseReader) Read(ctx context.Context, shardID uint64, query any) (any, error) {
	if q, ok := query.(fsm.WaitersQuery); ok {
		return f.waitByKey[q.Name], nil
	}
	return f.listByShard[shardID], nil
}

func (f fakeBrowseReader) StaleRead(shardID uint64, query any) (any, error) {
	return f.listByShard[shardID], nil
}

// fakeObservers returns a fixed observer set, ignoring the key.
type fakeObservers struct {
	set ObserverSet
}

func (f fakeObservers) Observers(ctx context.Context, key string) (ObserverSet, error) {
	return f.set, nil
}

// TestBrowseListRecords proves ListRecords fans out across shards, merges and sorts the
// results by key, infers election vs lock, and derives the sorted namespace set.
func TestBrowseListRecords(t *testing.T) {
	r, err := router.New([]uint64{1, 2})
	if err != nil {
		t.Fatalf("TestBrowseListRecords: router.New: %s", err)
	}
	reader := fakeBrowseReader{listByShard: map[uint64]fsm.LockSummaries{
		1: {Locks: []fsm.LockSummary{
			{Name: "/bob/lock", Held: true, Holder: "b", Token: 3},
			{Name: "/alice/elect", Held: true, Holder: "a", Token: 4, HasValue: true},
		}},
		2: {Locks: []fsm.LockSummary{
			{Name: "/alice/lock1", Held: true, Holder: "a", Token: 1, QueueDepth: 1},
		}},
	}}
	b, err := NewBrowseServer(BrowseConfig{Router: r, Reader: reader, Observers: fakeObservers{}})
	if err != nil {
		t.Fatalf("TestBrowseListRecords: NewBrowseServer: %s", err)
	}

	resp, err := b.ListRecords(t.Context(), &zuulv1.ListRecordsRequest{})
	if err != nil {
		t.Fatalf("TestBrowseListRecords: ListRecords: %s", err)
	}

	wantKeys := []string{"/alice/elect", "/alice/lock1", "/bob/lock"}
	var gotKeys []string
	for _, rec := range resp.GetRecords() {
		gotKeys = append(gotKeys, rec.GetKey())
	}
	if diff := pretty.Compare(wantKeys, gotKeys); diff != "" {
		t.Errorf("TestBrowseListRecords: record keys -want +got:\n%s", diff)
	}
	if diff := pretty.Compare([]string{"alice", "bob"}, resp.GetNamespaces()); diff != "" {
		t.Errorf("TestBrowseListRecords: namespaces -want +got:\n%s", diff)
	}
	// The election record is classified by its published value.
	for _, rec := range resp.GetRecords() {
		if rec.GetKey() == "/alice/elect" && rec.GetKind() != zuulv1.RecordKind_RECORD_KIND_ELECTION {
			t.Errorf("TestBrowseListRecords: /alice/elect kind = %s, want ELECTION", rec.GetKind())
		}
	}
}

// TestBrowseGetRecord proves GetRecord maps the FSM holder/contenders and the aggregated
// observers, including the partial flag.
func TestBrowseGetRecord(t *testing.T) {
	r, err := router.New([]uint64{1, 2})
	if err != nil {
		t.Fatalf("TestBrowseGetRecord: router.New: %s", err)
	}
	const key = "/alice/lock1"
	reader := fakeBrowseReader{waitByKey: map[string]fsm.Waiters{
		key: {
			Held:     true,
			Holder:   "a",
			Token:    1,
			Revision: 9,
			Entries:  []fsm.WaiterInfo{{ClientID: "b", Seq: 2, Position: 1}},
		},
	}}
	obs := fakeObservers{set: ObserverSet{
		Observers: []Observer{{Identity: "carol", ReplicaID: 7}},
		Partial:   true,
	}}
	b, err := NewBrowseServer(BrowseConfig{Router: r, Reader: reader, Observers: obs})
	if err != nil {
		t.Fatalf("TestBrowseGetRecord: NewBrowseServer: %s", err)
	}

	resp, err := b.GetRecord(t.Context(), &zuulv1.GetRecordRequest{Key: key})
	if err != nil {
		t.Fatalf("TestBrowseGetRecord: GetRecord: %s", err)
	}
	switch {
	case resp.GetRecord().GetHolderClientId() != "a":
		t.Errorf("TestBrowseGetRecord: holder = %q, want a", resp.GetRecord().GetHolderClientId())
	case len(resp.GetContenders()) != 1 || resp.GetContenders()[0].GetClientId() != "b":
		t.Errorf("TestBrowseGetRecord: contenders = %v, want [b]", resp.GetContenders())
	case len(resp.GetObservers()) != 1 || resp.GetObservers()[0].GetIdentity() != "carol":
		t.Errorf("TestBrowseGetRecord: observers = %v, want [carol]", resp.GetObservers())
	case !resp.GetPartial():
		t.Errorf("TestBrowseGetRecord: partial = false, want true")
	}
}
