package context

import (
	"testing"
	"time"
)

func TestEnsureDeadline(t *testing.T) {
	// A context without a deadline gains one bounded by the fallback, with a real cancel.
	added, cancel := EnsureDeadline(Background(), time.Minute)
	if cancel == nil {
		t.Fatal("TestEnsureDeadline: got nil CancelFunc for a deadline-less context, want non-nil")
	}
	defer cancel()
	if _, ok := added.Deadline(); !ok {
		t.Error("TestEnsureDeadline: deadline-less context did not gain a deadline")
	}

	// A context that already has a deadline passes through unchanged, with a nil cancel.
	parent, cancelParent := WithTimeout(Background(), 2*time.Minute)
	defer cancelParent()
	got, cancelGot := EnsureDeadline(parent, time.Minute)
	switch {
	case cancelGot != nil:
		t.Error("TestEnsureDeadline: got non-nil CancelFunc for a context with an existing deadline, want nil")
	case got != parent:
		t.Error("TestEnsureDeadline: returned a different context, want the original unchanged")
	}
}
