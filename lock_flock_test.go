//go:build linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd

package diskqueue

import (
	"errors"
	"testing"
)

// TestDirectoryLockRejectsSecondOpen: two queues over one directory would
// interleave writes into the same segments and destroy each other's cursors. The
// second opener has to be turned away, and the lock has to be released again when
// the first one closes.
func TestDirectoryLockRejectsSecondOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: 4}

	w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := New[uint64](dir, marshalU64, unmarshalU64, opts); !errors.Is(err, ErrLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second open: %v, want ErrLocked (does this filesystem support flock?)", err)
	}
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing releases the lock, and the data is intact.
	w2, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	defer func() { _ = w2.Close() }()
	v, ok, err := w2.NewReader().TryTake()
	if err != nil || !ok || v != 1 {
		t.Fatalf("after reopen: v=%d ok=%v err=%v", v, ok, err)
	}
}
