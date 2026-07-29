package diskqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// The progress contract: every consume operation either delivers an item, reports
// a loss that it has already stepped past, or ends. There is no third outcome —
// no call may report damage it did not actually get past, because the documented
// recovery loop ("count it and go round again") then spins forever.
//
// comparison.md names this failure mode in Promtail and calls it worse than both
// dropping the record and crashing. These tests exist so it cannot appear here.

// withDeadline runs fn and fails if it has not returned within d. A livelock in a
// consume path would otherwise hang the whole test binary until the panic timeout.
//
// Note what these tests deliberately do NOT do: close the queue. A spinning
// consumer holds the queue mutex, so a deferred Close would block on it and turn
// a clean failure report into the very hang we are trying to detect. The temp
// directory is removed either way.
func withDeadline(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v: it is not making progress", what, d)
	}
}

// TestTakeHeadAlwaysMakesProgress: a corruption report must mean the cursor moved.
// takeHead used to return ErrCorrupt even when skipCorruptSegment had failed and
// nothing had been quarantined, so the caller's retry loop re-read the same bytes
// and got the same answer forever.
func TestTakeHeadAlwaysMakesProgress(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; i < 3; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Damage the framing so the read cannot decode a record, then make the
	// quarantine's header write fail: the store cannot record the skip.
	corruptData(t, s, 0, 0, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	reopenReadOnly(t, s, 0)

	before := s.headOff
	_, _, ok, err := s.takeHead()
	if ok {
		t.Fatal("takeHead delivered an item it could not read")
	}
	if err == nil {
		t.Fatal("takeHead reported success over damage it could not step past")
	}
	if s.headOff == before && errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead returned ErrCorrupt without advancing (headOff=%d): "+
			"the caller's retry loop would spin forever", s.headOff)
	}
}

// TestDrainTerminatesOnUnquarantinableDamage is the same hazard one level up,
// where it is worse: Reader.next loops under the queue mutex, so a livelock there
// wedges every other goroutine too.
func TestDrainTerminatesOnUnquarantinableDamage(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 4; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	corruptData(t, w.st, 0, 0, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	reopenReadOnly(t, w.st, 0)

	withDeadline(t, 10*time.Second, "Drain over damage that cannot be quarantined", func() {
		for range r.Drain(context.Background()) {
		}
	})
	if r.Err() == nil {
		t.Fatal("the iteration ended without reporting why")
	}
}

// TestDrainTerminatesOnCodecReturningErrCorrupt: the iterator's "corruption
// already advanced the queue, keep going" rule keyed on the error alone. A codec
// that wraps ErrCorrupt — entirely legitimate, and not a store fault — hit the
// same continue with the cursor rewound under it, and span.
func TestDrainTerminatesOnCodecReturningErrCorrupt(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, func([]byte) (uint64, error) {
		return 0, fmt.Errorf("codec rejects this record: %w", ErrCorrupt)
	}, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	withDeadline(t, 10*time.Second, "Drain with a codec that wraps ErrCorrupt", func() {
		for range r.Drain(context.Background()) {
		}
	})
	if r.Err() == nil {
		t.Fatal("the iteration ended without reporting why")
	}
	// The record stayed put: a codec error is not disk damage.
	if got := w.Count(); got != 3 {
		t.Fatalf("Count=%d, want 3: a rejected record must not be consumed", got)
	}
	if got := w.Stats().LostRecords; got != 0 {
		t.Fatalf("LostRecords=%d: a codec error is not data loss", got)
	}
}

// TestFollowTerminatesOnDamage: Follow has no upper bound to fall back on, so the
// progress precondition is the only thing that can end it.
func TestFollowTerminatesOnDamage(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 4; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	corruptData(t, w.st, 0, 0, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	reopenReadOnly(t, w.st, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	withDeadline(t, 10*time.Second, "Follow over damage that cannot be quarantined", func() {
		for range r.Follow(ctx) {
		}
	})
}

// TestEmptyDrainsPendingCorruptReports: a segment dropped at open owes the reader
// one ErrCorrupt, but nothing is left to read. Empty() counts that backlog, so a
// Drain that could never pay it down left the queue permanently non-empty.
func TestEmptyDrainsPendingCorruptReports(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	s.pendingCorrupt = 2 // as loadFile would leave it after dropping two segments

	withDeadline(t, 10*time.Second, "draining a pending-corruption backlog", func() {
		for i := 0; i < 100 && !s.empty(); i++ {
			if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
				t.Errorf("report %d: ok=%v err=%v, want ErrCorrupt", i, ok, err)
				return
			}
		}
	})
	if !s.empty() {
		t.Fatalf("still not empty after draining the reports (pendingCorrupt=%d)", s.pendingCorrupt)
	}
}
