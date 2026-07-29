package diskqueue

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// These tests fault-inject *while the store is running* — the previous fault
// tests (recovery_fault_test.go) only disturb the bytes on disk between a close
// and a reopen. The point here is that no I/O failure may panic, wedge a mutex,
// silently lose a record, or leave the store lying about durability: every one of
// them has to come back as an error.
//
// Two injections cover the interesting shapes, both applied behind the store's
// back through its own handles:
//
//	breakHandle     — the segment's descriptor is closed: every read, write and
//	                  fsync on it fails, like a device that went away.
//	reopenReadOnly  — the descriptor is swapped for a read-only one: reads still
//	                  work, every write fails, like a filesystem remounted ro.
//
// Neither needs a seam in the hot path, so the zero-allocation benchmark is
// untouched.

// breakHandle closes segment idx's descriptor without telling the store. df.f
// stays non-nil, so nothing reopens it and the failures are reproducible.
func breakHandle(t *testing.T, s *store, idx int) {
	t.Helper()
	if f := s.files[idx].f; f != nil {
		_ = f.Close()
	}
}

// reopenReadOnly swaps segment idx's descriptor for a read-only one: pread keeps
// working, every pwrite fails with EBADF.
func reopenReadOnly(t *testing.T, s *store, idx int) {
	t.Helper()
	swapHandle(t, s, idx, os.O_RDONLY)
}

// repairHandle gives segment idx a healthy descriptor again, so a test can show
// that a latched failure outlives the condition that caused it.
func repairHandle(t *testing.T, s *store, idx int) {
	t.Helper()
	swapHandle(t, s, idx, os.O_RDWR)
}

func swapHandle(t *testing.T, s *store, idx, flag int) {
	t.Helper()
	df := s.files[idx]
	if df.f != nil {
		_ = df.f.Close()
	}
	f, err := os.OpenFile(s.filePath(df.num), flag, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	df.f = f
}

// openFDs counts the process's open descriptors, for leak assertions.
func openFDs(t *testing.T) int {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("descriptor counting needs /proc")
	}
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("no /proc/self/fd")
	}
	return len(ents)
}

// TestStoreSyncFailureLatches pins the fsync-gate stance: a failed fsync is not
// retried and not forgotten. The kernel reports a writeback error once and then
// drops the pages, so a second fsync would report success over data that is
// already gone — the store must keep saying no instead.
func TestStoreSyncFailureLatches(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0) // noSync: records stay dirty until sync()
	mustAppend(t, s, idxRec(0))
	breakHandle(t, s, 0)

	err := s.sync()
	if !errors.Is(err, ErrIO) {
		t.Fatalf("sync after a broken fsync: %v, want ErrIO", err)
	}
	if !errors.Is(s.failure(), ErrIO) {
		t.Fatalf("failure() = %v, want the latched ErrIO", s.failure())
	}
	if got := s.sync(); !errors.Is(got, ErrIO) {
		t.Fatalf("second sync: %v, want the latched ErrIO", got)
	}
	if got := s.append(idxRec(1)); !errors.Is(got, ErrIO) {
		t.Fatalf("append on a poisoned store: %v, want ErrIO", got)
	}
	if got := s.commitTo(s.writeOffset()); !errors.Is(got, ErrIO) {
		t.Fatalf("commit on a poisoned store: %v, want ErrIO", got)
	}
	if got := s.count(); got != 1 {
		t.Fatalf("count=%d, want 1: the refused append must not have landed", got)
	}
}

// TestStoreWriteFailureIsRetriable is the counterpart: a failed *write* leaves
// the store consistent and unpoisoned, because nothing was published and nothing
// can have been dropped by the kernel. The append simply did not happen.
func TestStoreWriteFailureIsRetriable(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(0))
	count, size, woff := s.count(), s.size(), s.writeOffset()

	reopenReadOnly(t, s, 0)
	if err := s.append(idxRec(1)); err == nil {
		t.Fatal("append into a read-only segment should fail")
	} else if errors.Is(err, ErrIO) {
		t.Fatalf("a write failure must not poison the store: %v", err)
	}
	if s.count() != count || s.size() != size || s.writeOffset() != woff {
		t.Fatalf("failed append moved the cursors: count=%d/%d size=%d/%d off=%d/%d",
			s.count(), count, s.size(), size, s.writeOffset(), woff)
	}
	if s.failure() != nil {
		t.Fatalf("store poisoned by a write failure: %v", s.failure())
	}
	// The record that was already there still reads back.
	p, _, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("read after a failed append: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
}

// TestStoreCommitFailureSurfaced: commitTo used to return nothing at all, so a
// commit that never reached disk looked exactly like one that did.
func TestStoreCommitFailureSurfaced(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; i < 3; i++ {
		mustAppend(t, s, idxRec(i))
	}
	_, off, ok, err := s.takeHead()
	if err != nil || !ok {
		t.Fatalf("takeHead: ok=%v err=%v", ok, err)
	}
	reopenReadOnly(t, s, 0)
	if err := s.commitTo(off); err == nil {
		t.Fatal("commit whose header write fails must report it")
	}
}

// TestStoreReadFailureIsNotEmptiness: a segment that cannot be read must not read
// as "queue drained".
func TestStoreReadFailureIsNotEmptiness(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(0))
	breakHandle(t, s, 0)
	p, _, ok, err := s.takeHead()
	if err == nil {
		t.Fatalf("takeHead on a broken handle: p=%v ok=%v, want an error", p, ok)
	}
	if ok {
		t.Fatal("takeHead reported an item it could not read")
	}
}

// TestStoreTruncatedSegmentIsCorruption: a segment that lost bytes is corruption,
// not a SegmentSize mismatch — a different (and unrecoverable) complaint that
// would have shut the recovery path out of a case it handles. The records that
// are still there survive; the cut tail is counted as discarded.
func TestStoreTruncatedSegmentIsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// Cut the file after the first record: the header still publishes all three.
	path := filepath.Join(dir, "data.00000001")
	if err := os.Truncate(path, headerSize+idxRecLen+5); err != nil {
		t.Fatal(err)
	}

	rec, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("open of a truncated segment: %v, want the surviving records", err)
	}
	defer func() { _ = rec.close() }()
	got, events := drainRecovering(t, rec)
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("delivered %v, want just the record that survived the cut", got)
	}
	if events == 0 {
		t.Fatal("the truncation was not reported to the reader")
	}
	if rec.corruptionCount() == 0 {
		t.Fatal("the truncation should have been counted")
	}
	if rec.discardedBytes == 0 {
		t.Fatal("the cut-off tail was not counted in discardedBytes")
	}
}

// TestStoreEmptyTrailingSegmentTolerated: a crash between linking a new segment
// and writing its header leaves a zero-length file. It cannot hold a record, so
// dropping it loses nothing — even a strict open should not be bricked by it.
func TestStoreEmptyTrailingSegmentTolerated(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(7))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(dir, "data.00000002")
	f, err := os.Create(stub)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s2, err := openStore(dir, 4096, 0, true, 0, 0) // strict
	if err != nil {
		t.Fatalf("empty trailing segment failed a strict open: %v", err)
	}
	defer func() { _ = s2.close() }()
	if _, err := os.Stat(stub); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the empty stub should have been removed, stat: %v", err)
	}
	p, _, ok, err := s2.takeHead()
	if err != nil || !ok || recIdx(p) != 7 {
		t.Fatalf("record after the stub was dropped: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if s2.corruptionCount() != 0 {
		t.Fatalf("corruptionCount=%d: an empty segment loses no data", s2.corruptionCount())
	}
}

// TestStoreFailedOpenLeaksNothing: openStore must not leave the directory handle
// (and its lock) behind when load fails.
func TestStoreFailedOpenLeaksNothing(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// Make the segment unreadable — not damaged, which would simply be dropped,
	// but unreadable, which fails the open after the directory handle was taken.
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	path := filepath.Join(dir, "data.00000001")
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path, 0o644) }()

	before := openFDs(t)
	for i := 0; i < 20; i++ {
		if s, err := openStore(dir, 4096, 0, true, 0, 0); err == nil {
			_ = s.close()
			t.Fatal("open of an unreadable segment should fail")
		}
	}
	if after := openFDs(t); after > before {
		t.Fatalf("failed opens leaked descriptors: %d -> %d", before, after)
	}
}

// TestQueuePoisonedAfterSyncFailure walks the whole public surface of a poisoned
// queue: writes and commits refuse, reads still work (the page cache is intact,
// and a reader draining what is there is exactly what you want in that state),
// Err reports it and Close hands it over one last time.
func TestQueuePoisonedAfterSyncFailure(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Err(); err != nil {
		t.Fatalf("healthy queue reports %v", err)
	}
	breakHandle(t, w.st, 0)

	if err := w.Sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("Sync: %v, want ErrIO", err)
	}
	if err := w.Err(); !errors.Is(err, ErrIO) {
		t.Fatalf("Err: %v, want ErrIO", err)
	}

	// Hand the store a perfectly good descriptor again. The latch must hold: a
	// retried fsync would now report success over pages the kernel already
	// dropped, which is exactly the lie the poisoning exists to prevent.
	repairHandle(t, w.st, 0)
	if err := w.Sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("Sync after the descriptor recovered: %v, want the latch to hold", err)
	}
	if err := w.Add(99); !errors.Is(err, ErrIO) {
		t.Fatalf("Add on a poisoned queue: %v, want ErrIO", err)
	}
	// Reads still serve what is there, so a consumer can drain the backlog.
	v, ok, off, err := r.TryReserve()
	if err != nil || !ok || v != 0 {
		t.Fatalf("TryReserve on a poisoned queue: v=%d ok=%v err=%v", v, ok, err)
	}
	if err := r.Commit(off); !errors.Is(err, ErrIO) {
		t.Fatalf("Commit on a poisoned queue: %v, want ErrIO", err)
	}
	if err := w.Close(); !errors.Is(err, ErrIO) {
		t.Fatalf("Close: %v, want the latched ErrIO", err)
	}
}

// TestTakeReportsCommitFailure: Take hands the item over *and* reports that its
// commit did not stick, rather than claiming a consumption that will replay.
func TestTakeReportsCommitFailure(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	if err := w.Add(42); err != nil {
		t.Fatal(err)
	}
	reopenReadOnly(t, w.st, 0)

	v, ok, err := r.TryTake()
	if !ok || v != 42 {
		t.Fatalf("TryTake: v=%d ok=%v, want the item to still be delivered", v, ok)
	}
	if err == nil {
		t.Fatal("TryTake whose commit failed must report the failure")
	}
}

// TestDrainReportsErrorViaErr: an iter.Seq cannot carry an error, so an I/O
// failure used to be indistinguishable from an exhausted queue.
func TestDrainReportsErrorViaErr(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	// A clean drain leaves Err nil.
	n := 0
	for range r.Drain(context.Background()) {
		n++
	}
	if n != 3 || r.Err() != nil {
		t.Fatalf("clean drain: n=%d Err=%v", n, r.Err())
	}

	if err := w.Add(9); err != nil {
		t.Fatal(err)
	}
	breakHandle(t, w.st, 0)
	n = 0
	for range r.Drain(context.Background()) {
		n++
	}
	if n != 0 {
		t.Fatalf("drain over a broken segment yielded %d items", n)
	}
	if r.Err() == nil {
		t.Fatal("Err must report why the iteration stopped")
	}
}

// TestUnmarshalFailureLeavesRecordAtHead: a decode failure consumed the record
// from the read cursor and dropped it on the floor. It has to stay put, like a
// corrupt record does, so nothing is lost by a transient codec error.
func TestUnmarshalFailureLeavesRecordAtHead(t *testing.T) {
	failing := true
	w, err := New[uint64](t.TempDir(), marshalU64, func(b []byte) (uint64, error) {
		if failing {
			return 0, errors.New("decode failed")
		}
		return unmarshalU64(b)
	}, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	if err := w.Add(7); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := r.TryTake(); ok || err == nil {
		t.Fatalf("TryTake with a failing codec: ok=%v err=%v", ok, err)
	}
	if w.Empty() {
		t.Fatal("a record that was never delivered must stay at the head")
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1", got)
	}

	failing = false
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 7 {
		t.Fatalf("retry after the codec recovered: v=%d ok=%v err=%v", v, ok, err)
	}
}

// TestPanicInUnmarshalReleasesLock: Drain used to unlock by hand, so a panic out
// of the user's codec left the queue's mutex held forever — every later call
// would block, which is a worse way to lose a process than a crash.
func TestPanicInUnmarshalReleasesLock(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, func([]byte) (uint64, error) {
		panic("codec blew up")
	}, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected the codec panic to propagate")
			}
		}()
		for range w.NewReader().Drain(context.Background()) {
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Count()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the queue mutex was still held after the panic")
	}
}

// TestUnlinkFailureKeepsSegmentCounted: reclamation that cannot unlink must not
// pretend it did, or maxSegments stops describing what is on disk.
func TestUnlinkFailureKeepsSegmentCounted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	// Two segments' worth, so the first can be fully committed and reclaimed.
	for i := 0; s.active().num < 2; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	for {
		_, off, ok, err := s.takeHead()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := s.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.files) != 1 {
		t.Fatalf("live segments=%d, want the drained ones reclaimed", len(s.files))
	}

	// Now make removal impossible and drain another segment's worth.
	for i := 0; s.active().num < 3; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o755) }()
	live := len(s.files)
	for {
		_, off, ok, err := s.takeHead()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := s.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.files) != live {
		t.Fatalf("live segments=%d, want %d kept: an un-unlinked file is still on disk", len(s.files), live)
	}
	if s.unreclaimed == 0 {
		t.Fatal("the failed unlink was not accounted for")
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0: everything was committed", got)
	}

	// Once removal is possible again the next drop clears the backlog.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, genPayload(300, 1))
	_, off, _, err := s.takeHead()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.commitTo(off); err != nil {
		t.Fatal(err)
	}
	if len(s.files) != 1 {
		t.Fatalf("live segments=%d, want the backlog reclaimed once unlink works again", len(s.files))
	}
}

// TestCycleFailureLeavesStoreUsable: a segment that cannot be created must fail
// the Add and nothing else — no stub file, no lost records, and the queue keeps
// working once the condition clears.
func TestCycleFailureLeavesStoreUsable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	mustAppend(t, s, idxRec(0))
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o755) }()

	// Fill the active segment so the next append has to cycle.
	var failed error
	for i := 1; i < 1000 && failed == nil; i++ {
		failed = s.append(genPayload(300, byte(i)))
	}
	if failed == nil {
		t.Fatal("cycling into an unwritable directory should fail")
	}
	if errors.Is(failed, ErrIO) {
		t.Fatalf("a failed create must not poison the store: %v", failed)
	}
	if n := countSegments(t, dir); n != len(s.files) {
		t.Fatalf("%d files on disk but %d tracked: a failed create left a stub", n, len(s.files))
	}

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(1))
	p, _, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("first record after the failed cycle: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
}

// vanishSegment unlinks segment idx and drops the store's handle to it, so the
// next access has to open a file that is not there — a segment lost to an
// operator, a stray cleanup job, or a filesystem repair.
func vanishSegment(t *testing.T, s *store, idx int) {
	t.Helper()
	df := s.files[idx]
	if df.f != nil {
		_ = df.f.Close()
		df.f = nil
		s.removeMapped(df)
	}
	if err := os.Remove(s.filePath(df.num)); err != nil {
		t.Fatal(err)
	}
}

// TestVanishedSegmentIsCorruption: a segment the header says holds records but
// which is no longer on disk is corruption — those bytes are not coming back. It
// used to surface as a bare ENOENT, which no recovery path recognised, so the
// reader produced the identical failure forever with Corruptions() stuck at 0.
func TestVanishedSegmentIsCorruption(t *testing.T) {
	fill := func(t *testing.T, s *store) {
		t.Helper()
		for i := 0; s.active().num < 3; i++ {
			mustAppend(t, s, genPayload(300, byte(i)))
		}
	}

	s, _ := newTestStore(t, 4096, 0)
	fill(t, s)
	vanishSegment(t, s, 0)

	// One event is reported, the segment is abandoned, and the later segments
	// deliver in order — a missing file must not stop the queue for good.
	got, events := drainRecovering(t, s)
	if events != 1 {
		t.Fatalf("%d corruption events, want 1", events)
	}
	if len(got) == 0 {
		t.Fatal("the surviving segments delivered nothing")
	}
	assertAscending(t, got)
	if s.lostSegments != 1 {
		t.Fatalf("lostSegments=%d, want 1", s.lostSegments)
	}
	if s.lostBytes == 0 {
		t.Fatal("the vanished segment's bytes were not counted")
	}
	if n := s.count(); n != 0 {
		t.Fatalf("count=%d after a full drain, want 0", n)
	}
}

// TestLoadOpenErrorIsNotCorruption: recovery licenses dropping a segment
// that is *damaged*, not one the process merely could not open. A chmod blip used
// to be laundered into a corruption verdict and unlink a healthy segment.
func TestLoadOpenErrorIsNotCorruption(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; s.active().num < 2; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	want := s.count()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	last := filepath.Join(dir, "data.00000002")
	if err := os.Chmod(last, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(last, 0o644) }()

	bad, err := openStore(dir, 4096, 0, true, 0, 0)
	if err == nil {
		_ = bad.close()
		t.Fatal("an unreadable segment must fail the open, not be deleted")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("open: %v, want a permission error", err)
	}
	if _, serr := os.Stat(last); serr != nil {
		t.Fatalf("the segment was destroyed: %v", serr)
	}

	if err := os.Chmod(last, 0o644); err != nil {
		t.Fatal(err)
	}
	good, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("reopen once readable again: %v", err)
	}
	defer func() { _ = good.close() }()
	if got := good.count(); got != want {
		t.Fatalf("count=%d, want %d: records were lost", got, want)
	}
	if good.corruptionCount() != 0 {
		t.Fatalf("corruptionCount=%d: nothing was actually corrupt", good.corruptionCount())
	}
}

// TestReopenAccountingIsExact: the committed counts are per-segment while the
// commit cursor is global, and writeback across segments is not ordered — so a
// header can claim "fully committed" for records the recovered cursor will
// replay. Counting them at load *and* when they are re-consumed drove Count()
// negative.
func TestReopenAccountingIsExact(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; s.active().num < 3; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	total := s.count()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// Segment 1's cursor stays at the start (nothing committed) while segment 2
	// claims to be fully committed — the residue of a lost header writeback.
	_, w2, written2, _ := readFileHeader(t, dir, 2)
	forgeHeader(t, dir, 2, func(h []byte) {
		binary.LittleEndian.PutUint64(h[8:16], uint64(w2))        // commit cursor at the end
		binary.LittleEndian.PutUint64(h[32:40], uint64(written2)) // all records committed
	})

	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != total {
		t.Fatalf("count=%d, want %d: the recovered cursor replays everything", got, total)
	}
	delivered := int64(0)
	for {
		_, off, ok, err := s2.takeHead()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := s2.commitTo(off); err != nil {
			t.Fatal(err)
		}
		delivered++
	}
	if delivered != total {
		t.Fatalf("delivered %d records, Count() promised %d", delivered, total)
	}
	if got := s2.count(); got != 0 {
		t.Fatalf("count=%d after draining everything, want 0", got)
	}
}

// TestCommitBeyondReadCursorRejected: Commit used to accept any offset up to the
// write cursor, so one bad offset reclaimed — and deleted — segments no reader had
// seen, including the one the read cursor was standing in. Every later read then
// addressed a file that was gone.
func TestCommitBeyondReadCursorRejected(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	const n = 400
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Commit(w.Size()); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("Commit past the read cursor: %v, want ErrInvalidOffset", err)
	}
	if got := w.Count(); got != n {
		t.Fatalf("Count=%d, want %d: the rejected commit must change nothing", got, n)
	}
	// And everything still drains, in order.
	got := uint64(0)
	for v := range r.Drain(context.Background()) {
		if v != got {
			t.Fatalf("out of order: got %d, want %d", v, got)
		}
		got++
	}
	if err := r.Err(); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got != n {
		t.Fatalf("drained %d of %d", got, n)
	}
}

// TestSkipPastPoisonRecord: because a decode failure now leaves the record at the
// head, there has to be a deliberate way past one the codec will never accept.
func TestSkipPastPoisonRecord(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, func(b []byte) (uint64, error) {
		v, err := unmarshalU64(b)
		if err == nil && v == 2 {
			return 0, errors.New("poison record")
		}
		return v, err
	}, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	for i := uint64(1); i <= 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	if v, ok, err := r.TryTake(); !ok || err != nil || v != 1 {
		t.Fatalf("first take: v=%d ok=%v err=%v", v, ok, err)
	}
	// The poison record is offered again rather than silently dropped.
	for i := 0; i < 2; i++ {
		if _, ok, err := r.TryTake(); ok || err == nil {
			t.Fatalf("take %d of the poison record: ok=%v err=%v", i, ok, err)
		}
		if got := w.Count(); got != 2 {
			t.Fatalf("Count=%d, want 2: a rejected record must stay in the queue", got)
		}
	}
	ok, err := r.Skip()
	if !ok || err != nil {
		t.Fatalf("Skip: ok=%v err=%v", ok, err)
	}
	if v, ok, err := r.TryTake(); !ok || err != nil || v != 3 {
		t.Fatalf("take after Skip: v=%d ok=%v err=%v", v, ok, err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count=%d, want 0", got)
	}
}

// TestCloseFlushesUnderNoSync: the documented contract is that Close always
// flushes. Close used to skip the flush entirely under NoSync, so it reported
// success for data it had not even tried to make durable.
func TestCloseFlushesUnderNoSync(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	breakHandle(t, w.st, 0)
	if err := w.Close(); !errors.Is(err, ErrIO) {
		t.Fatalf("Close: %v, want the failed flush reported", err)
	}
}

// TestNewRejectsNilCodec: a missing codec used to be a nil call on the first Add.
func TestNewRejectsNilCodec(t *testing.T) {
	if w, err := New[uint64](t.TempDir(), nil, unmarshalU64); err == nil {
		_ = w.Close()
		t.Fatal("New with a nil MarshalFunc should fail")
	}
	if w, err := New[uint64](t.TempDir(), marshalU64, nil); err == nil {
		_ = w.Close()
		t.Fatal("New with a nil UnmarshalFunc should fail")
	}
}

// TestSegmentCapacityRounding pins the geometry to a fixed alignment: rounding by
// the running host's page size would put a store created on a 4 KiB-page machine
// out of reach of a 64 KiB-page one.
func TestSegmentCapacityRounding(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{0, 8 << 20},
		{-1, 8 << 20},
		{1, 4096},
		{4095, 4096},
		{4096, 4096},
		{4097, 8192},
		{5000, 8192},
		{1 << 20, 1 << 20},
	}
	for _, tc := range cases {
		if got := segmentCapacity(tc.in); got != tc.want {
			t.Errorf("segmentCapacity(%d)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

// countSegments counts data.* files in dir.
func countSegments(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if len(e.Name()) > len(filePrefix) && e.Name()[:len(filePrefix)] == filePrefix {
			n++
		}
	}
	return n
}
