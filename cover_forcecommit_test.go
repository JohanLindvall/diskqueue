// UNCOVERED: nothing. forceCommitAll is at 100% of its coverage blocks — the
// already-fully-committed skip, the publishFullCommit failure, the abandoned-count
// bump and the global squaring are all executed and asserted below.
//
// The arm this file exists for is unreachable through the public API: both callers
// of skipCorruptSegment pass a cursor commitTo keeps inside a live segment, so
// `fileForOffset` never returns nil there. It cannot be deleted either — dropping
// the guard nil-derefs on the next line — so it is reached here by driving the
// store directly, which is what makes this file worth its length.

package diskqueue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Coverage for forceCommitAll's failure arms, skipCorruptSegment's propagation of
// them, and Reader.next's hard-error exit.
//
// forceCommitAll exists to square the counters when the cursor addresses no live
// segment, and CLAUDE.md binds it to the same rule as the per-segment quarantine:
// the in-memory advance is applied only once the header carrying it is durable.
// Every test here is about a failure *between* those two things — the case where
// getting it wrong leaves the store believing it retired records the next open
// replays. That state is invisible in process; only a reopen catches it, which is
// why most of these tests reopen.

// fcStore opens a store with a real sync policy. newTestStore passes noSync,
// which would skip the flushFile call two of these tests are aimed at.
func fcStore(t *testing.T, segmentSize int64) (*store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := openStore(dir, segmentSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { _ = s.close() })
	return s, dir
}

// fcFill appends until the store has at least want segments, leaving records
// spread across them.
func fcFill(t *testing.T, s *store, want uint64) {
	t.Helper()
	for i := 0; s.active().num < want; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
}

// TestForceCommitAllSkipsFullyCommittedSegment covers the early `continue`.
//
// A segment every record of which is already retired has nothing to publish and
// nothing to count. Re-publishing it would be harmless but not free — a WriteAt
// and an fsync per segment — but double-counting it into nCommittedTotal would not
// be harmless at all, since that counter feeds Stats().Committed and no reopen
// undoes it.
//
// The segment has to be the ACTIVE one. Any other fully-committed segment is
// reclaimed by the dropCommitted at the end of commitTo and is gone from s.files
// before forceCommitAll could skip it; the active file is the one dropCommitted
// always keeps, because it holds the write position. An earlier version of this
// test retired a non-active segment and passed without the continue ever running —
// re-publishing an already-committed header rewrites the same bytes, so the
// "header unchanged" assertion could not tell the two apart.
func TestForceCommitAllSkipsFullyCommittedSegment(t *testing.T) {
	s, _ := fcStore(t, 4096)
	for i := 0; i < 4; i++ { // stays inside the first segment, so it is the active one
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	if len(s.files) != 1 {
		t.Fatalf("setup: %d segments, want the records in the active one", len(s.files))
	}
	active := s.active()
	if err := s.commitTo(s.writeOff); err != nil {
		t.Fatalf("commitTo: %v", err)
	}
	// Precondition for the continue, asserted so the test cannot silently stop
	// exercising it: fully committed, and its header says so.
	if active.committed != active.written {
		t.Fatalf("setup: committed=%d written=%d", active.committed, active.written)
	}
	if active.commitCursor() < headerSize+active.size {
		t.Fatalf("setup: commitCursor=%d, want >= %d", active.commitCursor(), headerSize+active.size)
	}
	if s.files[0] != active {
		t.Fatal("setup: the fully-committed segment was reclaimed out of the live set")
	}

	totalBefore := s.nCommittedTotal
	if err := s.forceCommitAll(); err != nil {
		t.Fatalf("forceCommitAll: %v", err)
	}

	// The whole point of the skip: nothing was counted a second time.
	if s.nCommittedTotal != totalBefore {
		t.Fatalf("nCommittedTotal %d -> %d: an already-retired segment was counted again",
			totalBefore, s.nCommittedTotal)
	}
	if s.count() != 0 {
		t.Fatalf("count=%d, want 0", s.count())
	}
	if got := s.stats().Committed; got != uint64(active.written) {
		t.Fatalf("Stats().Committed=%d, want %d", got, active.written)
	}
}

// TestForceCommitAllHeaderWriteFailureRollsBack covers the writeHeader arm.
//
// This is the invariant itself. A failed header write must leave *nothing* moved:
// not the header bytes, not the per-file count, not the global counters. If the
// counts moved anyway the store would report a backlog it had already retired
// while the next open replayed every record — and because the rollback restores
// the header bytes, the reopen has to agree with the pre-failure state exactly.
func TestForceCommitAllHeaderWriteFailureRollsBack(t *testing.T) {
	s, dir := fcStore(t, 4096)
	fcFill(t, s, 2)

	first := s.files[0]
	hdrBefore := append([]byte(nil), first.hdr...)
	before := quarantineSnapshot(s, first)
	countBefore := s.count()

	// Read-only descriptor: pread still works, every pwrite fails with EBADF.
	reopenReadOnly(t, s, 0)

	err := s.forceCommitAll()
	if err == nil {
		t.Fatal("forceCommitAll succeeded with an unwritable segment header")
	}
	// A failed pwrite is the RETRIABLE class: the store is consistent, nothing was
	// published, and the caller may try again. It must not be laundered into
	// corruption (which licenses deleting data) or into a durability latch.
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a failed header write reported as corruption: %v", err)
	}
	if s.ioErr != nil {
		t.Fatalf("a failed pwrite latched the store: %v", s.ioErr)
	}

	if string(first.hdr) != string(hdrBefore) {
		t.Fatal("header bytes not rolled back after the failed write")
	}
	if got := quarantineSnapshot(s, first); got != before {
		t.Fatalf("counters moved despite the failure:\n got %+v\nwant %+v", got, before)
	}
	if s.count() != countBefore {
		t.Fatalf("count=%d, want %d", s.count(), countBefore)
	}

	// And the decisive check: the store on disk never learned about a quarantine.
	repairHandle(t, s, 0)
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.close() }()
	if s2.count() != countBefore {
		t.Fatalf("reopen count=%d, want %d: the failed force-commit was partly believed",
			s2.count(), countBefore)
	}
}

// TestForceCommitAllFlushFailureLatches covers the flushFile arm — the one shape
// neither breakHandle nor reopenReadOnly can produce, because with those the
// header write fails first and the flush is never reached.
//
// A failed fsync is the DURABILITY class, and it is unrecoverable in place: the
// kernel reports a writeback error once and then drops the dirty pages, so a
// retry can report success over data that is already gone. It must latch as ErrIO
// and stay latched.
func TestForceCommitAllFlushFailureLatches(t *testing.T) {
	s, _ := fcStore(t, 4096)
	fcFill(t, s, 2)

	// Write lands, fsync fails. Skips on a platform where /dev/null answers fsync.
	quarantineSinkHandle(t, s, 0)

	err := s.forceCommitAll()
	if err == nil {
		t.Fatal("forceCommitAll succeeded with an unsyncable segment")
	}
	if !errors.Is(err, ErrIO) {
		t.Fatalf("forceCommitAll: %v, want ErrIO — a failed fsync is a durability failure", err)
	}
	if s.ioErr == nil {
		t.Fatal("the failure was reported but not latched; a retry could claim durability it lost")
	}
	// Latched forever, even though the condition is gone: that is the whole point.
	repairHandle(t, s, 0)
	if err := s.append(genPayload(8, 1)); !errors.Is(err, ErrIO) {
		t.Fatalf("append after the latch: %v, want ErrIO", err)
	}
	if err := s.sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("sync after the latch: %v, want ErrIO", err)
	}
	// Reads deliberately keep working, so a poisoned queue can still be drained.
	if _, _, _, err := s.takeHead(); errors.Is(err, ErrIO) {
		t.Fatal("the latch blocked reads; a poisoned queue must still be drainable")
	}
}

// TestForceCommitAllVanishedSegment covers the os.ErrNotExist arm.
//
// A segment that is gone from the directory has no header left to record the
// quarantine in — and nothing left to replay out of it either, which is what
// makes applying the squaring in memory alone correct *here* and wrong
// everywhere else. It must not be reported as an error: the file is already as
// retired as it can get.
func TestForceCommitAllVanishedSegment(t *testing.T) {
	s, dir := fcStore(t, 4096)
	fcFill(t, s, 3)

	// Segment 0 disappears from under the store: handle evicted, file unlinked.
	gone := s.files[0]
	quarantineEvictHandle(t, s, 0)
	if err := os.Remove(filepath.Join(dir, filepath.Base(gone.path))); err != nil {
		t.Fatalf("unlink: %v", err)
	}

	if err := s.forceCommitAll(); err != nil {
		t.Fatalf("forceCommitAll: %v, want nil — a vanished segment is not a failure", err)
	}
	if gone.committed != gone.written {
		t.Fatalf("vanished segment: committed=%d written=%d, want them squared",
			gone.committed, gone.written)
	}
	if s.count() != 0 {
		t.Fatalf("count=%d, want 0", s.count())
	}
	// The surviving segments were published for real, so a reopen agrees.
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.close() }()
	if s2.count() != 0 {
		t.Fatalf("reopen count=%d, want 0: the squaring of the live segments did not persist", s2.count())
	}
}

// TestForceCommitAllRetriableOpenFailure covers the default arm.
//
// An open that fails for a reason that is not "the file is gone" — EACCES here —
// is retriable: the segment is presumed intact, nothing may be deleted, nothing
// may be counted as lost, and the error is returned as itself. CLAUDE.md records
// that laundering this class into corruption is how an audit once found a chmod
// blip unlinking a healthy segment.
func TestForceCommitAllRetriableOpenFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: mode 000 does not deny access")
	}
	s, dir := fcStore(t, 4096)
	fcFill(t, s, 2)

	first := s.files[0]
	before := quarantineSnapshot(s, first)
	path := filepath.Join(dir, filepath.Base(first.path))

	quarantineEvictHandle(t, s, 0)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

	err := s.forceCommitAll()
	if err == nil {
		t.Fatal("forceCommitAll succeeded with an unopenable segment")
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a permission failure reported as corruption: %v", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a permission failure reported as a missing file: %v", err)
	}
	if s.corruptions != before.corruptions || s.lostSegments != before.lostSegments {
		t.Fatal("a retriable open failure was booked as data loss")
	}
	if got := quarantineSnapshot(s, first); got != before {
		t.Fatalf("counters moved despite the failure:\n got %+v\nwant %+v", got, before)
	}
	// Nothing deleted: the segment must still be on disk.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the segment was removed on a retriable failure: %v", err)
	}
}

// TestSkipCorruptSegmentPropagatesForceCommitFailure covers the `return err` in
// skipCorruptSegment's no-live-segment arm.
//
// The quarantine must not book its loss counters when the force-commit under it
// failed. Booking them would tell an operator that a segment was abandoned and
// its records retired, while the next open replays every one of them — the
// reverse of the "every loss path is observable" rule, since the observation
// would be of something that did not happen.
func TestSkipCorruptSegmentPropagatesForceCommitFailure(t *testing.T) {
	s, _ := fcStore(t, 4096)
	fcFill(t, s, 3)

	before := quarantineSnapshot(s, s.files[0])
	countBefore := s.count()

	// Detach segment 0 so the read cursor at 0 lands in no live segment, then make
	// the force-commit of the survivors fail.
	quarantineDetachSegment(t, s, 0)
	reopenReadOnly(t, s, 0)

	err := s.skipCorruptSegment(s.headOff)
	if err == nil {
		t.Fatal("skipCorruptSegment succeeded though its force-commit could not be published")
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a failed header write surfaced as corruption: %v", err)
	}
	// It returns before the loss bookkeeping, so none of it may have happened.
	if s.corruptions != before.corruptions {
		t.Fatalf("corruptions=%d, want %d: an event was booked for a quarantine that failed",
			s.corruptions, before.corruptions)
	}
	if s.lostSegments != before.lostSegments || s.lostBytes != before.lostBytes {
		t.Fatalf("loss counters moved: segments=%d bytes=%d, want %d and %d",
			s.lostSegments, s.lostBytes, before.lostSegments, before.lostBytes)
	}
	if s.count() != countBefore {
		t.Fatalf("count=%d, want %d: the backlog changed under a failed quarantine",
			s.count(), countBefore)
	}
	repairHandle(t, s, 0)
}

// TestDrainStopsOnHardReadError covers Reader.next's error exit — the arm taken
// when a read fails for a reason the store did NOT step past.
//
// The neighbouring arm continues the iteration for corruption that was dropped,
// which is right: one bad record must not silently truncate a drain. But that is
// gated on the cursor having actually moved, because damage the store cannot step
// past reports the same error from the same offset forever. A codec error is the
// clearest case — read() puts the record back deliberately — so it must stop.
func TestDrainStopsOnHardReadError(t *testing.T) {
	dir := t.TempDir()
	boom := errors.New("codec says no")
	q, err := New[[]byte](dir,
		func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil },
		func(data []byte) ([]byte, error) { return nil, boom },
		Options{NoSync: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = q.Close() }()

	for i := 0; i < 3; i++ {
		if err := q.Add([]byte{byte(i)}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	r := q.NewReader()
	n := 0
	for range r.Drain(context.Background()) {
		n++
	}
	if n != 0 {
		t.Fatalf("Drain yielded %d items through a codec that rejects everything", n)
	}
	if r.Err() == nil {
		t.Fatal("Drain stopped silently; a codec failure is indistinguishable from an empty queue")
	}
	if !errors.Is(r.Err(), ErrCodec) {
		t.Fatalf("Err = %v, want ErrCodec", r.Err())
	}
	if !errors.Is(r.Err(), boom) {
		t.Fatalf("Err = %v, want the codec's own error reachable through Unwrap", r.Err())
	}
	// A codec error is not data loss, so nothing may be counted as corrupt...
	if st := q.Stats(); st.Corruptions != 0 || st.LostRecords != 0 {
		t.Fatalf("a codec error was booked as corruption: %+v", st)
	}
	// ...and the records must all still be there, at the head, undelivered.
	if got := q.Count(); got != 3 {
		t.Fatalf("Count=%d, want 3: a codec error consumed records", got)
	}
}
