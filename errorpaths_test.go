package diskqueue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The error arms of the store, reached through the descriptor-swap injectors in
// robust_test.go rather than the build-tagged fault seam — everything here is
// reachable in the default build, so it runs in the ordinary suite.
//
// These are the paths that only execute when something has already gone wrong,
// which is exactly when their behaviour matters and exactly when nobody is
// watching. Coverage on them was under 70%.

// TestAppendRollsBackOnHeaderWriteFailure reaches rollbackAppend without the
// fault seam: a read-only descriptor lets writeRecord's pwrite fail... except it
// does not, so the record write fails first. Swap the handle only after the
// record lands by using the batched policy, where the header write is the first
// thing to touch the file after writeRecord.
//
// The simpler route: append into a store whose handle goes read-only, and assert
// the store is left exactly as it was. That covers the rollback's contract —
// nothing advanced — whichever write failed first.
func TestAppendLeavesNothingBehindOnWriteFailure(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; i < 3; i++ {
		mustAppend(t, s, idxRec(i))
	}
	count, size, woff, added := s.count(), s.size(), s.writeOffset(), s.nAdded
	hdrBefore := append([]byte(nil), s.files[0].hdr...)

	reopenReadOnly(t, s, 0)
	if err := s.append(idxRec(99)); err == nil {
		t.Fatal("append into a read-only segment should fail")
	}
	if s.count() != count || s.size() != size || s.writeOffset() != woff || s.nAdded != added {
		t.Fatalf("cursors moved: count=%d/%d size=%d/%d off=%d/%d added=%d/%d",
			s.count(), count, s.size(), size, s.writeOffset(), woff, s.nAdded, added)
	}
	// The in-memory header must be back to what it published, or the next
	// successful append writes a cursor that skips over live bytes.
	if string(s.files[0].hdr) != string(hdrBefore) {
		t.Fatal("the in-memory header was left advanced past the write cursor")
	}
	// The records already there are unaffected.
	for i := 0; i < 3; i++ {
		p, _, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d after the failed append: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
	}
}

// TestShortReadIsCorruptClassification: a read that runs past the real end of a
// segment means the bytes the header published are gone — corruption, which the
// recovery path knows how to quarantine. A genuine device error is not, and must
// be left alone so it is never mistaken for damage worth dropping data over.
func TestShortReadIsCorruptClassification(t *testing.T) {
	// Truncating a segment under a live store makes ReadAt return io.EOF.
	s, dir := newTestStore(t, 4096, 0)
	for i := 0; i < 5; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := os.Truncate(filepath.Join(dir, "data.00000001"), headerSize+idxRecLen); err != nil {
		t.Fatal(err)
	}
	// The first record is whole; the rest of the data region is gone.
	p, _, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("surviving record: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read past the cut: ok=%v err=%v, want ErrCorrupt", ok, err)
	}
	if s.lostSegments == 0 {
		t.Fatal("the cut segment was not counted as lost")
	}
}

// TestCommitFailurePreservesProgress: a commit that cannot persist its header
// still keeps whatever it already advanced — the cursor only ever moves over
// records that were really there, and an unpersisted commit simply replays,
// which is the at-least-once contract. What it must not do is claim success.
func TestCommitFailurePreservesProgress(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	const n = 5
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	var last int64
	for i := 0; i < n; i++ {
		_, off, ok, err := s.takeHead()
		if !ok || err != nil {
			t.Fatalf("read %d: ok=%v err=%v", i, ok, err)
		}
		last = off
	}
	reopenReadOnly(t, s, 0)

	before := s.commitOff
	err := s.commitTo(last)
	if err == nil {
		t.Fatal("a commit whose header write fails must report it")
	}
	if errors.Is(err, ErrIO) {
		t.Fatalf("a failed header WRITE must not poison the store: %v", err)
	}
	if s.commitOff < before {
		t.Fatalf("commitOff went backwards: %d -> %d", before, s.commitOff)
	}
	if s.commitOff > s.writeOff {
		t.Fatalf("commitOff=%d past writeOff=%d", s.commitOff, s.writeOff)
	}
	// The invariant that keeps reclamation safe holds even here.
	if s.headOff < s.commitOff {
		t.Fatalf("the read cursor fell behind the commit cursor: head=%d commit=%d",
			s.headOff, s.commitOff)
	}
}

// TestQuarantineFailureIsRetriable: skipCorruptSegment persists the force-commit
// before believing it. When that write fails, nothing may be left believing the
// segment was quarantined — the next open would replay and re-quarantine it
// forever — and the error must be the retriable one, not ErrCorrupt, or the
// caller's recovery loop spins.
func TestQuarantineFailureIsRetriable(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; i < 4; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Unusable framing, and a handle that cannot record the quarantine.
	corruptData(t, s, 0, 0, unframeable)
	reopenReadOnly(t, s, 0)

	head, commit := s.headOff, s.commitOff
	_, _, ok, err := s.takeHead()
	if ok || err == nil {
		t.Fatalf("takeHead: ok=%v err=%v, want a failure", ok, err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatal("a quarantine that could not be recorded must not report ErrCorrupt: " +
			"the caller would retry forever on a queue that never advanced")
	}
	if s.headOff != head || s.commitOff != commit {
		t.Fatalf("cursors moved on a quarantine that did not persist: head %d->%d commit %d->%d",
			head, s.headOff, commit, s.commitOff)
	}

	// Once the write path works again, the quarantine goes through.
	repairHandle(t, s, 0)
	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("retry after repair: ok=%v err=%v, want ErrCorrupt", ok, err)
	}
	if s.headOff == head {
		t.Fatal("the retry did not advance")
	}
}

// TestCreateFileLeavesNoResidue: every failure path in createFile unlinks the
// file it just made, so a failed segment create can never brick a later open.
func TestCreateFileLeavesNoResidue(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	s, dir := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(0))
	before := countDataFiles(t, dir)

	// A read-only directory fails the create at the open, before any file exists;
	// the assertion that matters is that nothing is left behind either way.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o755) }()

	if _, err := s.createFile(s.nextNum, s.writeOff, s.segmentSize); err == nil {
		t.Fatal("createFile into an unwritable directory should fail")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := countDataFiles(t, dir); got != before {
		t.Fatalf("%d segments on disk, want %d: a failed create left residue", got, before)
	}
	// And the store is still usable.
	mustAppend(t, s, idxRec(1))
	for i := 0; i < 2; i++ {
		p, _, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
	}
}

// TestTruncatedMiddleSegmentIsRepairedOnce: the clamped write cursor has to be
// republished for EVERY truncated segment, not just the active one. A middle
// segment is never re-extended and never appended to, so nothing else would ever
// correct it — every reopen re-detected the same short file and booked the same
// loss again, forever, owing the consumer another spurious ErrCorrupt each time.
//
// Every other truncation test in the suite cuts data.00000001 while it is the
// only segment, which is exactly why this went unnoticed.
func TestTruncatedMiddleSegmentIsRepairedOnce(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Two segments, so the damaged one is not the active one.
	for i := 0; s.active().num < 2; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "data.00000001"), headerSize+600); err != nil {
		t.Fatal(err)
	}

	var corruptions []uint64
	var discarded []uint64
	for open := 0; open < 3; open++ {
		s, err := openStore(dir, 4096, 0, true, 0, 0)
		if err != nil {
			t.Fatalf("open %d: %v", open, err)
		}
		corruptions = append(corruptions, s.corruptionCount())
		discarded = append(discarded, s.discardedBytes)
		if err := s.close(); err != nil {
			t.Fatal(err)
		}
	}
	if corruptions[0] == 0 {
		t.Fatal("the truncation was not detected on the first open")
	}
	if corruptions[1] != 0 || corruptions[2] != 0 {
		t.Fatalf("corruptions per open = %v: the same loss is re-reported forever", corruptions)
	}
	if discarded[1] != 0 || discarded[2] != 0 {
		t.Fatalf("discardedBytes per open = %v: the same bytes are re-counted", discarded)
	}
}

// TestCodecErrorDoesNotImpersonateCorruption: a UnmarshalFunc may return
// anything, including something that wraps ErrCorrupt. Reporting that to the
// caller unchanged said "data on disk was damaged and dropped" about a record
// that is intact, still queued, and counted nowhere in Stats.
func TestCodecErrorDoesNotImpersonateCorruption(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, func([]byte) (uint64, error) {
		return 0, fmt.Errorf("my decoder gave up: %w", ErrCorrupt)
	}, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}

	_, ok, err := r.TryTake()
	if ok || err == nil {
		t.Fatalf("TryTake: ok=%v err=%v, want the codec error", ok, err)
	}
	if !errors.Is(err, ErrCodec) {
		t.Fatalf("err=%v, want it to wrap ErrCodec so callers can tell the two apart", err)
	}
	// The caller's own error stays reachable.
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal("the codec's error was not preserved in the chain")
	}
	// And nothing was actually lost — which is the claim ErrCorrupt alone made.
	st := w.Stats()
	if st.Corruptions != 0 || st.LostRecords != 0 || st.LostBytes != 0 {
		t.Fatalf("a codec error was booked as data loss: %+v", st)
	}
	if w.Count() != 1 {
		t.Fatalf("Count=%d, want 1: the record is intact and still queued", w.Count())
	}
}

// TestLargeRecordRoundTrip: a record bigger than the read-ahead block takes the
// second pread in recordAt — the "the record ran past the block" arm added with
// that optimisation, which nothing exercised. Every other test uses payloads far
// under 4 KiB, so for the batched-log-line case this library is built for, the
// read path had a branch the suite had never run.
func TestLargeRecordRoundTrip(t *testing.T) {
	s, _ := newTestStore(t, 1<<20, 0) // 1 MiB segments
	sizes := []int{
		readAhead - 32, // just inside the block
		readAhead,      // exactly the block
		readAhead + 1,  // one byte over: the second pread
		64 << 10,       // comfortably over
		200 << 10,
	}
	for i, n := range sizes {
		mustAppend(t, s, genPayload(n, byte(i)))
	}
	// Interleave a small one, so the buffer shrinks back across a size change.
	mustAppend(t, s, genPayload(8, 0xAB))

	for i, n := range sizes {
		p, off, ok, err := s.takeHead()
		if err != nil || !ok {
			t.Fatalf("record %d (%d bytes): ok=%v err=%v", i, n, ok, err)
		}
		checkPayload(t, p, n, byte(i))
		if err := s.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
	p, off, ok, err := s.takeHead()
	if err != nil || !ok {
		t.Fatalf("the small record after the large ones: ok=%v err=%v", ok, err)
	}
	checkPayload(t, p, 8, 0xAB)
	if err := s.commitTo(off); err != nil {
		t.Fatal(err)
	}
	if !s.empty() {
		t.Fatal("should be drained")
	}
}

// TestLargeRecordSurvivesReopen: the same records, read back by a fresh store,
// so the second pread runs against a cold buffer too.
func TestLargeRecordSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 1<<20, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 64 << 10
	for i := 0; i < 3; i++ {
		mustAppend(t, s, genPayload(n, byte(i)))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 1<<20, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	for i := 0; i < 3; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok {
			t.Fatalf("record %d after reopen: ok=%v err=%v", i, ok, err)
		}
		checkPayload(t, p, n, byte(i))
		if err := s2.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCommitRoundsDownToARecordBoundary: an offset that is not a frame boundary
// used to round UP, retiring one more record than the caller acknowledged — the
// silent single-record loss this library refuses to have. The bias everywhere
// else is to redeliver, so it stops short instead.
func TestCommitRoundsDownToARecordBoundary(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	const n = 4
	offs := make([]int64, n)
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	for i := 0; i < n; i++ {
		_, off, ok, err := s.takeHead()
		if !ok || err != nil {
			t.Fatalf("read %d: ok=%v err=%v", i, ok, err)
		}
		offs[i] = off
	}
	// One byte past the second record's end: not a boundary.
	if err := s.commitTo(offs[1] + 1); err != nil {
		t.Fatal(err)
	}
	if s.commitOff != offs[1] {
		t.Fatalf("commitOff=%d, want %d: a non-boundary offset must round DOWN, "+
			"never retire a record nobody acknowledged", s.commitOff, offs[1])
	}
	if got := s.count(); got != n-2 {
		t.Fatalf("count=%d, want %d", got, n-2)
	}
}

// TestInFlightBytesTracksUnacknowledgedWork: the gauge behind Rewind. Without it
// the state Rewind exists to undo can only be discovered by performing the
// recovery — you cannot tell whether it was needed until after you have done it.
func TestInFlightBytesTracksUnacknowledgedWork(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	if st := w.Stats(); st.InFlightBytes != 0 {
		t.Fatalf("fresh queue: InFlightBytes=%d", st.InFlightBytes)
	}
	const n = 5
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if st := w.Stats(); st.InFlightBytes != 0 {
		t.Fatalf("nothing read yet: InFlightBytes=%d, want 0", st.InFlightBytes)
	}

	// Reserve everything: now it is all in flight, and Empty is true while the
	// backlog is not — the documented oddity this gauge explains.
	var lastOff int64
	for i := uint64(0); i < n; i++ {
		_, ok, off, err := r.TryReserve()
		if !ok || err != nil {
			t.Fatalf("reserve %d: ok=%v err=%v", i, ok, err)
		}
		lastOff = off
	}
	st := w.Stats()
	if st.InFlightBytes != st.BacklogBytes {
		t.Fatalf("InFlightBytes=%d BacklogBytes=%d: everything read is unacknowledged",
			st.InFlightBytes, st.BacklogBytes)
	}
	if !w.Empty() || w.Count() == 0 {
		t.Fatal("expected the Empty-but-not-drained state this gauge describes")
	}

	if err := r.Commit(lastOff); err != nil {
		t.Fatal(err)
	}
	if st := w.Stats(); st.InFlightBytes != 0 {
		t.Fatalf("InFlightBytes=%d after acknowledging everything, want 0", st.InFlightBytes)
	}
}
