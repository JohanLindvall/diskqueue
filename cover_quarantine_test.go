package diskqueue

// The quarantine paths: skipCorruptSegment and the force-commit under it.
//
// UNCOVERED: nothing in this file's targets. skipCorruptSegment is at 100% of its
// coverage blocks.
//
// A note on where these arms live, because it moved. The header-publishing switch
// this file exercises — the writeHeader-failure rollback, the flushFile failure,
// the os.ErrNotExist case and the default retriable arm — was hoisted out of
// skipCorruptSegment's "cursor addresses no live segment" branch into
// forceCommitAll (store.go:739), because that branch used to square every counter
// with no header written anywhere and a reopen replayed the lot. The tests here
// still reach it through skipCorruptSegment, which is the caller; the arms
// themselves are additionally driven directly in cover_forcecommit_test.go.
//
// Two structural points that survive the move:
//
//   - The os.ErrNotExist case emits NO coverage counter at all: its body is a
//     comment, and cmd/cover only instruments case clauses containing statements.
//     It cannot be "shown covered", only shown to behave — which
//     TestQuarantineVanishedSegmentSquaresCountsInMemory does.
//   - The default arm (an open that failed for a reason other than ENOENT) is
//     unreachable through the public API, and no production seam may be added to
//     change that. skipCorruptSegment is only ever called from takeHead and
//     commitTo, and both have already opened the segment successfully before they
//     call it (takeHead through read, commitTo through its own ensureOpen) — a
//     failure there returns before the quarantine, because a segment nobody could
//     open is not evidence of damage. Nothing runs between that open and the
//     quarantine's own ensureOpen that could close the handle: eviction only
//     happens when some other file is opened, and the just-opened file is both
//     `keep` and the most-recently-used entry. So the arm is exercised by calling
//     the function directly with the handle evicted the way the LRU evicts one —
//     which is exactly the state the arm exists to survive.

import (
	"errors"
	"os"
	"testing"
)

// Fault injection used here, beyond robust_test.go's breakHandle/reopenReadOnly/
// repairHandle/vanishSegment:
//
//	quarantineDetachSegment — drops a segment from the store's live set without
//	                          touching the counters, so a cursor addresses a hole.
//	quarantineSinkHandle    — swaps in a descriptor that accepts every pwrite and
//	                          fails every fsync, which is the one shape neither
//	                          breakHandle (both fail) nor reopenReadOnly (writes
//	                          fail first) can produce.
//	quarantineEvictHandle   — closes a handle the way evictOpen does, leaving the
//	                          dataFile in the live set with f == nil.

// quarantineDetachSegment removes segment idx from s.files behind the store's
// back, closing its handle, and leaves every counter (nWritten in particular)
// exactly as it was. The result is a store whose read cursor points into a hole:
// fileForOffset finds nothing for it, which is the state skipCorruptSegment's
// first arm exists to survive ("Unreachable now that commitTo drags the read
// cursor along, but if it ever happens"). Nothing in the public API produces it,
// so it is produced here — the same technique as breakHandle, one level up from
// the descriptor.
func quarantineDetachSegment(t *testing.T, s *store, idx int) *dataFile {
	t.Helper()
	df := s.files[idx]
	if df.f != nil {
		_ = df.f.Close()
		df.f = nil
		s.untrackOpen(df)
	}
	// Full slice expression: copy into a fresh array rather than shifting in place,
	// so the store's slice never aliases the detached entry.
	s.files = append(s.files[:idx:idx], s.files[idx+1:]...)
	return df
}

// quarantineSinkHandle swaps segment idx's descriptor for one on os.DevNull:
// pwrite succeeds (so writeHeader publishes its 64 bytes into nothing), fsync
// fails with EINVAL (so flushFile's datasync fails), and pread returns EOF (so a
// record read is a short read, which is corruption). That is precisely the seam
// the flushFile arm needs — a header write that lands and a flush that does not —
// and it is the one failure shape breakHandle and reopenReadOnly cannot make: with
// both of those the write fails first and the flush is never reached.
//
// The properties are asserted rather than assumed, so the test skips instead of
// silently exercising nothing on a platform where /dev/null answers fsync.
func quarantineSinkHandle(t *testing.T, s *store, idx int) {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Skipf("no writable %s: %v", os.DevNull, err)
	}
	if _, err := f.WriteAt([]byte{0}, 0); err != nil {
		_ = f.Close()
		t.Skipf("pwrite on %s fails here (%v): no write-ok/fsync-fails seam", os.DevNull, err)
	}
	if err := datasync(f); err == nil {
		_ = f.Close()
		t.Skipf("fsync on %s succeeds here: no write-ok/fsync-fails seam", os.DevNull)
	}
	df := s.files[idx]
	if df.f != nil {
		_ = df.f.Close()
	}
	df.f = f
}

// quarantineEvictHandle closes segment idx's handle exactly as evictOpen does —
// the dataFile stays live with f == nil and its resident header intact — so the
// next access has to reopen it.
func quarantineEvictHandle(t *testing.T, s *store, idx int) {
	t.Helper()
	df := s.files[idx]
	if df.f == nil {
		return
	}
	if err := df.f.Close(); err != nil {
		t.Fatal(err)
	}
	df.f = nil
	s.untrackOpen(df)
}

// quarantineCounters is the store's view of what has been retired and what has
// been lost — everything the force-commit would move. A quarantine that did not
// complete must leave every one of these alone; that is the whole invariant.
type quarantineCounters struct {
	head, commit       int64
	written, committed int64
	committedTotal     uint64
	corruptions        uint64
	lostSegments       uint64
	lostBytes          uint64
	fileCommitted      int64 // the segment's own retired-record count
}

func quarantineSnapshot(s *store, df *dataFile) quarantineCounters {
	return quarantineCounters{
		head:           s.headOff,
		commit:         s.commitOff,
		written:        s.nWritten,
		committed:      s.nCommitted,
		committedTotal: s.nCommittedTotal,
		corruptions:    s.corruptions,
		lostSegments:   s.lostSegments,
		lostBytes:      s.lostBytes,
		fileCommitted:  df.committed,
	}
}

// TestQuarantineCursorAddressesNoLiveSegment drives skipCorruptSegment's first
// arm: the read cursor names an offset no live segment covers.
//
// Why it matters even though the ordering rules make it unreachable: this arm is
// the store's answer to its own bookkeeping being wrong. If it merely reported
// corruption and left the counters alone, Count() would keep promising a backlog
// that no drain can deliver and Empty() would never come true — a blocking
// consumer would wait forever on records that cannot be addressed. So the arm
// abandons everything to the tail *and* squares the counters, globally and per
// file, and the assertions below check exactly that.
func TestQuarantineCursorAddressesNoLiveSegment(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	// Three segments: the first is the one that will go missing from the live set,
	// the last is the active one (it holds exactly one record, so the appends after
	// the quarantine cannot cycle and the reclamation below stays deterministic).
	for i := 0; s.active().num < 3; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	if len(s.files) != 3 {
		t.Fatalf("live segments=%d, want 3", len(s.files))
	}
	total, writeOff := s.nWritten, s.writeOff
	detached := quarantineDetachSegment(t, s, 0)
	survivors := total - detached.written
	if survivors <= 0 {
		t.Fatalf("survivors=%d: the fixture put every record in one segment", survivors)
	}

	// The read cursor still sits at 0, which is now a hole: read cannot map it to a
	// file and says so with ErrCorrupt, and the quarantine takes it from there.
	_, _, ok, err := s.takeHead()
	if ok {
		t.Fatal("takeHead delivered a record out of a segment the store cannot address")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead: %v, want ErrCorrupt", err)
	}

	// Everything left is abandoned: the cursors go to the tail, and the loss is
	// booked once, in full.
	if s.headOff != writeOff || s.commitOff != writeOff {
		t.Fatalf("cursors after the quarantine: head=%d commit=%d, want both %d",
			s.headOff, s.commitOff, writeOff)
	}
	if s.corruptions != 1 || s.lostSegments != 1 {
		t.Fatalf("corruptions=%d lostSegments=%d, want 1 and 1", s.corruptions, s.lostSegments)
	}
	if s.lostBytes != uint64(writeOff) {
		t.Fatalf("lostBytes=%d, want %d: everything from the cursor to the tail is gone",
			s.lostBytes, writeOff)
	}

	// The point of the arm: Count() and Empty() agree afterwards, so a blocking
	// consumer wakes up instead of waiting on a backlog that is not there.
	if s.nCommitted != s.nWritten {
		t.Fatalf("nCommitted=%d nWritten=%d: the global counters were left out of step",
			s.nCommitted, s.nWritten)
	}
	if !s.empty() || s.count() != 0 {
		t.Fatalf("empty=%v count=%d, want a drained queue", s.empty(), s.count())
	}
	// ...and the per-file counters with it, which is what keeps the *next*
	// reclamation honest (dropCommitted subtracts df.committed from nCommitted).
	for _, df := range s.files {
		if df.committed != df.written {
			t.Fatalf("segment %d: committed=%d written=%d, want them squared",
				df.num, df.committed, df.written)
		}
	}
	if s.nCommittedTotal != uint64(survivors) {
		t.Fatalf("Committed counter=%d, want %d: every abandoned record is retired exactly once",
			s.nCommittedTotal, survivors)
	}
	// One event, and only one: the queue reads clean immediately after.
	if _, _, ok, err := s.takeHead(); ok || err != nil {
		t.Fatalf("second takeHead: ok=%v err=%v, want a clean empty", ok, err)
	}

	// The store must still work, and the counters must survive a reclamation.
	// Appending two records and committing the first drops the (fully committed,
	// non-active) middle segment. If the per-file squaring above had been skipped,
	// dropCommitted would subtract that segment's written records from nWritten and
	// nothing from nCommitted, driving the difference negative — count() clamps at
	// zero, so the raw arithmetic is what is asserted.
	live := len(s.files)
	mustAppend(t, s, idxRec(100))
	mustAppend(t, s, idxRec(101))
	p, off, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 100 {
		t.Fatalf("read after the quarantine: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if err := s.commitTo(off); err != nil {
		t.Fatalf("commit after the quarantine: %v", err)
	}
	if len(s.files) >= live {
		t.Fatalf("live segments=%d, want fewer than %d: the drained segment was not reclaimed",
			len(s.files), live)
	}
	if got := s.nWritten - s.nCommitted; got != 1 {
		t.Fatalf("nWritten-nCommitted=%d, want 1: the reclaim unbalanced the counters", got)
	}
	if got := s.count(); got != 1 {
		t.Fatalf("count=%d, want 1 (the uncommitted record)", got)
	}
}

// TestQuarantineFlushFailureDoesNotApplyForceCommit drives the flushFile arm: the
// header carrying the quarantine reached the page cache and the fsync that would
// make it durable failed.
//
// The invariant is that the in-memory force-commit is applied only *after* the
// header carrying it is durable, so nothing here may move: not the cursors, not
// the counts. And the classification has to be right in both directions — a failed
// fsync is a durability failure (ErrIO, latched), and it is emphatically not an
// ErrCorrupt, because takeHead did not step past anything and a caller told
// "corruption, go round again" would spin on the same bytes forever.
func TestQuarantineFlushFailureDoesNotApplyForceCommit(t *testing.T) {
	dir := t.TempDir()
	// Per-op durability (noSync=false, syncEvery<=1): only then does the quarantine
	// try to make its header durable, which is the arm under test.
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}

	// A descriptor that swallows writes and refuses to sync. The read at the head
	// now comes back short, which is corruption (the bytes the header published are
	// not there), so takeHead reaches for the quarantine — and the quarantine's own
	// header write succeeds while its flush fails.
	quarantineSinkHandle(t, s, 0)
	before := quarantineSnapshot(s, s.files[0])

	_, _, ok, err := s.takeHead()
	if ok {
		t.Fatal("takeHead delivered a record it could not read")
	}
	if !errors.Is(err, ErrIO) {
		t.Fatalf("takeHead: %v, want ErrIO: a failed fsync is a durability failure", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatal("a quarantine that could not be made durable must not report ErrCorrupt: " +
			"nothing was stepped past, and the caller's recovery loop would spin")
	}
	if !errors.Is(s.failure(), ErrIO) {
		t.Fatalf("failure()=%v, want the latched ErrIO", s.failure())
	}

	// Nothing was quarantined: no cursor moved, no record was retired, no loss was
	// booked. A reopen would replay these records, and the store's view says so.
	after := quarantineSnapshot(s, s.files[0])
	if after != before {
		t.Fatalf("the failed quarantine changed the store's view:\n before=%+v\n after =%+v",
			before, after)
	}
	if got := s.count(); got != n {
		t.Fatalf("count=%d, want %d: the records are still a backlog", got, n)
	}
	// Note the asymmetry with the writeHeader arm, which rolls df.hdr back: here the
	// 64 bytes were accepted by the descriptor, so the resident header does carry the
	// forced cursor. What makes that inert is the latch — every path that writes a
	// header out (append, commitTo, sync) refuses once ioErr is set, so no store
	// operation can publish it. That is asserted, because it is what stops the
	// unrolled-back bytes from ever becoming the disk's opinion.
	if err := s.append(idxRec(n)); !errors.Is(err, ErrIO) {
		t.Fatalf("append on the poisoned store: %v, want ErrIO", err)
	}
	if err := s.commitTo(s.writeOff); !errors.Is(err, ErrIO) {
		t.Fatalf("commit on the poisoned store: %v, want ErrIO", err)
	}
	if err := s.sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("sync on the poisoned store: %v, want ErrIO", err)
	}
	// The failure repeats identically rather than degrading into a claim of
	// progress: same error, same cursor.
	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrIO) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("retry: ok=%v err=%v, want the same ErrIO", ok, err)
	}
	if s.headOff != before.head {
		t.Fatalf("headOff=%d, want %d: a retry advanced over damage it never quarantined",
			s.headOff, before.head)
	}

	// Nothing was destroyed either. The bytes were always on disk — it was the
	// descriptor that was lying — so with a healthy one the whole backlog reads
	// back, in order. (Commits stay refused: the store is poisoned, by design.)
	repairHandle(t, s, 0)
	for i := 0; i < n; i++ {
		p, _, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d after the descriptor recovered: idx=%d ok=%v err=%v",
				i, recIdx(p), ok, err)
		}
	}
	if _, _, ok, err := s.takeHead(); ok || err != nil {
		t.Fatalf("after draining: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// TestQuarantineOpenFailureQuarantinesNothing drives the default arm of the
// ensureOpen switch: the segment could not be opened, and not because it is gone.
//
// This is the three-way classification at its sharpest. ENOENT means the bytes are
// never coming back and licenses the quarantine; EACCES means "I could not look",
// which is evidence of nothing. The arm therefore returns the error raw — not
// wrapped in ErrCorrupt, not latched as ErrIO — and must leave every cursor and
// every counter untouched, so the operation is a retry rather than a loss.
//
// See the UNCOVERED note at the top for why this is a direct call: no sequence of
// public operations can reach skipCorruptSegment with the segment's handle closed,
// because both callers open it first. The handle is closed here the way evictOpen
// closes one.
func TestQuarantineOpenFailureQuarantinesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	s, _ := newTestStore(t, 4096, 0)
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	df := s.files[0]
	quarantineEvictHandle(t, s, 0)
	path := s.filePath(df.num)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path, 0o644) }()

	before := quarantineSnapshot(s, df)
	hdrCursor, hdrCount := df.commitCursor(), df.committedCount()
	err := s.skipCorruptSegment(s.headOff)
	if err == nil {
		t.Fatal("quarantining a segment that cannot be opened should fail")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("skipCorruptSegment: %v, want the open error itself", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatal("an open failure is not damage: reporting ErrCorrupt licenses dropping data")
	}
	if errors.Is(err, ErrIO) || s.failure() != nil {
		t.Fatalf("a failed open must not poison the store: err=%v failure=%v", err, s.failure())
	}
	// Nothing moved and nothing was counted — including the corruptions bump, which
	// happens only at the end of a quarantine that actually took place.
	if after := quarantineSnapshot(s, df); after != before {
		t.Fatalf("a quarantine that never happened changed the store's view:\n before=%+v\n after =%+v",
			before, after)
	}
	// This arm returns before the header is touched at all, so the resident 64 bytes
	// still describe the segment as it really is — no forced cursor to leak out on
	// some later, successful header write.
	if df.commitCursor() != hdrCursor || df.committedCount() != hdrCount {
		t.Fatalf("header bytes forced by a quarantine that never ran: commitCursor=%d/%d committedCount=%d/%d",
			df.commitCursor(), hdrCursor, df.committedCount(), hdrCount)
	}
	if got := s.count(); got != n {
		t.Fatalf("count=%d, want %d: nothing was retired", got, n)
	}

	// Once the segment can be opened again the quarantine goes through, is durable,
	// and the abandoned records do not come back on the next open — the other half
	// of "apply the force-commit only once the header carrying it is durable".
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.skipCorruptSegment(s.headOff); err != nil {
		t.Fatalf("quarantine after the permissions were restored: %v", err)
	}
	if s.corruptions != 1 || s.lostSegments != 1 {
		t.Fatalf("corruptions=%d lostSegments=%d, want 1 and 1", s.corruptions, s.lostSegments)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0: the segment was abandoned", got)
	}
	dir := s.dir
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != 0 {
		t.Fatalf("count=%d after reopen, want 0: the quarantine was not durable", got)
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("reopened store: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// TestQuarantineHeaderRollbackIsNotPublishedLater is the reason the writeHeader
// arm rolls the header bytes back, rather than only returning the error.
//
// TestQuarantineFailureIsRetriable already pins the cursors and the error class
// for that arm; what it cannot see is the 64 bytes left in df.hdr. Those bytes are
// not inert: the very next header write — an ordinary append, once the descriptor
// is healthy again — publishes the whole header, forced commit cursor and all. So
// a missing rollback does not just look untidy, it durably retires records that
// were never delivered, and the loss only becomes visible on the next open. This
// asserts the in-memory rollback directly and then proves it end to end, through
// a reopen.
func TestQuarantineHeaderRollbackIsNotPublishedLater(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 4
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	df := s.files[0]
	wantCursor, wantCount := df.commitCursor(), df.committedCount()

	// Unusable framing, and then a descriptor that cannot record the quarantine.
	corruptData(t, s, 0, 0, unframeable)
	reopenReadOnly(t, s, 0)

	if _, _, ok, err := s.takeHead(); ok || err == nil {
		t.Fatalf("takeHead: ok=%v err=%v, want the failed quarantine reported", ok, err)
	}
	if df.commitCursor() != wantCursor || df.committedCount() != wantCount {
		t.Fatalf("header bytes left forced: commitCursor=%d/%d committedCount=%d/%d",
			df.commitCursor(), wantCursor, df.committedCount(), wantCount)
	}

	// The next successful header write must not smuggle the abandoned quarantine
	// onto disk. An append is the ordinary way that happens.
	repairHandle(t, s, 0)
	mustAppend(t, s, idxRec(n))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != n+1 {
		t.Fatalf("count=%d after reopen, want %d: a quarantine that never took effect "+
			"retired records anyway", got, n+1)
	}
	// And the queue is not wedged by the damage that is genuinely there: the
	// segment is quarantined now, once, and the store reads clean afterwards.
	delivered, events := drainRecovering(t, s2)
	if events != 1 {
		t.Fatalf("%d corruption events, want 1", events)
	}
	if len(delivered) != 0 {
		t.Fatalf("delivered %v, want nothing: the damaged framing costs the whole segment",
			delivered)
	}
	if s2.lostSegments != 1 {
		t.Fatalf("lostSegments=%d, want 1", s2.lostSegments)
	}
	if got := s2.count(); got != 0 {
		t.Fatalf("count=%d after the drain, want 0", got)
	}
}

// TestQuarantineVanishedSegmentSquaresCountsInMemory drives the os.ErrNotExist arm
// of the switch: the segment is gone from the directory, so there is no header to
// write the quarantine into and the in-memory advance is all there is to do.
//
// That arm's body is a comment, so it emits no coverage counter — it can only be
// pinned by behaviour. TestVanishedSegmentIsCorruption already shows the event is
// reported and the queue keeps going; what is asserted here is the accounting the
// arm falls through to, which is what keeps a later reclamation exact: nothing is
// latched (no I/O was even attempted), the file's own counts are squared, and the
// entry is retired by the next drop without unbalancing Count().
func TestQuarantineVanishedSegmentSquaresCountsInMemory(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; s.active().num < 3; i++ {
		mustAppend(t, s, genPayload(300, byte(i)))
	}
	total := s.count()
	gone := s.files[0]
	vanishSegment(t, s, 0)

	_, _, ok, err := s.takeHead()
	if ok {
		t.Fatal("takeHead delivered a record from a segment that is not on disk")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead: %v, want ErrCorrupt: those bytes are not coming back", err)
	}
	if s.failure() != nil {
		t.Fatalf("failure()=%v: no write was attempted, so nothing may be latched", s.failure())
	}
	if gone.committed != gone.written {
		t.Fatalf("vanished segment: committed=%d written=%d, want them squared",
			gone.committed, gone.written)
	}
	if s.nCommittedTotal != uint64(gone.written) {
		t.Fatalf("Committed counter=%d, want %d", s.nCommittedTotal, gone.written)
	}
	end := gone.base + gone.size
	if s.commitOff != end || s.headOff != end {
		t.Fatalf("cursors: head=%d commit=%d, want both past the segment at %d",
			s.headOff, s.commitOff, end)
	}
	if got, want := s.count(), total-gone.written; got != want {
		t.Fatalf("count=%d, want %d: the abandoned records must stop counting", got, want)
	}

	// The entry is retired by the next reclamation, and the counters stay exact
	// through it — that is what the squaring above buys.
	p, off, ok, err := s.takeHead()
	if err != nil || !ok {
		t.Fatalf("read after the vanished segment: ok=%v err=%v", ok, err)
	}
	if len(p) != 300 {
		t.Fatalf("payload len=%d, want 300: the wrong record was delivered", len(p))
	}
	if err := s.commitTo(off); err != nil {
		t.Fatalf("commit: %v", err)
	}
	for _, df := range s.files {
		if df == gone {
			t.Fatal("the vanished segment is still in the live set after a reclaiming commit")
		}
	}
	if got, want := s.nWritten-s.nCommitted, total-gone.written-1; got != want {
		t.Fatalf("nWritten-nCommitted=%d, want %d: the reclaim unbalanced the counters", got, want)
	}
}
