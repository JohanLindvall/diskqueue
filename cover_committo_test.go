// UNCOVERED: none. All nine target blocks in commitTo are now executed and
// asserted; `go test -covermode=set` reports a non-zero count for each of
// store.go 833.22 (clamp past the tail), 845.16 (cursor in no live segment),
// 852.54 (persistCommit failure on leaving a segment), 858.42 (ensureOpen
// failure), 865.55 (destination guard), 877.63 (quarantine failure), 890.4
// (non-corrupt frameEnd error), 918.15 (nothing advanced) and 922.53 (batched
// recordOp failure). Package coverage 87.2% -> 89.1%.
//
// Two notes on what these tests are and are not.
//
//  1. The "stop short of a non-boundary target" arm (store.go 893) is named in
//     the brief but was already covered before this file — Reserve/Commit at a
//     mid-record offset reaches it — so nothing here targets it.
//
//  2. Every test was mutation-checked: the corresponding block was broken in
//     store.go and the test confirmed to fail, then reverted. Seven fail on an
//     assertion. Two fail by *hanging*, which is the finding rather than a
//     weakness of the test: removing the `next <= s.commitOff` half of the
//     destination guard, or swallowing skipCorruptSegment's error so the
//     quarantine never sticks, both leave commitTo spinning on the same offset
//     forever — under the queue mutex. The block at 918 needed care: deleting
//     the early return outright leaves `advanced` write-only and does not
//     compile, so it was neutered instead (`if !advanced && stop == nil`), and
//     TestCommitToReadFailureIsNotCorruption then fails on the batch-counter
//     assertion.

package diskqueue

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The error and edge arms of commitTo. Every one of these is a place where the
// commit walk has to decide what a failure *means* — retriable, corrupt, or a
// durability loss — and the decision picks who may delete a segment. They are
// reached here through the store's own internals (this is an in-package,
// white-box file) because none of them can be produced from the public API on a
// healthy filesystem:
//
//   - the clamp of a target past the tail: Reader.Commit rejects any offset
//     beyond the read cursor, so only a direct store call gets there;
//   - "the cursor points at no live segment": commitTo drags the read cursor
//     along precisely so this cannot happen, so the entry has to be taken out of
//     s.files by hand;
//   - the destination guard: frameEnd consults the cached frame boundary
//     (s.lastFrameAt/s.lastFrameEnd) before falling back to recordLen, and
//     recordLen already bounds its answer by the segment — so the guard can only
//     be reached through the cache. Forging the cache is exactly what a bit-flip
//     in that pair of words would do, and the guard is the defence in depth that
//     stops it from moving the commit cursor backwards (an infinite loop under
//     the queue lock) or past the tail.
//
// The fault injection itself reuses robust_test.go's helpers — breakHandle,
// reopenReadOnly, repairHandle — which swap a segment's descriptor behind the
// store's back and rely on ensureOpen not reopening a file whose f is non-nil.

// committoFillSegments appends idxRec records until the store has grown a new
// active segment with num == want, so the commit walk has more than one file to
// cross.
func committoFillSegments(t *testing.T, s *store, want uint64) {
	t.Helper()
	for i := 0; s.active().num < want; i++ {
		if i > 100000 {
			t.Fatal("segment never cycled")
		}
		mustAppend(t, s, idxRec(i))
	}
}

// committoDropHandle closes segment idx's descriptor *and* clears it, so the next
// ensureOpen really has to open the file again. breakHandle leaves f non-nil on
// purpose (it fakes a dead device); this fakes an evicted handle, which is what
// makes the open itself — and its failure — reachable.
func committoDropHandle(t *testing.T, s *store, idx int) {
	t.Helper()
	df := s.files[idx]
	if df.f != nil {
		if err := df.f.Close(); err != nil {
			t.Fatal(err)
		}
		df.f = nil
		s.untrackOpen(df)
	}
}

// committoForgeFrame plants a frame boundary in the store's read-side cache. A
// consume op leaves this pair set for the commit that follows it under the same
// lock; forging it is how a destination that recordLen could never produce
// reaches commitTo's guard.
func committoForgeFrame(s *store, at, end int64) {
	s.lastFrameAt, s.lastFrameEnd = at, end
}

// TestCommitToClampsTargetPastTail: commitTo accepts an offset past the write
// cursor and clamps it, rather than walking off the end of the last segment.
//
// It matters because the clamp is the only thing between a caller's arithmetic
// slip and a corruption verdict: with the clamp removed the walk commits every
// record, arrives at commitOff == writeOff — which is one past the last record
// and therefore inside no file's [base, base+size) — and reports ErrCorrupt for a
// queue that is perfectly intact. So the assertion is not just "no error": it is
// that nothing was booked as damage.
func TestCommitToClampsTargetPastTail(t *testing.T) {
	s, _ := newTestStore(t, 512, 0)
	const n = 5
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	tail := s.writeOffset()

	if err := s.commitTo(tail + 4096); err != nil {
		t.Fatalf("commitTo past the tail: %v, want the target clamped and committed", err)
	}
	if s.commitOff != tail {
		t.Fatalf("commitOff=%d, want the clamped tail %d", s.commitOff, tail)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0: every record was acknowledged", got)
	}
	if st := s.stats(); st.Corruptions != 0 || st.LostSegments != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d: an over-large target is a clamp, not damage",
			st.Corruptions, st.LostSegments)
	}
	if !s.empty() {
		t.Fatal("store not empty after committing everything")
	}
}

// TestCommitToCursorInNoLiveSegment: if the commit cursor ever addresses no live
// file, the walk stops with ErrCorrupt and changes nothing — it does not spin,
// and it does not reclaim.
//
// The state is unreachable by design (commitTo drags the read cursor along so a
// commit can never strand it), so the entry is taken out of s.files by hand. What
// is being pinned is the defensive arm's *shape*: a cursor with no file under it
// cannot be advanced by anything, so the only safe answers are "report" and
// "touch nothing" — the walk must not fall through to dropCommitted, which
// unlinks.
func TestCommitToCursorInNoLiveSegment(t *testing.T) {
	s, dir := newTestStore(t, 512, 0)
	committoFillSegments(t, s, 2)
	total := s.count()
	files := countSegments(t, dir)

	// Hide the segment the commit cursor stands in.
	lost := s.files[0]
	s.files = s.files[1:]

	err := s.commitTo(s.writeOffset())
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("commitTo with the cursor in no live segment: %v, want ErrCorrupt", err)
	}
	if s.commitOff != 0 {
		t.Fatalf("commitOff=%d, want 0: nothing could be advanced", s.commitOff)
	}
	if got := s.count(); got != total {
		t.Fatalf("count=%d, want %d: a refused commit retires no records", got, total)
	}
	if got := countSegments(t, dir); got != files {
		t.Fatalf("%d segments on disk, want %d: a stopped walk must not reclaim", got, files)
	}
	if s.failure() != nil {
		t.Fatalf("store poisoned by a stopped walk: %v", s.failure())
	}

	// Put it back: the store was never damaged, so the commit now works and the
	// queue drains to empty.
	s.files = append([]*dataFile{lost}, s.files...)
	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatalf("commitTo once the segment is live again: %v", err)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0", got)
	}
}

// TestCommitToOpenFailureIsRetriableAndDeletesNothing: a segment the process
// merely could not *open* is a retriable failure — class (2) — and licenses
// nothing. The walk stops, the error comes back as itself, and every byte stays
// where it is; laundering this into ErrCorrupt is what once had a chmod blip
// unlink a healthy segment.
func TestCommitToOpenFailureIsRetriableAndDeletesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	s, dir := newTestStore(t, 512, 0)
	committoFillSegments(t, s, 2)
	total := s.count()
	files := countSegments(t, dir)

	// The commit cursor sits in segment 1; make opening it fail.
	committoDropHandle(t, s, 0)
	path := filepath.Join(dir, "data.00000001")
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path, 0o644) }()

	err := s.commitTo(s.writeOffset())
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("commitTo over an unopenable segment: %v, want the permission error itself", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a failed open must not be reported as corruption: %v", err)
	}
	if errors.Is(err, ErrIO) || s.failure() != nil {
		t.Fatalf("a failed open must not poison the store: err=%v failure=%v", err, s.failure())
	}
	if s.commitOff != 0 {
		t.Fatalf("commitOff=%d, want 0: nothing was acknowledged", s.commitOff)
	}
	if st := s.stats(); st.Corruptions != 0 || st.LostSegments != 0 || st.LostBytes != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d lostBytes=%d: nothing was damaged",
			st.Corruptions, st.LostSegments, st.LostBytes)
	}
	if got := countSegments(t, dir); got != files {
		t.Fatalf("%d segments on disk, want %d: a retriable failure deletes nothing", got, files)
	}
	if got := s.count(); got != total {
		t.Fatalf("count=%d, want %d", got, total)
	}

	// The condition clears and the retry succeeds with no loss recorded, which is
	// the whole point of not treating an open failure as damage.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatalf("commitTo after the permissions came back: %v", err)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0: the retry acknowledged everything", got)
	}
	if s.corruptionCount() != 0 {
		t.Fatalf("corruptions=%d, want 0: nothing was ever corrupt", s.corruptionCount())
	}
}

// TestCommitToPersistFailureOnLeavingSegment: the header of an outgoing segment
// is written when the walk leaves it, and that write can fail. commitTo used to
// return nothing at all, so a commit whose header never reached even the page
// cache looked exactly like one that did.
//
// The failure is a pwrite, not an fsync — class (2) — so it must be returned
// without poisoning the store, and what the walk already committed stands (the
// cursor only ever moved forward over records that were really there).
func TestCommitToPersistFailureOnLeavingSegment(t *testing.T) {
	s, _ := newTestStore(t, 512, 0)
	committoFillSegments(t, s, 2)
	total := s.count()
	firstSeg := s.files[0]
	boundary := s.files[1].base // where the walk crosses out of segment 1

	// Reads keep working, so the walk crosses the whole of segment 1; the header
	// write it does on the way out is what fails.
	reopenReadOnly(t, s, 0)

	err := s.commitTo(s.writeOffset())
	if err == nil {
		t.Fatal("a commit whose header write failed must report it")
	}
	if errors.Is(err, ErrIO) || s.failure() != nil {
		t.Fatalf("a failed pwrite must not poison the store: err=%v failure=%v", err, s.failure())
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a failed pwrite is not corruption: %v", err)
	}
	if s.commitOff != boundary {
		t.Fatalf("commitOff=%d, want %d: the walk stops where the header write failed",
			s.commitOff, boundary)
	}
	if want := total - firstSeg.written; s.count() != want {
		t.Fatalf("count=%d, want %d: what was committed before the failure stands", s.count(), want)
	}
	if st := s.stats(); st.Corruptions != 0 {
		t.Fatalf("corruptions=%d: a write failure is not damage", st.Corruptions)
	}

	// The store is still usable: the rest of the backlog commits normally, now
	// that the walk starts in a segment whose handle is healthy.
	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatalf("second commitTo: %v", err)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0", got)
	}
}

// TestCommitToRejectsBackwardFrameEnd: a frame boundary that does not move
// strictly forward is corruption, not a destination.
//
// This is the arm that keeps a bad number from becoming a hang: with the
// `next <= s.commitOff` half of the guard removed, the loop assigns
// s.commitOff = next, makes no progress, and spins forever holding the queue
// mutex. With it, the record is unframable, the segment is quarantined, and the
// loss is booked — so the assertion is on the corruption accounting (an
// unguarded build never returns at all).
func TestCommitToRejectsBackwardFrameEnd(t *testing.T) {
	s, _ := newTestStore(t, 512, 0)
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Commit the first record honestly, so the cursor is somewhere non-zero.
	_, off, ok, err := s.takeHead()
	if err != nil || !ok {
		t.Fatalf("takeHead: ok=%v err=%v", ok, err)
	}
	if err := s.commitTo(off); err != nil {
		t.Fatal(err)
	}
	head := s.headOff

	// A boundary that points at the cursor itself: forward progress of zero.
	committoForgeFrame(s, s.commitOff, s.commitOff)

	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatalf("commitTo over a non-advancing boundary: %v, want the segment quarantined", err)
	}
	if s.corruptions != 1 {
		t.Fatalf("corruptions=%d, want 1: the guard must book the event", s.corruptions)
	}
	if s.lostSegments != 1 {
		t.Fatalf("lostSegments=%d, want 1", s.lostSegments)
	}
	if want := uint64(s.writeOffset() - head); s.lostBytes != want {
		t.Fatalf("lostBytes=%d, want %d: everything from the read cursor to the segment end",
			s.lostBytes, want)
	}
	if s.commitOff != s.writeOffset() {
		t.Fatalf("commitOff=%d, want the segment end %d", s.commitOff, s.writeOffset())
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0: the abandoned tail is accounted for", got)
	}

	// The commit path has no read of its own to carry the report, so the backlog
	// pays it out: exactly one ErrCorrupt, then the queue reads empty. Nothing is
	// wedged and nothing is delivered as data.
	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead: ok=%v err=%v, want the owed ErrCorrupt", ok, err)
	}
	if p, _, ok, err := s.takeHead(); ok || err != nil {
		t.Fatalf("takeHead: p=%v ok=%v err=%v, want empty", p, ok, err)
	}
}

// TestCommitToRejectsFrameEndPastTail is the other half of the same guard: a
// boundary beyond the write cursor names bytes that were never written.
//
// Without the `next > s.writeOff` clause the walk would simply stop short (the
// non-boundary-target arm) and report success, leaving the commit cursor frozen
// on a record it can never frame — which also stops all reclamation, so the disk
// fills behind it. The guard turns it into one counted, reported loss instead, so
// the assertions are on Corruptions/LostSegments: they stay at zero in an
// unguarded build.
func TestCommitToRejectsFrameEndPastTail(t *testing.T) {
	s, _ := newTestStore(t, 512, 0)
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	tail := s.writeOffset()
	committoForgeFrame(s, s.commitOff, tail+1) // one byte past everything ever written

	if err := s.commitTo(tail); err != nil {
		t.Fatalf("commitTo over an out-of-range boundary: %v", err)
	}
	if s.corruptions != 1 {
		t.Fatalf("corruptions=%d, want 1", s.corruptions)
	}
	if s.lostSegments != 1 || s.lostBytes != uint64(tail) {
		t.Fatalf("lostSegments=%d lostBytes=%d, want 1 and %d", s.lostSegments, s.lostBytes, tail)
	}
	if s.commitOff != tail || s.headOff != tail {
		t.Fatalf("commitOff=%d headOff=%d, want both at %d", s.commitOff, s.headOff, tail)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0", got)
	}
	// Never wedged: one report, then empty.
	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead: ok=%v err=%v, want the owed ErrCorrupt", ok, err)
	}
	if _, _, ok, err := s.takeHead(); ok || err != nil {
		t.Fatalf("takeHead: ok=%v err=%v, want empty", ok, err)
	}
}

// TestCommitToQuarantineFailureLeavesCursorPut: the quarantine the commit path
// applies to an unframable record is only real once the header carrying it is
// durable. When that header write fails the walk stops and *nothing* moves —
// otherwise the store would believe it had quarantined a segment the next open
// replays and re-quarantines forever.
//
// It also must not book the event: a Corruptions counter that climbs for a
// quarantine that did not happen is exactly the kind of number that destroys an
// operator's trust in it.
func TestCommitToQuarantineFailureLeavesCursorPut(t *testing.T) {
	s, _ := newTestStore(t, 512, 0)
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	tail := s.writeOffset()
	df := s.files[0]

	// Reads still work (so the walk gets as far as the guard), writes do not (so
	// the quarantine cannot be recorded).
	reopenReadOnly(t, s, 0)
	committoForgeFrame(s, s.commitOff, tail+1)

	err := s.commitTo(tail)
	if err == nil {
		t.Fatal("a quarantine whose header write failed must be reported")
	}
	if errors.Is(err, ErrIO) || s.failure() != nil {
		t.Fatalf("a failed pwrite must not poison the store: err=%v failure=%v", err, s.failure())
	}
	if s.commitOff != 0 || s.headOff != 0 {
		t.Fatalf("commitOff=%d headOff=%d, want both still 0: the quarantine did not stick",
			s.commitOff, s.headOff)
	}
	if s.corruptions != 0 || s.lostSegments != 0 || s.pendingCorrupt != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d pending=%d: nothing was quarantined",
			s.corruptions, s.lostSegments, s.pendingCorrupt)
	}
	// The header bytes were rolled back too, so a reopen sees the original cursor
	// rather than a quarantine that only half happened.
	if got, want := df.commitCursor(), int64(headerSize); got != want {
		t.Fatalf("in-memory commit cursor=%d, want the rolled-back %d", got, want)
	}
	if got := df.committedCount(); got != 0 {
		t.Fatalf("in-memory committed count=%d, want the rolled-back 0", got)
	}
	if got := s.count(); got != n {
		t.Fatalf("count=%d, want %d: nothing was retired", got, n)
	}

	// Give the segment a writable descriptor again: the retry quarantines for
	// real, so the failure was a delay, not a wedge.
	repairHandle(t, s, 0)
	if err := s.commitTo(tail); err != nil {
		t.Fatalf("retry after the descriptor recovered: %v", err)
	}
	if s.corruptions != 1 || s.commitOff != tail {
		t.Fatalf("corruptions=%d commitOff=%d, want 1 and %d", s.corruptions, s.commitOff, tail)
	}
}

// TestCommitToReadFailureIsNotCorruption: a pread that fails for a reason other
// than "the file ended" is an I/O error, and an I/O error is not damage. The
// bytes may well still be there next time, so the walk stops, nothing is dropped,
// and the cursor stays put.
//
// The distinction is load-bearing: ErrCorrupt is the only class that licenses
// skipCorruptSegment to abandon a whole segment, so misclassifying a transient
// read failure here would destroy intact records.
//
// The store is on the batched policy so the walk that commits nothing can also be
// checked against the batch counter: a commit that retired no record must not
// consume a slot in the batch, or the flush that owes durability to earlier ops
// arrives early and books them as durable a syscall too soon.
func TestCommitToReadFailureIsNotCorruption(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 512, 0, false, 1000, 0) // batched; no flush during the test
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	files := countSegments(t, dir)
	unsynced := s.unsynced

	// No read has happened, so the frame cache is cold and the walk has to pread
	// the length prefix — through a descriptor that is closed underneath it.
	breakHandle(t, s, 0)

	err = s.commitTo(s.writeOffset())
	if err == nil {
		t.Fatal("commitTo over a dead descriptor must report the failure")
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("a device failure must not be classified as corruption: %v", err)
	}
	if errors.Is(err, ErrIO) || s.failure() != nil {
		t.Fatalf("a failed pread must not poison the store: err=%v failure=%v", err, s.failure())
	}
	if s.commitOff != 0 {
		t.Fatalf("commitOff=%d, want 0: nothing could be framed, so nothing was committed", s.commitOff)
	}
	if st := s.stats(); st.Corruptions != 0 || st.LostSegments != 0 || st.LostBytes != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d lostBytes=%d: an I/O error destroys nothing",
			st.Corruptions, st.LostSegments, st.LostBytes)
	}
	if got := countSegments(t, dir); got != files {
		t.Fatalf("%d segments on disk, want %d: nothing may be reclaimed", got, files)
	}
	if got := s.count(); got != n {
		t.Fatalf("count=%d, want %d", got, n)
	}
	if s.unsynced != unsynced {
		t.Fatalf("unsynced=%d, want %d: a walk that committed nothing must not consume a batch slot",
			s.unsynced, unsynced)
	}

	// And the records are all still there once the descriptor is healthy again.
	repairHandle(t, s, 0)
	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatalf("commitTo after the descriptor recovered: %v", err)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count=%d, want 0", got)
	}
	if s.corruptionCount() != 0 {
		t.Fatalf("corruptions=%d, want 0: nothing was ever corrupt", s.corruptionCount())
	}
}

// TestCommitToBatchedFlushFailureIsReported: on the batched sync policy the
// commit's durability is owed to flushBatch, not to the commit itself — so the
// batch flush is where a commit can fail to become durable, and commitTo has to
// hand that back. recordOp used to swallow it, which meant a fsync failure could
// be reported to nobody at all while the store went on claiming health.
//
// The failing fsync is on a segment the walk never touches: that is what makes
// this reachable without also failing the header write first, and it mirrors the
// real shape of the bug — a batch covers every dirty file, not just the one the
// caller was working on.
func TestCommitToBatchedFlushFailureIsReported(t *testing.T) {
	dir := t.TempDir()
	// syncEvery=2: batched(), and one op short of a flush after the setup below.
	s, err := openStore(dir, 512, 0, false, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()

	committoFillSegments(t, s, 2)
	if err := s.sync(); err != nil { // every segment clean, batch counter at zero
		t.Fatal(err)
	}
	segs := len(s.files)
	mustAppend(t, s, idxRec(999)) // dirties the active segment; unsynced == 1
	if len(s.files) != segs {
		t.Fatalf("the extra append cycled (%d -> %d segments); the active segment must be the dirty one",
			segs, len(s.files))
	}
	active := s.active()
	if !active.dirty {
		t.Fatal("the active segment is not dirty, so the batch flush would have nothing to fail on")
	}
	if s.unsynced != s.syncEvery-1 {
		t.Fatalf("unsynced=%d, want %d: the commit below must be the op that triggers the flush",
			s.unsynced, s.syncEvery-1)
	}
	total := s.count()

	// Read one record out of the first segment, then kill the *active* segment's
	// descriptor: the commit walk stays in segment 1, so only the batch flush
	// touches the broken one.
	_, off, ok, err := s.takeHead()
	if err != nil || !ok {
		t.Fatalf("takeHead: ok=%v err=%v", ok, err)
	}
	breakHandle(t, s, len(s.files)-1)

	cerr := s.commitTo(off)
	if !errors.Is(cerr, ErrIO) {
		t.Fatalf("commitTo whose batch flush failed: %v, want ErrIO", cerr)
	}
	if !errors.Is(s.failure(), ErrIO) {
		t.Fatalf("failure()=%v, want the latched ErrIO", s.failure())
	}
	// The commit itself stands — the cursor only moved over a record that was
	// really there — and it is the *durability* of the batch that was lost.
	if s.commitOff != off {
		t.Fatalf("commitOff=%d, want %d: what was committed before the flush failed stands",
			s.commitOff, off)
	}
	if got := s.count(); got != total-1 {
		t.Fatalf("count=%d, want %d", got, total-1)
	}
	if st := s.stats(); st.Corruptions != 0 {
		t.Fatalf("corruptions=%d: a durability failure is not corruption", st.Corruptions)
	}
	// Poisoned for good: a retried fsync would report success over pages the
	// kernel has already dropped.
	repairHandle(t, s, len(s.files)-1)
	if err := s.commitTo(s.writeOffset()); !errors.Is(err, ErrIO) {
		t.Fatalf("commitTo on a poisoned store: %v, want ErrIO", err)
	}
	if err := s.append(idxRec(0)); !errors.Is(err, ErrIO) {
		t.Fatalf("append on a poisoned store: %v, want ErrIO", err)
	}
}
