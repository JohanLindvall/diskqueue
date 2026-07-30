package diskqueue

// UNCOVERED: three of the seven targeted blocks cannot be reached from a test
// without changing production code. The reasoning, so nobody re-litigates it:
//
//  1. store.go, append's `if err := s.writeHeader(af); err != nil` arm (the
//     rollback arm; profile block store.go:458.42,463.3 in this tree). To reach
//     it the record's WriteAt at
//     data offset headerSize+af.size must SUCCEED and the header's WriteAt of 64
//     bytes at offset 0 must FAIL — on the *same* descriptor, inside one call,
//     under the queue lock, with nothing in between but memory arithmetic
//     (af.header(...)). Nothing can change the descriptor between the two writes:
//     evictOpen never touches the active file, and every store op holds the lock,
//     so no helper can be interleaved. That leaves properties of the descriptor
//     itself, and every one of them is monotone in the wrong direction:
//     descriptor/permission failures (a closed fd, a read-only fd, EBADF, EACCES,
//     EROFS) fail *both* writes, so the append dies at writeRecord — which is
//     what TestStoreWriteFailureIsRetriable already observes — while every
//     size- or space-based failure (ENOSPC, EFBIG/RLIMIT_FSIZE, a quota) hits the
//     HIGHER offset first, and the header lives at offset 0, the lowest offset in
//     the file. So "the record lands but the header does not" has no real-I/O
//     realization. The faults build reaches the semantically identical arm one
//     line above it via faultPoint("append.writeHeader"), and
//     TestHeaderWriteFailureRollsBack asserts the rollback there; that test is
//     the only coverage this arm can have.
//
//  2. store.go, append's per-op HEADER fdatasync failure (profile block
//     store.go:475.40,479.4). Reaching it needs the DATA fdatasync above it to
//     succeed and the header one to fail — same descriptor, same call, and both
//     are gated on the single `perOp` bool, so the second is never executed
//     unless the first already returned nil. A descriptor whose fdatasync fails
//     (see TestAppendDataSyncFailurePublishesNothing below, which uses one) fails
//     the FIRST one and returns, and a descriptor whose fdatasync succeeds
//     succeeds twice. Nothing legitimate runs between them to change that.
//     Producing "the first fsync works, the second does not" needs either the
//     faults seam (faultPoint("append.syncHeader"), covered by
//     TestHeaderSyncFailurePublishesTheRecord) or a privileged block-device
//     fault injector (dm-error/dm-flakey), which is root-only and not a unit
//     test. NOT reported as covered: the faults-build twin is what runs there.
//
//  3. reader.go, Reader.next's `if !ok` arm (profile block
//     reader.go:265.10,267.4). It is
//     defensive and currently unreachable. read() returns ok=false with a nil
//     error only when store.read finds off >= s.writeOff. Both guards above it
//     already exclude that: the follow branch waits while empty() (headOff >=
//     writeOff && no corruption backlog), and with a backlog owed takeHead
//     returns ErrCorrupt instead of !ok, so the follow path takes the error arm.
//     The bounded branch stops at drained(end); to slip through with headOff >=
//     writeOff it would need writeOff < end, and end is a snapshot of writeOff
//     taken earlier under the lock. writeOff is assigned in exactly three places
//     (load, append's `+= recLen`, rollbackAppend's `-= recLen`) and the rollback
//     only ever undoes an append that happened *after* the snapshot, so writeOff
//     >= end always holds. Reaching this block would require a production change
//     that lets the tail move backwards past a live iterator's snapshot.
//
// Covered here (profile blocks store.go:420.41,422.3 and store.go:439.40,441.4;
// reader.go:223.15,225.4 and reader.go:272.44,274.4): append's ensureOpen failure
// for the active file and its per-op DATA fdatasync failure (a real fdatasync,
// not an injected one), and Reader.next's mid-iteration w.closed arm and its
// failed-commit arm.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// appendDetachHandle closes segment idx's descriptor and clears it, without
// removing the file — the state vanishSegment produces minus the unlink, so the
// next access has to go through ensureOpen and really open the path again. Like
// the other helpers here it works behind the store's back and relies on
// ensureOpen NOT reopening a file whose f is non-nil.
func appendDetachHandle(t *testing.T, s *store, idx int) {
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

// appendNullHandle returns a descriptor on the null device together with the
// error its fdatasync produces — the one descriptor state that makes a pwrite
// succeed and the fsync after it fail, which is exactly the shape append's
// durability arms are written for. On Linux the null device has no fsync file
// operation, so fdatasync(2) answers EINVAL while pwrite(2) reports the full
// count; where that is not true the test is skipped rather than faked.
func appendNullHandle(t *testing.T) (*os.File, error) {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Skipf("cannot open %s: %v", os.DevNull, err)
	}
	if _, err := f.WriteAt([]byte("probe"), headerSize); err != nil {
		_ = f.Close()
		t.Skipf("pwrite to %s fails here (%v): no write-succeeds/fsync-fails descriptor on this platform", os.DevNull, err)
	}
	serr := datasync(f)
	if serr == nil {
		_ = f.Close()
		t.Skipf("fdatasync on %s succeeds here: no write-succeeds/fsync-fails descriptor on this platform", os.DevNull)
	}
	return f, serr
}

// appendInstallHandle swaps in f as segment idx's descriptor. The file stays
// "open" as far as the store's LRU is concerned, which is the point: nothing
// reopens it, so the failure is reproducible.
func appendInstallHandle(t *testing.T, s *store, idx int, f *os.File) {
	t.Helper()
	df := s.files[idx]
	if df.f != nil {
		_ = df.f.Close()
	}
	df.f = f
}

// appendRootErr peels err down to its innermost cause, so an errno can be
// compared across platforms whose datasync wraps it in a *os.PathError (a fresh
// pointer every call, which errors.Is could never match).
func appendRootErr(err error) error {
	for {
		u := errors.Unwrap(err)
		if u == nil {
			return err
		}
		err = u
	}
}

// TestAppendActiveSegmentOpenFailureIsRetriable covers append's ensureOpen arm:
// the active segment's handle is gone and the file cannot be opened again.
//
// The arm exists because the classification matters. An open failure is class
// (2) in CLAUDE.md's three-way split — retriable, store untouched, nothing
// deleted — so it must come back as itself, must NOT latch ErrIO (which would
// make a chmod blip permanent), and must NOT be laundered into ErrCorrupt (which
// is the only class licensed to drop data). And because it happens before the
// record is written, the invariant "a failure before the advance leaves the
// store untouched" applies in its strongest form: not one counter moves.
func TestAppendActiveSegmentOpenFailureIsRetriable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	count, size, woff := s.count(), s.size(), s.writeOffset()
	added := s.stats().Added

	// Drop the active segment's descriptor and make reopening it impossible: an
	// operator's chmod, an SELinux relabel, a filesystem that went away. The file
	// itself is untouched, so this is emphatically not corruption.
	path := s.filePath(s.files[0].num)
	appendDetachHandle(t, s, 0)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}

	aerr := s.append(idxRec(1))
	if aerr == nil {
		t.Fatal("append into a segment that cannot be opened should fail")
	}
	if !errors.Is(aerr, os.ErrPermission) {
		t.Fatalf("append: %v, want the open failure returned as itself", aerr)
	}
	if errors.Is(aerr, ErrIO) {
		t.Fatalf("a failed open must not poison the store: %v", aerr)
	}
	if errors.Is(aerr, ErrCorrupt) {
		t.Fatalf("a failed open must not be laundered into corruption: %v", aerr)
	}
	if s.failure() != nil {
		t.Fatalf("store poisoned by a failed open: %v", s.failure())
	}
	if s.count() != count || s.size() != size || s.writeOffset() != woff {
		t.Fatalf("the failed append moved the store: count=%d/%d size=%d/%d off=%d/%d",
			s.count(), count, s.size(), size, s.writeOffset(), woff)
	}
	if got := s.stats().Added; got != added {
		t.Fatalf("Added=%d, want %d: a refused append is not an accepted record", got, added)
	}

	// Retriable means retriable: once the condition clears the same append works,
	// into the same space, and both records are there in order.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(1))
	if got := s.count(); got != count+1 {
		t.Fatalf("count=%d after the retry, want %d", got, count+1)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// And a reopen agrees: exactly the two records that were really appended.
	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != 2 {
		t.Fatalf("count=%d after reopen, want 2", got)
	}
	if got := s2.corruptionCount(); got != 0 {
		t.Fatalf("corruptions=%d: a failed open destroys nothing", got)
	}
	for i := 0; i < 2; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		if err := s2.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAppendDataSyncFailurePublishesNothing covers append's per-op DATA fdatasync
// arm with a REAL fdatasync failure — the store calls datasync and the kernel
// says no — rather than the injected one the faults build uses.
//
// The descriptor is the null device: pwrite(2) reports the full count, fdatasync(2)
// answers EINVAL because the device has no fsync operation. That is precisely the
// state the arm is written for, and the only one reachable without a privileged
// block-device fault injector: the record's bytes were accepted and then could
// not be made durable.
//
// Two things have to hold. The failure is class (3), a durability failure, so it
// latches ErrIO and keeps saying so even after the descriptor recovers — a second
// fsync would report success over pages the kernel already dropped. And nothing
// is published: the cursors have not advanced at this point in append, so the
// record must be invisible both to this store and to a reopen.
func TestAppendDataSyncFailurePublishesNothing(t *testing.T) {
	null, syncErr := appendNullHandle(t) // skips where this descriptor state does not exist
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0) // per-op policy: a real fdatasync per record
	if err != nil {
		_ = null.Close()
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	count, size, woff := s.count(), s.size(), s.writeOffset()

	// The per-op path fsyncs and clears dirty at the end of every append, so this
	// is a clean file. That makes dirty a witness below: only writeRecord sets it.
	if s.files[0].dirty {
		t.Fatal("the per-op path should have left the segment clean")
	}
	appendInstallHandle(t, s, 0, null)

	aerr := s.append(idxRec(1))
	if !errors.Is(aerr, ErrIO) {
		t.Fatalf("append whose data fsync failed: %v, want ErrIO", aerr)
	}
	if want := appendRootErr(syncErr); !errors.Is(aerr, want) {
		t.Fatalf("append: %v, want it to carry the fdatasync errno %v", aerr, want)
	}
	// Proof the record write really did succeed and the failure came from the
	// fdatasync after it: writeRecord is the only thing that marks the file dirty,
	// and append returned before reaching the `af.dirty = false` at its end.
	if !s.files[0].dirty {
		t.Fatal("the record write did not happen: the append failed before the data fsync, so this test proves nothing")
	}
	if !errors.Is(s.failure(), ErrIO) {
		t.Fatalf("failure() = %v, want the latched ErrIO", s.failure())
	}
	if s.count() != count || s.size() != size || s.writeOffset() != woff {
		t.Fatalf("an unsynced record was published: count=%d/%d size=%d/%d off=%d/%d",
			s.count(), count, s.size(), size, s.writeOffset(), woff)
	}

	// The latch outlives the condition. A healthy descriptor does not make the
	// lost writeback reappear, so every later durability claim still refuses.
	repairHandle(t, s, 0)
	if err := s.append(idxRec(2)); !errors.Is(err, ErrIO) {
		t.Fatalf("append after the descriptor recovered: %v, want the latch to hold", err)
	}
	if err := s.sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("sync after the descriptor recovered: %v, want the latch to hold", err)
	}
	if err := s.close(); !errors.Is(err, ErrIO) {
		t.Fatalf("close: %v, want the latched ErrIO reported once more", err)
	}

	// The reopen is the real assertion: "Add failed" has to mean the item is not
	// in the log, and the record whose bytes were never made durable must not be
	// there — nor may its absence look like damage.
	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != count {
		t.Fatalf("count=%d after reopen, want %d: the refused record became visible", got, count)
	}
	if got := s2.corruptionCount(); got != 0 {
		t.Fatalf("corruptions=%d: a record that was never published is a clean tail, not damage", got)
	}
	p, _, ok, err := s2.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("first record after reopen: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("past the last durable record: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// TestDrainStopsWhenClosedMidIteration covers Reader.next's w.closed arm, which
// is only reachable *between* two items: stream checks closed once before the
// loop, so a queue closed before iteration begins never reaches next at all
// (TestEveryMethodRejectsUseAfterClose covers that one). Closing from inside the
// loop body — the lock is released across a yield, and the docs invite calling
// other methods there — is the case this arm exists for.
//
// The contract is that this is an ordinary end of iteration, not a failure: Err
// must stay nil, exactly as it does for a drained queue or a cancelled context.
func TestDrainStopsWhenClosedMidIteration(t *testing.T) {
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

	var got []uint64
	for v := range r.Drain(context.Background()) {
		got = append(got, v)
		if len(got) == 1 {
			// The queue lock is free here, so this is a caller doing something the
			// package explicitly permits, not a contrived race.
			if err := w.Close(); err != nil {
				t.Fatalf("Close from inside the loop: %v", err)
			}
		}
	}
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("drain yielded %v, want just the item delivered before Close", got)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("Err=%v, want nil: a closed queue is an ordinary reason to stop", err)
	}
	// And the queue really is closed rather than merely out of items.
	if _, ok, err := r.TryTake(); ok || !errors.Is(err, ErrClosed) {
		t.Fatalf("TryTake after the loop: ok=%v err=%v, want ErrClosed", ok, err)
	}
	// Nothing was lost on the way out: the two items Close raced past are still
	// uncommitted, and Stats stays readable after Close to say so.
	if got := w.Count(); got != 2 {
		t.Fatalf("Count=%d after Close, want the 2 unconsumed items", got)
	}
}

// TestFollowStopsWhenCloseWakesTheWaiter is the same arm reached the way it
// actually happens in production: a follower is parked in waitLocked on an empty
// queue and Close closes the notify channel underneath it. Whichever order the
// two goroutines interleave, the follower re-checks w.closed on the way round
// and must return — if it did not, Close would hang forever on the second half
// of its own lock acquisition and every Follow loop would be unkillable.
func TestFollowStopsWhenCloseWakesTheWaiter(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	if err := w.Add(42); err != nil {
		t.Fatal(err)
	}

	yielded := make(chan uint64, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := range r.Follow(context.Background()) {
			yielded <- v
		}
	}()

	// Waiting for the item is what tells us the follower is inside the iteration
	// (so stream's pre-loop closed check is behind it) and about to park.
	select {
	case v := <-yielded:
		if v != 42 {
			t.Errorf("Follow yielded %d, want 42", v)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Follow never delivered the item")
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Follow did not return after Close: the waiter was never woken, or woke and looped")
	}
	if err := r.Err(); err != nil {
		t.Fatalf("Err=%v, want nil: Close is an ordinary reason for Follow to stop", err)
	}
	select {
	case v := <-yielded:
		t.Fatalf("Follow yielded %d after Close", v)
	default:
	}
}

// TestDrainCommitFailureDoesNotYieldTheItem covers Reader.next's failed-commit
// arm. The iterators commit as they read, so a commit that does not stick leaves
// the item's fate ambiguous, and the resolution is deliberate: stop WITHOUT
// yielding. Handing the value over would present a record as consumed that a
// reopen is going to deliver again — at-most-once semantics silently turning into
// at-least-once *duplicates* inside a loop the caller believes is exactly-once.
//
// The failure is a read-only descriptor: reads still work (so the record really
// is read — this is not the read-failure path TestDrainReportsErrorViaErr
// exercises) and the header write behind the commit is what fails.
func TestDrainCommitFailureDoesNotYieldTheItem(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	const n = 3
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	reopenReadOnly(t, w.st, 0) // pread keeps working; every pwrite fails

	var got []uint64
	for v := range r.Drain(context.Background()) {
		got = append(got, v)
	}
	if len(got) != 0 {
		t.Fatalf("drain yielded %v: an item whose commit failed must not be handed over", got)
	}
	if r.Err() == nil {
		t.Fatal("Err must report the failed commit; an empty iteration is indistinguishable otherwise")
	}
	if errors.Is(r.Err(), ErrCorrupt) || errors.Is(r.Err(), ErrCodec) {
		t.Fatalf("Err=%v: a failed header write is neither corruption nor a codec fault", r.Err())
	}
	// The read itself succeeded — that is what makes this the commit arm and not
	// the read arm. takeHead counted the delivery and nothing rewound it.
	if got := w.Stats().Delivered; got != 1 {
		t.Fatalf("Delivered=%d, want 1: the record was read, and only its commit failed", got)
	}

	// The item replays instead of being consumed. The in-memory commit cursor did
	// move (whatever committed before the failure stands), so the proof is on
	// disk: the header carrying it never landed, and a reopen offers all three
	// records again.
	repairHandle(t, w.st, 0)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	if got := w2.Count(); got != n {
		t.Fatalf("Count=%d after reopen, want %d: an uncommitted record was retired", got, n)
	}
	want := uint64(0)
	for v := range w2.NewReader().Drain(context.Background()) {
		if v != want {
			t.Fatalf("replay out of order: got %d, want %d", v, want)
		}
		want++
	}
	if want != n {
		t.Fatalf("replayed %d of %d records", want, n)
	}
}

// TestCodecPanicLeavesTheRecordQueued is the other half of
// TestPanicInUnmarshalReleasesLock: that one proves the mutex is released, this
// one proves the queue is still *correct* afterwards. Reader.read arms its rewind
// with a defer rather than a branch precisely so a panic out of UnmarshalFunc
// unwinds through it — otherwise the record is consumed on the way out, and on
// the Take path the caller's deferred unlock hands a committed cursor to a record
// nobody ever received.
func TestCodecPanicLeavesTheRecordQueued(t *testing.T) {
	boom := true
	w, err := New[uint64](t.TempDir(), marshalU64, func(b []byte) (uint64, error) {
		if boom {
			panic("codec blew up")
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

	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected the codec panic to propagate to the caller")
			}
		}()
		_, _, _ = r.TryTake()
	}()

	if w.Empty() || w.Count() != 1 {
		t.Fatalf("after the panic: Empty=%v Count=%d, want false and 1", w.Empty(), w.Count())
	}
	if got := w.Stats().Delivered; got != 0 {
		t.Fatalf("Delivered=%d, want 0: the record never reached the consumer", got)
	}

	boom = false
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 7 {
		t.Fatalf("take after the panic: v=%d ok=%v err=%v", v, ok, err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count=%d, want 0", got)
	}
}
