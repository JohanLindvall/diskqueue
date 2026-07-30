// UNCOVERED: recovery.go:132 — the `return err` arm of `flushFile` inside load's
// truncated-segment repair loop — is not reached by this file, and cannot be
// reached without adding a seam to non-test code.
//
// At that point flushFile can only fail in datasync: writeHeader has just set
// df.dirty and ensureOpen has just installed df.f, so neither early return in
// flushFile applies. That descriptor is created by load itself, from a dataFile
// that does not exist until load builds it, so the injectors in robust_test.go
// (breakHandle / reopenReadOnly, which swap df.f behind the store's back) have
// nothing to swap — there is no store to reach into before openStore returns.
// fdatasync on a healthy O_RDWR descriptor to a regular file fails only on a
// writeback/device error (EIO) or ENOSPC, neither of which is producible from
// user space on a normal filesystem. In particular RLIMIT_FSIZE — the lever this
// file uses to fail the pwrite in writeHeader and the fallocate in preallocate —
// is checked at write and allocate time only: fdatasync returns success under
// RLIMIT_FSIZE=0 (measured). Reaching that arm needs either a faultPoint on the
// open path (append has one under -tags diskqueue_faults; load does not) or an
// injectable filesystem, and both are production changes.
//
// Every other target block is now covered. Verified with
// `go test -covermode=set -coverprofile=... ./...`:
//
//	recovery.go:31  ReadDir failed .................. covered (TestLoadDirScanFailureIsReturnedRaw)
//	recovery.go:36  entry filter (dir / no prefix) .. covered (TestLoadIgnoresForeignDirectoryEntries)
//	recovery.go:40  segment number unparsable ....... covered (TestLoadIgnoresForeignDirectoryEntries)
//	recovery.go:121 repair: ensureOpen failed ....... covered (TestLoadRepairOpenFailureFailsTheOpen)
//	recovery.go:128 repair: writeHeader failed ...... covered (TestLoadRepairHeaderWriteFailureFailsTheOpen)
//	recovery.go:131 repair: flushFile (sync policy) . covered (TestLoadRepairsTruncatedSegmentDurably)
//	recovery.go:132 repair: flushFile failed ........ NOT covered (see above)
//	recovery.go:143 repair: preallocate failed ...... covered (TestLoadRepairPreallocFailureFailsTheOpen)
//	recovery.go:151 active file: ensureOpen failed .. covered (TestLoadActiveSegmentOpenFailureFailsTheOpen)

//go:build unix

package diskqueue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// These tests drive the arms of load() that only run when the open path itself
// hits something it cannot do: a directory it cannot list, entries that are not
// segments, and each I/O step of the truncated-segment repair loop failing.
//
// They are the error paths of the one function every other operation depends on,
// so the contract they pin is narrow and absolute: load either recovers or fails,
// never both. A failure must come back to the caller as itself (New returns it,
// no half-open store, no released-but-latched state), it must never be laundered
// into ErrCorrupt or ErrIO, and it must never delete or extend a segment — the
// data has to still be there when the condition clears.
//
// Two levers are used, neither of which needs a seam in the library:
//
//	file mode 0444  — the segment can be stat'd and its header pread (loadFile is
//	                  read-only), but every O_RDWR open fails, so ensureOpen is
//	                  the first thing to break.
//	RLIMIT_FSIZE    — a process-wide cap on how far into a regular file a write
//	                  may reach. At 0 every pwrite fails with EFBIG; at the
//	                  segment's current length the 64-byte header write still
//	                  lands but the fallocate that re-extends the segment does
//	                  not. That is what separates the writeHeader arm from the
//	                  preallocate arm, and the two tests below prove which one
//	                  ran by reading the header back off disk.
//
// The file is unix-only for RLIMIT_FSIZE and for file modes meaning anything.

// loadSegPath names segment num inside dir.
func loadSegPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%08d", filePrefix, num))
}

// loadCutSegment truncates segment num to keep bytes of records — the residue a
// power loss can leave, where the header still publishes a write cursor past the
// real end of the file. It returns how many published bytes went away, which is
// what Stats().DiscardedBytes has to report.
func loadCutSegment(t *testing.T, dir string, num uint64, keep int64) uint64 {
	t.Helper()
	_, w, _, _ := readFileHeader(t, dir, num)
	if w <= headerSize+keep {
		t.Fatalf("segment %d publishes %d bytes; cutting to %d would remove nothing",
			num, w-headerSize, keep)
	}
	if err := os.Truncate(loadSegPath(dir, num), headerSize+keep); err != nil {
		t.Fatal(err)
	}
	return uint64(w - (headerSize + keep))
}

// loadFileSize is the length of segment num on disk.
func loadFileSize(t *testing.T, dir string, num uint64) int64 {
	t.Helper()
	fi, err := os.Stat(loadSegPath(dir, num))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// loadCapFileSize caps RLIMIT_FSIZE at n bytes for the whole process and returns
// a restore function. It is idempotent and also registered with t.Cleanup, but
// callers must still call it explicitly the moment the capped operation is over:
// under `go test -v` a t.Fatalf writes to stdout, and if stdout is a file that
// write is subject to the same cap and the failure message is lost.
//
// The cap is verified to bite before it is relied on — a write past it must fail
// with EFBIG — so a platform that does not enforce RLIMIT_FSIZE skips instead of
// silently passing a test that never reached its target.
func loadCapFileSize(t *testing.T, n uint64) func() {
	t.Helper()
	probeDir := t.TempDir() // created before the cap; mkdir is not limited by it
	var old syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &old); err != nil {
		t.Skipf("RLIMIT_FSIZE unavailable: %v", err)
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: n, Max: old.Max}); err != nil {
		t.Skipf("cannot lower RLIMIT_FSIZE: %v", err)
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &old); err != nil {
			panic(fmt.Sprintf("restoring RLIMIT_FSIZE: %v", err))
		}
	}
	t.Cleanup(restore)

	f, err := os.Create(filepath.Join(probeDir, "probe"))
	if err != nil {
		restore()
		t.Fatalf("probe file: %v", err)
	}
	// A write that starts at the cap is refused outright (a write that merely
	// crosses it is clamped short instead), so this is the unambiguous check.
	_, werr := f.WriteAt([]byte{0}, int64(n))
	_ = f.Close()
	if !errors.Is(werr, syscall.EFBIG) {
		restore()
		t.Skipf("RLIMIT_FSIZE=%d is not enforced here: a write at the cap returned %v", n, werr)
	}
	return restore
}

// TestLoadDirScanFailureIsReturnedRaw: load starts by listing the directory, and
// a listing it could not do says nothing about the segments in it. It has to come
// back as itself — the three-way classification puts EACCES in the retriable
// class, and laundering it into ErrCorrupt is exactly what once let a chmod blip
// unlink a healthy segment.
//
// Reaching the ReadDir arm at all takes a white-box call: openStore opens (and
// flocks) the directory before load runs, and that open needs the same read
// permission ReadDir does, so through the public path the failure always lands
// one step earlier. Both steps are asserted here.
func TestLoadDirScanFailureIsReturnedRaw(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can list a 0000 directory")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(7))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o755) }()

	// The public path: New/openStore fails on the directory handle it takes before
	// load, and hands back no store at all.
	if bad, oerr := openStore(dir, 4096, 0, true, 0, 0); oerr == nil {
		_ = bad.close()
		t.Fatal("openStore on an unreadable directory should fail")
	} else if !errors.Is(oerr, os.ErrPermission) {
		t.Fatalf("openStore: %v, want a permission error", oerr)
	}

	// The scan itself, on a store that has not been loaded yet.
	ls := &store{dir: dir, segmentSize: 4096, noSync: true}
	lerr := ls.load()
	if lerr == nil {
		t.Fatal("load must fail when it cannot list the directory")
	}
	if !errors.Is(lerr, os.ErrPermission) {
		t.Fatalf("load: %v, want a permission error", lerr)
	}
	if errors.Is(lerr, ErrCorrupt) {
		t.Fatalf("load: %v — a directory it could not read is not evidence of damage", lerr)
	}
	if errors.Is(lerr, ErrIO) {
		t.Fatalf("load: %v — a failed listing is retriable, it must not latch", lerr)
	}
	// It has to be the *listing* that failed, and be reported against the
	// directory itself. Swallowing the scan error instead would land on the same
	// permission complaint one step later — load would see no segments, try to
	// create data.00000001 in the same unreadable directory, and fail there — so
	// without naming the path this assertion cannot tell the two apart.
	var pe *os.PathError
	if !errors.As(lerr, &pe) {
		t.Fatalf("load: %v, want an *os.PathError naming the directory", lerr)
	}
	if pe.Path != dir {
		t.Fatalf("load failed on %q (op %q), want the directory scan of %q", pe.Path, pe.Op, dir)
	}
	if len(ls.files) != 0 || ls.nextNum != 0 || ls.writeOff != 0 || ls.ioErr != nil {
		t.Fatalf("failed scan left state behind: files=%d nextNum=%d writeOff=%d ioErr=%v",
			len(ls.files), ls.nextNum, ls.writeOff, ls.ioErr)
	}

	// Nothing was deleted or invented: the record is still there once the
	// directory can be read again.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	good, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("reopen once readable: %v", err)
	}
	defer func() { _ = good.close() }()
	if got := good.count(); got != 1 {
		t.Fatalf("count=%d, want 1: the failed scan must not have cost a record", got)
	}
	if good.corruptionCount() != 0 {
		t.Fatalf("corruptions=%d: nothing was damaged", good.corruptionCount())
	}
	p, _, ok, err := good.takeHead()
	if err != nil || !ok || recIdx(p) != 7 {
		t.Fatalf("record after the failed scan: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
}

// TestLoadIgnoresForeignDirectoryEntries: the queue directory is a directory like
// any other — an operator's note, a stray temp file, a subdirectory that happens
// to look like a segment. Anything that is not a numbered segment file is skipped
// by the scan, not fed to loadFile.
//
// The filter is load-bearing in both directions. Without the IsDir/prefix arm,
// "data.00000009" (a directory) would be stat'd as a segment and its header read
// with io.ReadFull, which answers EISDIR — an error loadFile returns raw, so the
// whole queue would fail to open because someone made a subdirectory. Without the
// ParseUint arm, an unparsable suffix decodes to segment 0, whose file does not
// exist, and the open fails on ENOENT. And a foreign entry must never be deleted:
// load only unlinks files it identified as segments.
func TestLoadIgnoresForeignDirectoryEntries(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// A subdirectory whose name is a perfectly well-formed segment number, and
	// three files the number parser must reject.
	junkDir := filepath.Join(dir, "data.00000009")
	if err := os.Mkdir(junkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	junkFiles := []string{"README", "data.", "data.tmp", "data.99999999999999999999"}
	for _, name := range junkFiles {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("not a segment"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatalf("open with foreign directory entries: %v, want them ignored", err)
	}
	defer func() { _ = s2.close() }()

	// Only the real segment was recovered, and numbering resumed past it — proof
	// the directory named "data.00000009" was filtered rather than parsed.
	if len(s2.files) != 1 || s2.files[0].num != 1 {
		t.Fatalf("recovered %d segments, want just data.00000001", len(s2.files))
	}
	if s2.nextNum != 2 {
		t.Fatalf("nextNum=%d, want 2: a foreign entry influenced the numbering", s2.nextNum)
	}
	if got := s2.count(); got != 2 {
		t.Fatalf("count=%d, want 2", got)
	}
	if s2.corruptionCount() != 0 || s2.foreignSegments != 0 {
		t.Fatalf("corruptions=%d foreignSegments=%d: junk in the directory is not data loss",
			s2.corruptionCount(), s2.foreignSegments)
	}
	got, events := drainRecovering(t, s2)
	if events != 0 {
		t.Fatalf("%d corruption events, want 0", events)
	}
	if len(got) != 2 {
		t.Fatalf("delivered %v, want both records", got)
	}
	assertAscending(t, got)

	// And nothing that was not a segment was touched.
	if fi, serr := os.Stat(junkDir); serr != nil || !fi.IsDir() {
		t.Fatalf("the subdirectory was removed: %v", serr)
	}
	for _, name := range junkFiles {
		if _, serr := os.Stat(filepath.Join(dir, name)); serr != nil {
			t.Fatalf("%s was removed by the open: %v", name, serr)
		}
	}
}

// TestLoadRepairsTruncatedSegmentDurably is the success path of the repair loop,
// and specifically its fsync: on a store that promises durability the clamped
// header is flushed inside load, not left for a later write to carry.
//
// It matters because the repair is the only thing that stops a truncated segment
// from being rediscovered forever. A cut segment publishes a write cursor past
// the end of its own file; load clamps the cursor, corrects the record count, and
// re-extends the file to the segment geometry. If any of that failed to reach
// disk, the next open would book the same DiscardedBytes and raise the same
// ErrCorrupt again — and again — while the backlog never changed. So the second
// open here must be completely quiet, and it is the assertion the whole loop
// hangs on.
func TestLoadRepairsTruncatedSegmentDurably(t *testing.T) {
	dir := t.TempDir()
	const recLen = 1 + 8 + checksumSize // uvarint(8) + payload + checksum trailer
	// Sync on (NoSync deliberately left false): flushFile inside the repair loop
	// only runs under a durability policy, which is the arm under test.
	w, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Cut on a record boundary: exactly one whole record survives, so what the
	// queue is allowed to deliver afterwards is unambiguous.
	cut := loadCutSegment(t, dir, 1, recLen)

	w2, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatalf("open of a truncated segment: %v, want the survivor recovered", err)
	}
	st := w2.Stats()
	if st.DiscardedBytes != cut {
		t.Fatalf("DiscardedBytes=%d, want %d (the bytes the header published and the file lost)",
			st.DiscardedBytes, cut)
	}
	if st.Corruptions != 1 {
		t.Fatalf("Corruptions=%d, want 1", st.Corruptions)
	}
	if got := w2.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: the header's count describes records that are gone", got)
	}
	// The repair ran to the end: the header on disk carries the clamped cursor and
	// the corrected count, and preallocate put the segment back to full geometry
	// (a short file would be read as a different SegmentSize on the next open).
	commit, wc, written, _ := readFileHeader(t, dir, 1)
	if wc != headerSize+recLen || written != 1 {
		t.Fatalf("repaired header: writeCursor=%d writtenCount=%d, want %d and 1",
			wc, headerSize+recLen, written)
	}
	if commit != headerSize {
		t.Fatalf("commit cursor=%d, want %d: the repair must not retire anything", commit, headerSize)
	}
	if got := loadFileSize(t, dir, 1); got != headerSize+4096 {
		t.Fatalf("segment size on disk=%d, want %d: the repair did not re-extend it",
			got, headerSize+4096)
	}
	// The fsync happened inside load, not merely a page-cache write left for
	// somebody else to flush: a segment still marked dirty here means the clamped
	// header is not durable, and the power loss that truncated the file in the
	// first place would put the store straight back where it started.
	if w2.st.files[0].dirty {
		t.Fatal("the repaired header was never flushed: the segment is still dirty after load")
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	// The repair was durable, so this open finds an ordinary partly-filled
	// segment: no truncation detected, no loss booked, nothing to report.
	w3, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatalf("reopen after the repair: %v", err)
	}
	defer func() { _ = w3.Close() }()
	st = w3.Stats()
	if st.DiscardedBytes != 0 || st.Corruptions != 0 {
		t.Fatalf("second open re-detected the same truncation: DiscardedBytes=%d Corruptions=%d",
			st.DiscardedBytes, st.Corruptions)
	}
	if got := w3.Count(); got != 1 {
		t.Fatalf("Count=%d after the repair, want 1", got)
	}
	r := w3.NewReader()
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 0 {
		t.Fatalf("survivor: v=%d ok=%v err=%v", v, ok, err)
	}
	// The re-extended tail is preallocated zeros; it must not read back as records.
	if _, ok, err := r.TryTake(); ok || err != nil {
		t.Fatalf("past the survivor: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// TestLoadRepairOpenFailureFailsTheOpen: the repair needs the segment writable,
// and when it cannot have it the open fails rather than continuing with a segment
// whose header still points past the end of its file.
//
// The victim is a *leading* segment, not the active one, which is the case the
// loop exists for: nothing else ever rewrites a middle segment's header, and the
// active file's own ensureOpen (at the end of load) would not have covered it.
// Mode 0444 is chosen so everything before the repair still works — stat and the
// header pread are read-only, so the segment is fully recovered and only the
// O_RDWR open fails. The failure must stay a permission error: nothing may be
// dropped, nothing re-extended, and the queue must come back intact when the mode
// is fixed.
func TestLoadRepairOpenFailureFailsTheOpen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 30 // enough for several 11-byte-record segments
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if len(s.files) < 2 {
		t.Fatalf("%d segments, need the cut one to be a leading segment", len(s.files))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	cut := loadCutSegment(t, dir, 1, idxRecLen) // one record survives in segment 1
	shortSize := loadFileSize(t, dir, 1)
	if err := os.Chmod(loadSegPath(dir, 1), 0o444); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(loadSegPath(dir, 1), 0o644) }()

	bad, err := openStore(dir, 128, 0, false, 0, 0)
	if err == nil {
		_ = bad.close()
		t.Fatal("a segment the repair cannot open must fail the open")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("open: %v, want a permission error", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v — a segment it could not open is not a segment it may drop", err)
	}
	if errors.Is(err, ErrIO) {
		t.Fatalf("open: %v — a failed open is retriable, it must not latch", err)
	}
	if _, serr := os.Stat(loadSegPath(dir, 1)); serr != nil {
		t.Fatalf("the segment was destroyed by the failed repair: %v", serr)
	}
	if got := loadFileSize(t, dir, 1); got != shortSize {
		t.Fatalf("segment size=%d, want %d: nothing may be written before the repair can open it",
			got, shortSize)
	}

	// Once it is writable the repair completes, the survivor and every later
	// segment are delivered in order, and the cut tail is reported exactly once.
	if err := os.Chmod(loadSegPath(dir, 1), 0o644); err != nil {
		t.Fatal(err)
	}
	good, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen once writable: %v", err)
	}
	defer func() { _ = good.close() }()
	if good.discardedBytes != cut {
		t.Fatalf("DiscardedBytes=%d, want %d", good.discardedBytes, cut)
	}
	got, events := drainRecovering(t, good)
	if events != 1 {
		t.Fatalf("%d corruption events, want 1 (the truncation)", events)
	}
	assertAscending(t, got)
	if len(got) == 0 || got[0] != 0 || got[len(got)-1] != n-1 {
		t.Fatalf("delivered %v, want the survivor of segment 1 through the last record", got)
	}
	if c := good.count(); c != 0 {
		t.Fatalf("count=%d after a full drain, want 0", c)
	}
}

// TestLoadRepairHeaderWriteFailureFailsTheOpen: the clamped cursor is published
// with a plain pwrite, and when that pwrite fails the open fails with it.
//
// The failure is injected with RLIMIT_FSIZE=0, which refuses every write to a
// regular file while leaving opens and reads alone — so load gets all the way to
// writeHeader and no further. What proves it is that block and not the next one
// is the header still on disk afterwards: it must be the *original*, over-
// promising header (the cursor and count from before the truncation), because a
// failed write publishes nothing. The preallocate test below asserts the mirror
// image, so between them the two arms cannot be confused.
func TestLoadRepairHeaderWriteFailureFailsTheOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
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
	_, wantCursor, wantWritten, _ := readFileHeader(t, dir, 1)
	cut := loadCutSegment(t, dir, 1, idxRecLen)
	shortSize := loadFileSize(t, dir, 1)

	restore := loadCapFileSize(t, 0)
	bad, err := openStore(dir, 4096, 0, false, 0, 0)
	restore() // before any t.Fatalf: under -v its output is a write too
	if err == nil {
		_ = bad.close()
		t.Fatal("a repair whose header write fails must fail the open")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("open: %v, want the injected write failure (EFBIG)", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v — a failed write is not damage", err)
	}
	if errors.Is(err, ErrIO) {
		t.Fatalf("open: %v — a failed write leaves nothing published, so it must not latch", err)
	}
	// Nothing reached the disk: the segment still has its old length and its old,
	// over-promising header.
	if got := loadFileSize(t, dir, 1); got != shortSize {
		t.Fatalf("segment size=%d, want %d: the file was extended after the header write failed",
			got, shortSize)
	}
	_, cursor, written, _ := readFileHeader(t, dir, 1)
	if cursor != wantCursor || written != wantWritten {
		t.Fatalf("header on disk: writeCursor=%d written=%d, want the unrepaired %d and %d",
			cursor, written, wantCursor, wantWritten)
	}

	// The store is not damaged by the failed attempt: it opens, repairs and
	// delivers the survivor as soon as writing works again.
	good, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen once writes work: %v", err)
	}
	defer func() { _ = good.close() }()
	if good.discardedBytes != cut {
		t.Fatalf("DiscardedBytes=%d, want %d", good.discardedBytes, cut)
	}
	if c := good.count(); c != 1 {
		t.Fatalf("count=%d, want 1 survivor", c)
	}
	got, events := drainRecovering(t, good)
	if events != 1 || len(got) != 1 || got[0] != 0 {
		t.Fatalf("delivered %v with %d events, want just the survivor and one report", got, events)
	}
}

// TestLoadRepairPreallocFailureFailsTheOpen: the last step of the repair puts the
// segment back to its full preallocated length, and a failure there fails the
// open too.
//
// The cap is set to the truncated segment's own length, so the 64-byte header
// write still fits and only the fallocate that would grow the file to
// headerSize+SegmentSize is refused. The header on disk therefore has to be the
// *repaired* one — which is the ordering the loop is written for: the clamped
// cursor is made durable first, and only then is the file re-extended, so a crash
// in between can never leave a full-length segment whose header points past its
// real records (the zero fill would read back as a phantom backlog).
//
// The price of that ordering is asserted too: a segment left short with a header
// that matches its length is indistinguishable from a store built with a smaller
// SegmentSize, so reopening with the configured size is refused with
// ErrSegmentSizeMismatch. The records are still there — no byte was lost — but
// the store cannot be opened with its own geometry until it can be re-extended.
func TestLoadRepairPreallocFailureFailsTheOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
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
	loadCutSegment(t, dir, 1, idxRecLen)
	shortSize := loadFileSize(t, dir, 1)

	// Room for the header write, none for the re-extension.
	restore := loadCapFileSize(t, uint64(shortSize))
	bad, err := openStore(dir, 4096, 0, false, 0, 0)
	restore()
	if err == nil {
		_ = bad.close()
		t.Fatal("a repair that cannot re-extend the segment must fail the open")
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("open: %v, want the injected allocation failure (EFBIG)", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v — a failed allocation is not damage", err)
	}
	if errors.Is(err, ErrIO) {
		t.Fatalf("open: %v — a failed allocation publishes nothing, so it must not latch", err)
	}
	if got := loadFileSize(t, dir, 1); got != shortSize {
		t.Fatalf("segment size=%d, want %d unchanged", got, shortSize)
	}
	// The clamped cursor got there first, which is the whole point of doing the
	// re-extension last.
	_, cursor, written, _ := readFileHeader(t, dir, 1)
	if cursor != headerSize+idxRecLen || written != 1 {
		t.Fatalf("header on disk: writeCursor=%d written=%d, want the repaired %d and 1",
			cursor, written, headerSize+idxRecLen)
	}

	// The consequence of stopping there: with the header now matching the file's
	// length, the short segment reads as a different geometry.
	if mism, merr := openStore(dir, 4096, 0, false, 0, 0); !errors.Is(merr, ErrSegmentSizeMismatch) {
		if merr == nil {
			_ = mism.close()
		}
		t.Fatalf("reopen with the configured segment size: %v, want ErrSegmentSizeMismatch", merr)
	}
	// And no record was lost by any of it: the survivor is still readable under
	// the geometry the repaired header describes.
	good, err := openStore(dir, idxRecLen, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen with the surviving geometry: %v", err)
	}
	defer func() { _ = good.close() }()
	if good.corruptionCount() != 0 || good.discardedBytes != 0 {
		t.Fatalf("corruptions=%d discardedBytes=%d: the repaired header must not re-report the cut",
			good.corruptionCount(), good.discardedBytes)
	}
	if c := good.count(); c != 1 {
		t.Fatalf("count=%d, want the 1 surviving record", c)
	}
	p, _, ok, err := good.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("survivor: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
}

// TestLoadActiveSegmentOpenFailureFailsTheOpen: load ends by opening the active
// segment, because every append writes into it. If that open fails there is no
// queue to hand back — an open that succeeded here would return a store whose
// very first Add reaches for a nil handle.
//
// Mode 0444 puts the failure exactly on that call and nowhere earlier: loadFile
// only stats the file and preads its header, both of which a read-only mode
// allows, and no segment is truncated so the repair loop above never runs. (The
// existing coverage of this shape, TestLoadOpenErrorIsNotCorruption, uses 0000,
// which fails one step earlier in readHeader.) Nothing may be deleted, and the
// directory lock must have been released — proved by the reopen below, which
// would answer ErrLocked if openStore had leaked its half-open store.
func TestLoadActiveSegmentOpenFailureFailsTheOpen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 30
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	active := s.active().num
	if active < 2 {
		t.Fatalf("active segment is %d, want several segments so only the last is unwritable", active)
	}
	want := s.count()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(loadSegPath(dir, active), 0o444); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(loadSegPath(dir, active), 0o644) }()

	bad, err := openStore(dir, 128, 0, false, 0, 0)
	if err == nil {
		_ = bad.close()
		t.Fatal("an active segment that cannot be opened for writing must fail the open")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("open: %v, want a permission error", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v — a segment it could not open is not a segment it may drop", err)
	}
	if got := countDataFiles(t, dir); got != int(active) {
		t.Fatalf("%d segments left on disk, want %d: the failed open deleted something", got, active)
	}

	// Everything is still there, and the lock came back with the failure.
	if err := os.Chmod(loadSegPath(dir, active), 0o644); err != nil {
		t.Fatal(err)
	}
	good, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen once writable: %v (ErrLocked would mean the failed open leaked its lock)", err)
	}
	defer func() { _ = good.close() }()
	if got := good.count(); got != want {
		t.Fatalf("count=%d, want %d: records were lost", got, want)
	}
	if good.corruptionCount() != 0 {
		t.Fatalf("corruptions=%d: nothing was damaged", good.corruptionCount())
	}
	got, events := drainRecovering(t, good)
	if events != 0 || len(got) != n {
		t.Fatalf("delivered %d records with %d events, want %d and 0", len(got), events, n)
	}
	assertAscending(t, got)
}
