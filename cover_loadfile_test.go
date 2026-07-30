package diskqueue

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// UNCOVERED: nothing. All eight target blocks in loadFile — recovery.go 196
// (os.Stat), 213 (readHeader said ErrCorrupt), 248 and 251 (the write-cursor
// clamps), 275 and 278 (the committed-count clamps), 282 and 285 (the
// commit-cursor clamps) — are reached by the tests below, as is readHeader's own
// short-read wrap (368-371), which 213 needs in order to fire.
//
// One caveat, on 285 (`if cc > headerSize+df.size`). The block is executed and the
// resulting behaviour is asserted, but *removing* the clamp does not change any
// observable behaviour, so no test can distinguish "clamped" from "not clamped"
// there today. The reason is that load's only consumer of cc is
//
//	if commitCurs[i] < headerSize+df.size { commitOff = df.base + (cc - headerSize) }
//
// and that comparison is false both for the clamped value (which is exactly
// headerSize+df.size) and for anything larger, so the segment is skipped either
// way. The clamp is a guard on the value, not a branch in the recovery. Mutation
// testing bears this out: deleting the clamp leaves TestLoadFileCommitCursorPast-
// SegmentClamped green, while clamping to the wrong end (headerSize) or clamping
// unconditionally both fail it — so the test does pin the clamp's *value* and the
// confinement behaviour around it, just not its existence. Every other target
// block has a mutant that the corresponding test catches.
//
// loadFile's triage and clamp arms: the eight branches that decide, for one
// segment, whether recovery may delete it, how much of it exists, and what its
// counts are allowed to say.
//
// Everything here forges on-disk residue between a close and a reopen (the
// recovery_fault_test.go model, reusing its forgeHeader) or makes the segment
// unreachable without damaging it. Two rules are under test throughout:
//
//   - The three-way classification. Corruption may drop data, and is counted and
//     owed to the reader as one ErrCorrupt. A retriable error ("I could not look")
//     fails the open and deletes nothing. Laundering the second into the first is
//     how an audit found a chmod blip unlinking a healthy segment.
//   - A header is data, not truth. Every figure taken out of one is clamped into
//     the geometry the file actually has, because a scribbled word must produce a
//     wrong number, never an impossible one: a negative segment size, a read cursor
//     below zero or a Count() promising a backlog no drain can deliver are each a
//     way for one bad byte to take the whole queue down.

// loadfileSegSize keeps segments small enough that a handful of records fills
// one, so multi-segment layouts stay cheap. Records are 11 bytes (1 length byte,
// a 2-byte idxRec payload, an 8-byte checksum), so a full segment holds 11.
const loadfileSegSize = 128

// loadfileFill appends indexed records until segment segs is the active one — so
// segments 1..segs-1 are full — and then extra more into it. It returns the total
// number of records appended, which is also the exclusive upper bound on the
// indices they carry.
func loadfileFill(t *testing.T, s *store, segs uint64, extra int) int {
	t.Helper()
	n := 0
	for s.active().num < segs {
		mustAppend(t, s, idxRec(n))
		n++
	}
	for i := 0; i < extra; i++ {
		mustAppend(t, s, idxRec(n))
		n++
	}
	return n
}

// loadfileCommitN consumes and commits the first n records, so the commit cursor
// comes to rest partway into the first segment — the only position in which a
// segment keeps its header's committed count through load's reconciliation pass
// (a segment wholly below the cursor is rewritten to written, one wholly above to
// zero), and therefore the only position in which the committed-count clamps are
// observable at all.
func loadfileCommitN(t *testing.T, s *store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, off, ok, err := s.takeHead()
		if err != nil || !ok {
			t.Fatalf("takeHead %d: ok=%v err=%v", i, ok, err)
		}
		if err := s.commitTo(off); err != nil {
			t.Fatalf("commitTo %d: %v", i, err)
		}
	}
}

// loadfileWritten returns segment num's record count as its header on disk states
// it, so the tests never hardcode how many records fit in a segment.
func loadfileWritten(t *testing.T, dir string, num uint64) int64 {
	t.Helper()
	_, _, written, _ := readFileHeader(t, dir, num)
	return written
}

// TestLoadFileStatFailureFailsOpenAndDeletesNothing covers the os.Stat arm.
//
// A segment recovery cannot even stat is not evidence of damage: os.Stat says
// nothing about the file's contents, so the open has to fail and leave every byte
// where it is. The failure must arrive as itself (retriable), never wrapped in
// ErrCorrupt — that wrapper is the licence to unlink, and handing it out for a
// permissions blip is how a healthy segment gets destroyed by a recovery that was
// only supposed to report.
func TestLoadFileStatFailureFailsOpenAndDeletesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := loadfileFill(t, s, 2, 3)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// Read permission without search permission: os.ReadDir still lists every
	// segment (a directory read needs only r), while resolving any name *inside*
	// the directory — which is exactly what os.Stat does — fails with EACCES. The
	// segments themselves are untouched and perfectly intact.
	if err := os.Chmod(dir, 0o400); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o755) }()

	bad, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err == nil {
		_ = bad.close()
		t.Fatal("a segment that cannot be stat'd must fail the open")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("open: %v, want the permission error itself", err)
	}
	if errors.Is(err, ErrCorrupt) {
		t.Fatalf("open: %v: a stat failure must not be classified as damage", err)
	}
	// Pin that the failure came from the *segment's* stat rather than the directory
	// read: os.Stat names the path it could not resolve.
	if !strings.Contains(err.Error(), "data.00000001") {
		t.Fatalf("open: %v, want the failure to name the segment it could not stat", err)
	}

	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := countSegments(t, dir); got != 2 {
		t.Fatalf("%d segments on disk, want 2: a directory it could not search must not cost a segment", got)
	}
	good, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen once the directory is searchable again: %v", err)
	}
	defer func() { _ = good.close() }()
	if got := good.count(); got != int64(total) {
		t.Fatalf("count=%d, want %d: records were lost", got, total)
	}
	if good.corruptionCount() != 0 || good.lostSegments != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d: nothing was ever damaged",
			good.corruptionCount(), good.lostSegments)
	}
	got, events := drainRecovering(t, good)
	if len(got) != total || events != 0 {
		t.Fatalf("drained %d records with %d events, want %d and 0", len(got), events, total)
	}
	assertAscending(t, got)
}

// TestLoadFileShortHeaderDropsSegment covers the readHeader-said-ErrCorrupt arm
// (and, with it, readHeader's own short-read wrap).
//
// A non-empty file too short to hold its 64-byte header really is missing bytes,
// which is the one classification that licenses a delete. So it is dropped where
// it sits — in the middle of the sequence, not at the tail — the open succeeds,
// the segments on either side of it are untouched, and the loss is counted and
// owed to the reader as exactly one ErrCorrupt.
func TestLoadFileShortHeaderDropsSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := loadfileFill(t, s, 3, 2)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	w1, w2, w3 := loadfileWritten(t, dir, 1), loadfileWritten(t, dir, 2), loadfileWritten(t, dir, 3)
	if w1+w2+w3 != int64(total) {
		t.Fatalf("segments hold %d+%d+%d records, %d were written", w1, w2, w3, total)
	}

	// Cut the middle segment down to less than a header: the file exists and is not
	// zero-length (which would be a merely aborted create), but io.ReadFull cannot
	// fill 64 bytes out of it.
	if err := os.Truncate(filepath.Join(dir, "data.00000002"), 32); err != nil {
		t.Fatal(err)
	}

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open with a headerless segment: %v, want the surviving segments", err)
	}
	defer func() { _ = rec.close() }()

	if _, serr := os.Stat(filepath.Join(dir, "data.00000002")); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("stat of the damaged segment: %v, want it unlinked", serr)
	}
	for _, num := range []uint64{1, 3} {
		if _, serr := os.Stat(rec.filePath(num)); serr != nil {
			t.Fatalf("healthy segment %d was destroyed: %v", num, serr)
		}
	}
	if rec.lostSegments != 1 || rec.corruptions != 1 {
		t.Fatalf("lostSegments=%d corruptions=%d, want 1 and 1", rec.lostSegments, rec.corruptions)
	}
	if rec.foreignSegments != 0 {
		t.Fatalf("foreignSegments=%d: a damaged header is data loss, not a format change", rec.foreignSegments)
	}
	// dropSegment sizes the loss as size-headerSize under a max(...,0) guard. Here
	// the file is *shorter* than a header, so the payload figure is zero — without
	// the guard the subtraction would underflow into a uint64 near 1.8e19 and
	// LostBytes would report eighteen exabytes of lost queue.
	if rec.lostBytes != 0 {
		t.Fatalf("lostBytes=%d, want 0: a file shorter than its header has no payload to size", rec.lostBytes)
	}
	if got := rec.count(); got != w1+w3 {
		t.Fatalf("count=%d, want %d: only the dropped segment's records are gone", got, w1+w3)
	}

	got, events := drainRecovering(t, rec)
	if len(got) != int(w1+w3) {
		t.Fatalf("delivered %d records, want the %d that survived", len(got), w1+w3)
	}
	assertAscending(t, got)
	if events != 1 {
		t.Fatalf("%d corruption events, want exactly 1: the dropped segment is owed one report", events)
	}
	if rec.corruptions != 1 {
		t.Fatalf("corruptions=%d after the drain, want 1: the surviving segments are intact", rec.corruptions)
	}
}

// TestLoadFileWriteCursorBelowHeaderClamped covers `if w < headerSize`.
//
// A segment's size is derived as writeCursor-headerSize, so a write cursor
// scribbled below the header turns into a *negative* segment size. That is not a
// wrong number, it is an impossible one: every later segment's base is computed by
// summing sizes, so one negative pulls the whole store's offset space backwards
// and the tail of the last segment falls off the end of writeOff. The clamp makes
// the segment read as empty instead.
func TestLoadFileWriteCursorBelowHeaderClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := loadfileFill(t, s, 2, 4)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	w1 := loadfileWritten(t, dir, 1)
	_, writeCur2, w2, _ := readFileHeader(t, dir, 2)

	// Scribble segment 1's write cursor into a word no offset arithmetic can use.
	// Its record count goes with it, so the segment describes itself as empty and
	// the only thing left under test is the clamp on the cursor.
	scribbled := int64(-1)
	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[16:24], uint64(scribbled))
		binary.LittleEndian.PutUint64(h[24:32], 0)
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v, want the intact segment recovered", err)
	}
	defer func() { _ = rec.close() }()

	if got := rec.files[0].size; got != 0 {
		t.Fatalf("segment 1 size=%d, want 0: a segment can never hold a negative number of bytes", got)
	}
	// The offset space is exactly the bytes the remaining segment really holds.
	// Unclamped it would start 64 bytes below zero and writeOff would stop that far
	// short of the last record.
	if got := rec.writeOff; got != writeCur2-headerSize {
		t.Fatalf("writeOff=%d, want %d (segment 2's data alone)", got, writeCur2-headerSize)
	}
	if got := rec.count(); got != w2 {
		t.Fatalf("count=%d, want %d", got, w2)
	}

	got, events := drainRecovering(t, rec)
	if len(got) != int(w2) {
		t.Fatalf("delivered %d records, want segment 2's %d", len(got), w2)
	}
	assertAscending(t, got)
	if got[0] != int(w1) || got[len(got)-1] != total-1 {
		t.Fatalf("delivered %d..%d, want %d..%d", got[0], got[len(got)-1], w1, total-1)
	}
	// The clamp is a geometry guard, not a loss event: nothing on disk was damaged
	// and nothing is counted or reported. The records the unusable cursor hid were
	// never published by a believable cursor in the first place.
	if events != 0 || rec.corruptions != 0 || rec.lostSegments != 0 {
		t.Fatalf("events=%d corruptions=%d lostSegments=%d, want all 0",
			events, rec.corruptions, rec.lostSegments)
	}
	if !rec.empty() {
		t.Fatal("the store did not read empty after a full drain")
	}
}

// TestLoadFileWriteCursorPastGeometryClamped covers `if w > headerSize+segmentSize`.
//
// The file here is exactly its preallocated length, so the geometry-vs-truncation
// discriminator correctly declines to call it truncated — nothing was cut off. Its
// header simply publishes a cursor beyond the segment's capacity. The clamp holds
// the store to the bytes that exist: it may address the whole data region and not
// one byte more. What lies between the real records and that end is preallocated
// zero fill, and the reader's job is to report it as damage and never hand it out
// as records.
func TestLoadFileWriteCursorPastGeometryClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
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

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[16:24], 1<<40) // a terabyte into a 128-byte segment
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rec.close() }()

	if got := rec.writeOff; got != loadfileSegSize {
		t.Fatalf("writeOff=%d, want %d: the store may not address more than the segment holds",
			got, loadfileSegSize)
	}
	// The discriminator's other arm: a file of exactly the right length lost no
	// bytes, so this is not a truncation and nothing is booked as discarded.
	if rec.discardedBytes != 0 || rec.corruptions != 0 {
		t.Fatalf("discardedBytes=%d corruptions=%d: a full-length file was misread as truncated",
			rec.discardedBytes, rec.corruptions)
	}

	got, events := drainRecovering(t, rec)
	if len(got) != n {
		t.Fatalf("delivered %v, want the %d real records", got, n)
	}
	assertAscending(t, got)
	if got[0] != 0 || got[n-1] != n-1 {
		t.Fatalf("delivered %v, want 0..%d", got, n-1)
	}
	// The zero fill the clamped cursor now covers decodes as a run of empty frames
	// whose checksums cannot match. Every one of them is reported and none is
	// delivered, and the queue still reaches its end rather than wedging on them.
	if events == 0 {
		t.Fatal("the zero fill past the real records was neither delivered nor reported")
	}
	if rec.corruptions == 0 {
		t.Fatal("the phantom records were not counted")
	}
	if !rec.empty() {
		t.Fatal("the store did not read empty after stepping past the zero fill")
	}
	if got := rec.count(); got != 0 {
		t.Fatalf("count=%d after draining everything, want 0", got)
	}
}

// TestLoadFileNegativeCommittedCountClamped covers `if df.committed < 0`.
//
// nCommitted is subtracted from nWritten to produce Count(), so a negative
// committed count inflates the backlog: the queue promises records that were never
// written, no drain can deliver them, and it never reads empty again. The clamp
// refuses the impossible figure and the segment is treated as having committed
// nothing.
func TestLoadFileNegativeCommittedCountClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	const acked = 2
	loadfileCommitN(t, s, acked) // leave the cursor inside the segment
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	scribbled := int64(-1000)
	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[32:40], uint64(scribbled))
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rec.close() }()

	if got := rec.count(); got > n {
		t.Fatalf("count=%d, want at most %d: a queue may never claim more records than were written", got, n)
	}
	if got := rec.count(); got <= 0 {
		t.Fatalf("count=%d: the records past the commit cursor are still there", got)
	}

	// The commit cursor is the source of truth for what has been retired, and it is
	// untouched: the records after it replay, and no more.
	got, events := drainRecovering(t, rec)
	if len(got) != n-acked || events != 0 {
		t.Fatalf("delivered %v with %d events, want %d records and 0", got, events, n-acked)
	}
	for i, v := range got {
		if v != acked+i {
			t.Fatalf("delivered %v, want %d..%d", got, acked, n-1)
		}
	}
	if !rec.empty() {
		t.Fatal("the store did not read empty after a full drain")
	}
}

// TestLoadFileCommittedCountAboveWrittenClamped covers `if df.committed > df.written`.
//
// This is the mirror image, and the more dangerous one: an inflated committed
// count drives Count() to zero, so a queue with a real backlog reports itself
// drained — a producer-stopped consumer watching the gauge simply stops. The clamp
// holds the figure to the records the segment actually holds.
func TestLoadFileCommittedCountAboveWrittenClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := loadfileFill(t, s, 2, 4)
	const acked = 2
	loadfileCommitN(t, s, acked) // the cursor stays inside segment 1
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	w1 := loadfileWritten(t, dir, 1)

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[32:40], uint64(w1+9999))
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rec.close() }()

	// Unclamped, segment 1 alone would retire ten thousand records and Count() would
	// bottom out at zero with the whole of segment 2 still undelivered.
	if got := rec.count(); got <= 0 {
		t.Fatalf("count=%d: %d records are still queued", got, total-acked)
	}
	if got := rec.count(); got > int64(total) {
		t.Fatalf("count=%d, want at most %d", got, total)
	}

	got, events := drainRecovering(t, rec)
	if len(got) != total-acked || events != 0 {
		t.Fatalf("delivered %d records with %d events, want %d and 0", len(got), events, total-acked)
	}
	assertAscending(t, got)
	if got[0] != acked || got[len(got)-1] != total-1 {
		t.Fatalf("delivered %d..%d, want %d..%d", got[0], got[len(got)-1], acked, total-1)
	}
	if got := rec.count(); got != 0 {
		t.Fatalf("count=%d after draining everything, want 0", got)
	}
}

// TestLoadFileCommitCursorBelowHeaderClamped covers `if cc < headerSize`.
//
// The commit cursor is stored as a file-relative byte offset and turned into a
// global one by subtracting the header. Below the header it goes negative, and for
// the first segment that puts the recovered read cursor *before* the start of the
// stream — an offset that belongs to no live file. takeHead's answer to that is
// skipCorruptSegment's no-file arm, which abandons everything to the tail and
// force-commits it: one scribbled word, and the entire backlog is quarantined
// unread. Clamped, the segment simply reads as nothing-committed and replays.
func TestLoadFileCommitCursorBelowHeaderClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	loadfileCommitN(t, s, 2) // so there is something for the forged cursor to un-commit
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[8:16], 0)
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rec.close() }()

	if got := rec.count(); got != n {
		t.Fatalf("count=%d, want %d: a cursor at the start of the segment retires nothing", got, n)
	}
	if got := rec.headOff; got != 0 {
		t.Fatalf("headOff=%d, want 0: the read cursor may never sit outside the stream", got)
	}

	// At-least-once: the two records the forged cursor no longer accounts for are
	// delivered again rather than lost, and nothing is reported as damage.
	got, events := drainRecovering(t, rec)
	if len(got) != n || events != 0 {
		t.Fatalf("delivered %v with %d events, want all %d records and 0", got, events, n)
	}
	assertAscending(t, got)
	if got[0] != 0 || got[n-1] != n-1 {
		t.Fatalf("delivered %v, want 0..%d", got, n-1)
	}
	if rec.corruptions != 0 || rec.lostSegments != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d: nothing was damaged, so nothing may be quarantined",
			rec.corruptions, rec.lostSegments)
	}
}

// TestLoadFileCommitCursorPastSegmentClamped covers `if cc > headerSize+df.size`.
//
// A commit cursor past its own segment's end lands, once made global, somewhere
// inside a later segment — or, as here, past the tail of the whole stream. load
// resets the read cursor to it, so the queue would come up already "drained": every
// record silently retired, Count() zero, nothing ever delivered and no corruption
// reported either. Clamped to the segment's own end, the damage is confined to the
// segment whose header was scribbled: that one reads as fully committed, and every
// later segment replays intact.
func TestLoadFileCommitCursorPastSegmentClamped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	total := loadfileFill(t, s, 2, 4)
	loadfileCommitN(t, s, 2)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	w1 := loadfileWritten(t, dir, 1)
	w2 := loadfileWritten(t, dir, 2)

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[8:16], 1<<40)
	})

	rec, err := openStore(dir, loadfileSegSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = rec.close() }()

	if got := rec.count(); got != w2 {
		t.Fatalf("count=%d, want %d: only the forged segment may read as committed", got, w2)
	}
	if got := rec.headOff; got != rec.files[1].base {
		t.Fatalf("headOff=%d, want %d (the start of the next segment)", got, rec.files[1].base)
	}

	got, events := drainRecovering(t, rec)
	if len(got) != int(w2) || events != 0 {
		t.Fatalf("delivered %d records with %d events, want %d and 0", len(got), events, w2)
	}
	assertAscending(t, got)
	if got[0] != int(w1) || got[len(got)-1] != total-1 {
		t.Fatalf("delivered %d..%d, want %d..%d", got[0], got[len(got)-1], w1, total-1)
	}
	if rec.corruptions != 0 || rec.lostSegments != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d: an out-of-range cursor is not damage",
			rec.corruptions, rec.lostSegments)
	}
	if !rec.empty() {
		t.Fatal("the store did not read empty after a full drain")
	}
}
