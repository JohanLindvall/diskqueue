package diskqueue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The recovery contract, from the outside. Two rules run through all of it:
//
//	Corruption degrades to reported loss — never to corrupt output, and never to
//	a queue that stops making progress.
//	Every loss path is observable: one ErrCorrupt per event, and Stats carries
//	the magnitude the error cannot.
//
// These tests are the public-API half; store_test.go and robust_test.go pin the
// same rules at the store level.

// openRecoveryTest opens a small-segment queue whose store the test can reach.
func openRecoveryTest(t *testing.T) (*Queue[uint64], *Reader[uint64], string) {
	t.Helper()
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, w.NewReader(), dir
}

// flipPayload corrupts one byte inside segment idx's data region.
func flipPayload(t *testing.T, w *Queue[uint64], idx, dataOff int) {
	t.Helper()
	flipData(t, w.st, idx, dataOff, 0xFF)
}

// TestCorruptRecordNeverWedgesTheQueue is the headline rule. A damaged record is
// reported once and dropped; the read after it makes progress. A queue that
// answered ErrCorrupt forever would need a human with a text editor to restart.
func TestCorruptRecordNeverWedgesTheQueue(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	for i := uint64(0); i < 4; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Record 0 is 1 length byte + 8 payload + 8 checksum; flip inside its payload.
	flipPayload(t, w, 0, 2)

	if _, ok, err := r.TryTake(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("TryTake over the damaged record: ok=%v err=%v, want ErrCorrupt", ok, err)
	}
	// The very next call must move: this is the difference between reported loss
	// and a dead queue.
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 1 {
		t.Fatalf("the read after the damage: v=%d ok=%v err=%v", v, ok, err)
	}
	st := w.Stats()
	if st.LostRecords != 1 {
		t.Fatalf("LostRecords=%d, want 1", st.LostRecords)
	}
	if st.LostSegments != 0 {
		t.Fatalf("LostSegments=%d: a damaged payload costs one record, not a segment", st.LostSegments)
	}
	if st.LostBytes == 0 {
		t.Fatal("LostBytes did not account for the dropped record")
	}
	if st.Corruptions != 1 {
		t.Fatalf("Corruptions=%d, want 1", st.Corruptions)
	}
}

// TestCorruptRecordIsNeverDelivered is the other half of rule one: the queue may
// drop damaged data, but it may never hand it over as if it were a record.
func TestCorruptRecordIsNeverDelivered(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	const n = 6
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	flipPayload(t, w, 0, 2+17) // inside record 1's payload

	var got []uint64
	for i := 0; i < 100; i++ {
		v, ok, err := r.TryTake()
		if errors.Is(err, ErrCorrupt) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, v)
	}
	for _, v := range got {
		if v == 1 {
			t.Fatal("the damaged record was delivered as a value")
		}
	}
	if len(got) != n-1 {
		t.Fatalf("delivered %v, want every record but the damaged one", got)
	}
}

// TestDrainContinuesPastCorruption: an iterator cannot carry an error, and
// stopping at the first bad record would strand everything behind it. Drain keeps
// going and reports through Err.
func TestDrainContinuesPastCorruption(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	const n = 8
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	flipPayload(t, w, 0, 2+17)

	var got []uint64
	for v := range r.Drain(context.Background()) {
		got = append(got, v)
	}
	if len(got) != n-1 {
		t.Fatalf("drained %v, want the %d undamaged records", got, n-1)
	}
	if !errors.Is(r.Err(), ErrCorrupt) {
		t.Fatalf("Err=%v, want the corruption reported", r.Err())
	}
	if w.Stats().LostRecords != 1 {
		t.Fatalf("LostRecords=%d, want 1", w.Stats().LostRecords)
	}
	// A clean drain afterwards clears Err.
	if err := w.Add(99); err != nil {
		t.Fatal(err)
	}
	for range r.Drain(context.Background()) {
	}
	if r.Err() != nil {
		t.Fatalf("Err=%v after a clean drain, want nil", r.Err())
	}
}

// TestDamagedSegmentDroppedAtOpen: a segment whose header cannot be believed is
// dropped wherever it sits, the open succeeds, the intact segments stay
// reachable, and the loss is reported to the reader exactly once.
func TestDamagedSegmentDroppedAtOpen(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	const n = 700 // several segments at 17 bytes a record
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	segs := w.Stats().Segments
	if segs < 3 {
		t.Fatalf("need several segments, got %d", segs)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Destroy a middle segment's magic — damage no torn header rewrite can
	// produce (the magic is identical in every header image), so it stays a drop
	// rather than a salvage. A failed checksum over an intact magic and version
	// is the torn-rewrite signature and is rebuilt from the records instead;
	// torn_header_test.go pins that path.
	forgeHeader(t, dir, 2, func(h []byte) { h[0] ^= 0xFF })

	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatalf("open with a damaged middle segment: %v, want it dropped", err)
	}
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.LostSegments != 1 {
		t.Fatalf("LostSegments=%d, want 1", st.LostSegments)
	}
	if st.Segments != segs-1 {
		t.Fatalf("Segments=%d, want %d", st.Segments, segs-1)
	}
	if st.LostBytes == 0 {
		t.Fatal("LostBytes did not account for the dropped segment")
	}

	r := w2.NewReader()
	var got []uint64
	for v := range r.Drain(context.Background()) {
		got = append(got, v)
	}
	if !errors.Is(r.Err(), ErrCorrupt) {
		t.Fatalf("Err=%v, want the dropped segment reported to the reader", r.Err())
	}
	if len(got) == 0 {
		t.Fatal("the surviving segments delivered nothing")
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("out of order: %d after %d", got[i], got[i-1])
		}
	}
	if uint64(len(got)) >= n {
		t.Fatalf("delivered %d of %d records: the damaged segment was not dropped", len(got), n)
	}
}

// TestEmptySegmentIsNotALossEvent: a create interrupted between linking the file
// and writing its header cannot have held a record, so removing it must not raise
// a data-loss alarm — an operator watching LostBytes should never be paged for it.
func TestEmptySegmentIsNotALossEvent(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(7); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(dir, "data.00000002")
	f, err := os.Create(stub)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.Corruptions != 0 || st.LostSegments != 0 || st.LostBytes != 0 {
		t.Fatalf("an aborted create was reported as data loss: %+v", st)
	}
	if _, serr := os.Stat(stub); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the stub was not removed: %v", serr)
	}
	v, ok, err := w2.NewReader().TryTake()
	if !ok || err != nil || v != 7 {
		t.Fatalf("record after the stub was dropped: v=%d ok=%v err=%v", v, ok, err)
	}
}

// recoverySegSize is the geometry the aborted-create tests build their residue
// at: small enough to zero a whole file cheaply, large enough that a phantom
// loss report would be unmistakable in LostBytes.
const recoverySegSize = 4096

// openRecoveryQueue opens a queue on dir at recoverySegSize, the same options on
// every open so a reopen recovers rather than rejecting the geometry.
func openRecoveryQueue(t *testing.T, dir string) *Queue[uint64] {
	t.Helper()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: recoverySegSize, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// zeroSegment overwrites segment num's whole file with zeros, in place, keeping
// its length: the shape a segment has when its blocks were reserved and nothing
// was ever written into them.
func zeroSegment(t *testing.T, dir string, num uint64) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("data.%08d", num))
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, fi.Size()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestAbortedPreallocatedCreateIsNotALossEvent: a create interrupted between
// reserving the blocks and writing the first header leaves a full-length file of
// zeros, which is one reservation further along than the zero-length stub above
// and just as empty. Booked as damage it reports a whole segment of loss — a
// default-geometry queue would tell an operator it lost 8 MiB to a power cut that
// cost it nothing — so it has to be reclaimed silently and owe the reader no
// ErrCorrupt.
func TestAbortedPreallocatedCreateIsNotALossEvent(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	if err := w.Add(7); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Exactly what createFile leaves behind when the crash lands between the
	// preallocation and the header write: same open flags, same reservation, no
	// header.
	stub := filepath.Join(dir, "data.00000002")
	f, err := os.OpenFile(stub, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := preallocate(f, headerSize+recoverySegSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if fi, serr := os.Stat(stub); serr != nil || fi.Size() != headerSize+recoverySegSize {
		t.Fatalf("stub is %v (%v), want a full-length reservation: the shape under test", fi, serr)
	}

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.Corruptions != 0 || st.LostSegments != 0 || st.LostBytes != 0 || st.DiscardedBytes != 0 {
		t.Fatalf("an aborted create was reported as data loss: %+v", st)
	}
	if _, serr := os.Stat(stub); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the reservation was not reclaimed: %v", serr)
	}
	// The record comes back on the *first* take: a pending corruption event would
	// be paid out before it, which is how the phantom loss reached the consumer.
	r := w2.NewReader()
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 7 {
		t.Fatalf("first take after the reservation was reclaimed: v=%d ok=%v err=%v", v, ok, err)
	}
	if _, ok, err := r.TryTake(); ok || err != nil {
		t.Fatalf("drained queue: ok=%v err=%v, want a clean empty with nothing owed", ok, err)
	}
}

// TestZeroedHeaderOverRecordsIsStillReported is the other half of the rule above:
// the silence is bought by the data region being zero too, so a segment whose
// header was destroyed while its records are still on disk is real loss and stays
// counted and reported. Getting this wrong is worse than the phantom it fixes —
// an operator would be told nothing at all.
func TestZeroedHeaderOverRecordsIsStillReported(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	for i := uint64(0); i < 8; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// One segment, holding every record, with its header zeroed: the residue of a
	// lost page 0, not of an unfinished create.
	path := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < headerSize; i++ {
		b[i] = 0
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.LostSegments != 1 || st.Corruptions != 1 {
		t.Fatalf("LostSegments=%d Corruptions=%d, want 1 and 1: the records were real", st.LostSegments, st.Corruptions)
	}
	if st.LostBytes == 0 {
		t.Fatal("LostBytes=0: a segment full of records was destroyed")
	}
	if _, ok, err := w2.NewReader().TryTake(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("first take: ok=%v err=%v, want the loss reported once", ok, err)
	}
}

// TestZeroFilledMiddleSegmentIsStillReported pins the position half of the rule.
// Segments are created one at a time at the tail, so only the highest-numbered
// one can be an unfinished create; a zero-filled segment with a live sibling
// above it is unexplained, and unexplained is corruption. Without this the silent
// path would swallow a segment a filesystem repair had zeroed.
func TestZeroFilledMiddleSegmentIsStillReported(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	for i := uint64(0); w.Stats().Segments < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	zeroSegment(t, dir, 2)

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.LostSegments != 1 || st.Corruptions != 1 || st.LostBytes == 0 {
		t.Fatalf("a zeroed middle segment was not reported: %+v", st)
	}
	if _, serr := os.Stat(filepath.Join(dir, "data.00000002")); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("stat of the zeroed segment: %v, want it unlinked", serr)
	}
	r := w2.NewReader()
	if _, ok, err := r.TryTake(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("first take: ok=%v err=%v, want the loss reported once", ok, err)
	}
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("take after the report: ok=%v err=%v, want the surviving records", ok, err)
	}
}

// TestStatsAccounting checks the gauges and the plain counters, so the numbers an
// operator dashboards against are not decoration.
func TestStatsAccounting(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	if st := w.Stats(); st.Added != 0 || st.Segments != 1 || st.MaxSegments != 0 {
		t.Fatalf("fresh queue: %+v", st)
	}
	const n = 10
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	st := w.Stats()
	if st.Added != n {
		t.Fatalf("Added=%d, want %d", st.Added, n)
	}
	if st.Backlog != int64(n) || st.Backlog != int64(w.Count()) {
		t.Fatalf("Backlog=%d, want %d", st.Backlog, n)
	}
	if st.BacklogBytes != w.Size() {
		t.Fatalf("BacklogBytes=%d, want %d", st.BacklogBytes, w.Size())
	}
	if st.DiskBytes < st.BacklogBytes {
		t.Fatalf("DiskBytes=%d below BacklogBytes=%d", st.DiskBytes, st.BacklogBytes)
	}

	for i := 0; i < 4; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	st = w.Stats()
	if st.Delivered != 4 {
		t.Fatalf("Delivered=%d, want 4", st.Delivered)
	}
	if st.Committed != 4 {
		t.Fatalf("Committed=%d, want 4", st.Committed)
	}
	if st.Backlog != n-4 {
		t.Fatalf("Backlog=%d, want %d", st.Backlog, n-4)
	}

	// Stats stays readable after Close and reports the final state.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := w.Stats().Added; got != n {
		t.Fatalf("Added=%d after Close, want %d", got, n)
	}
}

// TestStatsFullCounter: refused appends are counted, so "the producer is being
// throttled by the cap" is visible without instrumenting the caller.
func TestStatsFullCounter(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	var refused int
	for i := 0; i < 100000; i++ {
		if err := w.Add(uint64(i)); err != nil {
			if !errors.Is(err, ErrFull) {
				t.Fatal(err)
			}
			refused++
			if refused == 3 {
				break
			}
		}
	}
	if refused != 3 {
		t.Fatalf("never hit ErrFull")
	}
	if got := w.Stats().Full; got != 3 {
		t.Fatalf("Full=%d, want 3", got)
	}
}

// TestRewindReplaysUncommitted: Reserve/Commit is an acknowledgement protocol,
// and Rewind is its nack. Without it, a consumer that reserved records and then
// could not process them left the read cursor ahead of the commit cursor with no
// way back — Empty reported true and the records were unreachable until the
// process restarted, though they were still on disk and still uncommitted.
func TestRewindReplaysUncommitted(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	const n = 5
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Reserve everything without committing: the classic stuck-consumer state.
	for i := uint64(0); i < n; i++ {
		v, ok, _, err := r.TryReserve()
		if !ok || err != nil || v != i {
			t.Fatalf("reserve %d: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
	if !w.Empty() {
		t.Fatal("the queue should read empty with everything reserved")
	}
	if got := w.Count(); got != n {
		t.Fatalf("Count=%d, want %d: nothing was committed", got, n)
	}

	got, err := w.Rewind()
	if err != nil {
		t.Fatal(err)
	}
	if got != w.Size() {
		t.Fatalf("Rewind reported %d bytes, want Size()=%d", got, w.Size())
	}
	if w.Empty() {
		t.Fatal("the queue should be readable again after Rewind")
	}

	// Everything comes back, in order, exactly once.
	for i := uint64(0); i < n; i++ {
		v, ok, err := r.TryTake()
		if !ok || err != nil || v != i {
			t.Fatalf("replay %d: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count=%d after committing the replay, want 0", got)
	}
	// The redelivery is counted as a delivery, which is what "redeliveries
	// included" means; the records were genuinely handed over twice.
	if st := w.Stats(); st.Delivered != 2*n {
		t.Fatalf("Delivered=%d, want %d", st.Delivered, 2*n)
	}
}

// TestRewindCannotUncommit: Rewind moves the read cursor, never the commit
// cursor. Records already acknowledged stay acknowledged.
func TestRewindCannotUncommit(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	for i := uint64(0); i < 4; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Take (commit) two, reserve the rest.
	for i := 0; i < 2; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	if _, ok, _, err := r.TryReserve(); !ok || err != nil {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}

	if _, err := w.Rewind(); err != nil {
		t.Fatal(err)
	}
	// The two committed records must NOT come back.
	v, ok, err := r.TryTake()
	if !ok || err != nil || v != 2 {
		t.Fatalf("after Rewind: v=%d ok=%v err=%v, want the first uncommitted record", v, ok, err)
	}
}
