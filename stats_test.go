package diskqueue

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// errDecode stands in for a codec that rejects a record.
var errDecode = errors.New("decode failed")

// Stats splits into gauges, which describe the queue right now, and counters,
// which only ever climb. Mixing the two up is the easy mistake: a "counter" that
// falls makes rate() go negative and a dashboard lie in the direction that hides
// work being done.

// TestStatsCountersAreMonotone: Committed was nCommitted, which is a gauge —
// paired with nWritten to compute Count() and decremented whenever a
// fully-committed segment is reclaimed. It fell as the queue did its job.
func TestStatsCountersAreMonotone(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()

	const n = 1000 // enough to fill and reclaim many 4 KiB segments
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	var prev Stats
	for i := 0; i < n; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
		st := w.Stats()
		if st.Added < prev.Added || st.Delivered < prev.Delivered ||
			st.Committed < prev.Committed || st.Full < prev.Full {
			t.Fatalf("a counter went backwards at take %d: %+v then %+v", i, prev, st)
		}
		prev = st
	}
	st := w.Stats()
	if st.Added != n {
		t.Errorf("Added=%d, want %d", st.Added, n)
	}
	if st.Delivered != n {
		t.Errorf("Delivered=%d, want %d", st.Delivered, n)
	}
	if st.Committed != n {
		t.Errorf("Committed=%d, want %d — reclamation must not un-count a commit", st.Committed, n)
	}
	// The gauges, by contrast, are back to empty.
	if st.Backlog != 0 || st.BacklogBytes != 0 {
		t.Errorf("gauges not drained: Backlog=%d BacklogBytes=%d", st.Backlog, st.BacklogBytes)
	}
}

// TestDeliveredExcludesUndeliveredRecords: takeHead counts a record on its way
// out, but a decode failure rewinds the cursor and the consumer never saw it.
// Counting it anyway made Delivered climb on a queue that was handing out nothing.
func TestDeliveredExcludesUndeliveredRecords(t *testing.T) {
	failing := true
	w, err := New[uint64](t.TempDir(), marshalU64, func(b []byte) (uint64, error) {
		if failing {
			return 0, errDecode
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

	for i := 0; i < 5; i++ {
		if _, ok, err := r.TryTake(); ok || err == nil {
			t.Fatalf("attempt %d: ok=%v err=%v, want the decode error", i, ok, err)
		}
	}
	if got := w.Stats().Delivered; got != 0 {
		t.Fatalf("Delivered=%d after 5 failed decodes, want 0", got)
	}
	failing = false
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("take after the codec recovered: ok=%v err=%v", ok, err)
	}
	if got := w.Stats().Delivered; got != 1 {
		t.Fatalf("Delivered=%d, want 1", got)
	}
}

// TestTruncatedSegmentCountIsHonest: a cut file keeps a header that counts
// records the file no longer holds. Count() then promised a backlog no drain
// could deliver, and the queue reported non-empty forever.
func TestTruncatedSegmentCountIsHonest(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// Cut most of the segment away; the header still advertises all 20 records.
	path := filepath.Join(dir, "data.00000001")
	if err := os.Truncate(path, headerSize+3*idxRecLen); err != nil {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got > 3 {
		t.Fatalf("count=%d after truncation to 3 records' worth: the header was believed over the bytes", got)
	}
	delivered, _ := drainRecovering(t, s2)
	if int64(len(delivered)) > s2.count()+int64(len(delivered)) {
		t.Fatal("impossible")
	}
	if got := s2.count(); got != 0 {
		t.Fatalf("count=%d after a full drain, want 0", got)
	}
}

// TestTruncatedSegmentSurvivesReopen: load clamps a truncated segment's size but
// then re-preallocates the file back to full length. Unless the clamped write
// cursor is republished first, the next open finds a full-size file whose header
// points past the surviving records and reads the zero fill back as a phantom
// backlog — re-reporting the same loss on every open, forever.
func TestTruncatedSegmentSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "data.00000001"), headerSize+3*idxRecLen); err != nil {
		t.Fatal(err)
	}

	first, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstCorruptions := first.corruptionCount()
	got1, _ := drainRecovering(t, first)
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	if firstCorruptions == 0 {
		t.Fatal("the truncation was not reported on the first open")
	}

	// The second open must be quiet: the loss was already reported and accounted.
	second, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.close() }()
	if got := second.corruptionCount(); got != 0 {
		t.Fatalf("corruptions=%d on the second open: the same loss is being re-reported", got)
	}
	if got := second.count(); got != 0 {
		t.Fatalf("count=%d on the second open, want 0 (everything was drained): "+
			"the zero fill is being read back as records", got)
	}
	got2, _ := drainRecovering(t, second)
	if len(got2) != 0 {
		t.Fatalf("second open delivered %v after the first drained %v", got2, got1)
	}
}

// TestTruncatedSegmentBacklogIsExact: the count of records a truncated segment
// still holds has to come from the bytes, not from arithmetic. A bound of
// "size / smallest possible frame" is only tight when the records are minimal;
// for anything larger it sits above the header's own count and never fires, so
// Count() kept promising a backlog no drain could deliver and the queue never
// read empty again.
func TestTruncatedSegmentBacklogIsExact(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	const n, recLen = 10, 17 // 1 uvarint + 8 payload + 8 checksum
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	const kept = 6
	if err := os.Truncate(filepath.Join(dir, "data.00000001"), headerSize+recLen*kept); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w2.NewReader()
	var got []uint64
	for i := 0; i < 4*n; i++ {
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
	if len(got) != kept {
		t.Fatalf("delivered %v, want the %d records that survived the cut", got, kept)
	}
	if c := w2.Count(); c != 0 {
		t.Fatalf("Count=%d after a full drain, want 0: phantom backlog", c)
	}
	if !w2.Empty() {
		t.Fatal("the queue should read empty")
	}
	st := w2.Stats()
	if st.LostRecords != n-kept {
		t.Fatalf("LostRecords=%d, want %d", st.LostRecords, n-kept)
	}
	if st.DiscardedBytes == 0 {
		t.Fatal("the cut tail was not counted")
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	// And a third open is quiet: the clamped cursor was republished, so nothing
	// re-reports the same loss and the zero fill is not read back as records.
	w3, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w3.Close() }()
	if st := w3.Stats(); st.Corruptions != 0 || st.Backlog != 0 {
		t.Fatalf("third open: Corruptions=%d Backlog=%d, want 0 and 0", st.Corruptions, st.Backlog)
	}
}

// TestCommitQuarantineDoesNotOvercount: reached from the commit path, the read
// cursor is often already past the segment's end — every record in it was
// delivered. Booking a lost segment for zero lost bytes tells an operator data
// went missing when none did.
func TestCommitQuarantineDoesNotOvercount(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	for i := 0; s.active().num < 2; i++ {
		mustAppend(t, s, genPayload(400, byte(i)))
	}
	// Read past the first segment without committing.
	for s.headOff < s.files[0].base+s.files[0].size {
		if _, _, ok, err := s.takeHead(); !ok || err != nil {
			t.Fatalf("read: ok=%v err=%v", ok, err)
		}
	}
	// Now rot the framing behind the read cursor and commit across it.
	corruptData(t, s, 0, 400, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	lostSegs, lostB := s.lostSegments, s.lostBytes

	if err := s.commitTo(s.headOff); err != nil {
		t.Fatalf("commitTo: %v", err)
	}
	if s.lostBytes == lostB && s.lostSegments != lostSegs {
		t.Fatalf("lostSegments went %d->%d with no bytes lost: every record there was delivered",
			lostSegs, s.lostSegments)
	}
	if s.pendingCorrupt == 0 && s.corruptions > 0 {
		t.Fatal("a counted event with no read to report it")
	}
}

// TestOversizedAddReleasesScratch: the marshal buffer is the one buffer not
// bounded by the segment geometry — it retains whatever MarshalFunc produced,
// before anyone asks whether the record was storable at all. One rejected
// oversized Add would otherwise pin that capacity for the life of the queue.
func TestOversizedAddReleasesScratch(t *testing.T) {
	seg := int64(4096)
	m := func(dst []byte, v int) ([]byte, error) { return append(dst, make([]byte, v)...), nil }
	u := func(d []byte) (int, error) { return len(d), nil }
	w, err := New[int](t.TempDir(), m, u, Options{NoSync: true, SegmentSize: seg, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Add(8 << 20); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized Add: %v, want ErrRecordTooLarge", err)
	}
	if got := int64(cap(w.scratch)); got > seg {
		t.Fatalf("scratch pinned at %d bytes on a queue whose records cap at %d", got, seg)
	}
	// The queue still works, and the buffer regrows to a sane size.
	if err := w.Add(64); err != nil {
		t.Fatal(err)
	}
	if got := int64(cap(w.scratch)); got > seg {
		t.Fatalf("scratch=%d after a normal Add", got)
	}
}
