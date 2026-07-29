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
