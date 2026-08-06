package diskqueue

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A torn in-place header rewrite — a power cut mid-commit — leaves a header
// whose magic and version are intact (neither changes between images) but whose
// checksum fails. That used to drop the whole segment, records and all: the one
// way a routine, successful operation plus a power cut could destroy durable,
// acknowledged data. These tests pin the salvage that replaced it: the records
// vouch for themselves through their own checksums, so the header is rebuilt
// from a verified walk, nothing verified is lost, and the price is a replay
// (the commit position was the only witness of what was consumed) plus exactly
// one reported corruption event.

// tearHeader flips one checksum byte of segment num's header in place, leaving
// magic and version untouched — the torn-rewrite signature.
func tearHeader(t *testing.T, dir string, num uint64) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("data.%08d", num))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[56] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// drainQueue reads the queue dry the way a consumer is meant to, counting the
// corruption events it collects along the way.
func drainQueue(t *testing.T, r *Reader[uint64]) (got []uint64, events int) {
	t.Helper()
	for i := 0; ; i++ {
		if i > 100000 {
			t.Fatal("drain made no progress: the queue is wedged")
		}
		v, ok, err := r.TryTake()
		switch {
		case errors.Is(err, ErrCorrupt):
			events++
		case err != nil:
			t.Fatal(err)
		case !ok:
			return got, events
		default:
			got = append(got, v)
		}
	}
}

// TestTornHeaderSalvagesRecords tears a middle segment's header and verifies
// the open rebuilds it instead of unlinking it: every record in the store is
// still delivered, in order, and the only cost is one reported event.
func TestTornHeaderSalvagesRecords(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	total := uint64(0)
	for w.Stats().Segments < 3 {
		if err := w.Add(total); err != nil {
			t.Fatal(err)
		}
		total++
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 2)

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	st := w2.Stats()
	if st.Corruptions != 1 {
		t.Fatalf("Corruptions=%d, want 1: a torn header is an event", st.Corruptions)
	}
	if st.LostSegments != 0 || st.LostRecords != 0 || st.LostBytes != 0 {
		t.Fatalf("LostSegments=%d LostRecords=%d LostBytes=%d, want all 0: every record survived",
			st.LostSegments, st.LostRecords, st.LostBytes)
	}
	if got := w2.Count(); got != int(total) {
		t.Fatalf("Count=%d, want %d: the salvaged records are part of the backlog", got, total)
	}
	got, events := drainQueue(t, w2.NewReader())
	if events != 1 {
		t.Fatalf("%d corruption events reported, want 1", events)
	}
	if len(got) != int(total) {
		t.Fatalf("drained %d records, want %d: salvage must not lose any", len(got), total)
	}
	for i, v := range got {
		if v != uint64(i) {
			t.Fatalf("record %d = %d: order or identity lost in salvage", i, v)
		}
	}
}

// TestTornHeaderReplaysCommitted: the commit position is the one thing a torn
// header takes with it, so records consumed before the tear are offered again —
// a replay, which at-least-once permits, rather than the loss this used to be.
func TestTornHeaderReplaysCommitted(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if v, ok, err := w.NewReader().TryTake(); !ok || err != nil || v != 0 {
		t.Fatalf("TryTake: v=%d ok=%v err=%v", v, ok, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 1)

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	got, events := drainQueue(t, w2.NewReader())
	if events != 1 {
		t.Fatalf("%d corruption events reported, want 1", events)
	}
	if len(got) != 3 || got[0] != 0 {
		t.Fatalf("drained %v, want the committed record replayed ahead of [1 2]", got)
	}
}

// TestTornHeaderRepairIsDurable: the rebuilt header is republished, so the next
// open finds a header it can believe — no fresh event, no re-salvage, and the
// backlog intact.
func TestTornHeaderRepairIsDurable(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	const n = 5
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 1)

	// First open salvages; close without draining (Close flushes the repair).
	w2 := openRecoveryQueue(t, dir)
	if st := w2.Stats(); st.Corruptions != 1 {
		t.Fatalf("first reopen: Corruptions=%d, want 1", st.Corruptions)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	w3 := openRecoveryQueue(t, dir)
	defer func() { _ = w3.Close() }()
	if st := w3.Stats(); st.Corruptions != 0 {
		t.Fatalf("second reopen: Corruptions=%d, want 0: the repair did not stick", st.Corruptions)
	}
	got, events := drainQueue(t, w3.NewReader())
	if events != 0 || len(got) != n {
		t.Fatalf("second reopen drained %d records with %d events, want %d and 0", len(got), events, n)
	}
}

// TestTornHeaderWalkStopsAtDamagedRecord: with no believable header, only a
// frame whose payload verifies extends the walk. A record damaged mid-segment
// ends the salvage there — the records behind it are cut with it, because
// nothing unverified may ever be delivered — and the store still owes exactly
// one report.
func TestTornHeaderWalkStopsAtDamagedRecord(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	for i := uint64(0); i < 5; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Each record is 1 length byte + 8 payload + 8 checksum. Flip a payload byte
	// of record 1 (data offset 17+1), then tear the header.
	path := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[headerSize+18] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 1)

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	if got := w2.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: only the verified prefix survives a torn header", got)
	}
	got, events := drainQueue(t, w2.NewReader())
	if events != 1 {
		t.Fatalf("%d corruption events reported, want 1", events)
	}
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("drained %v, want exactly the verified record 0", got)
	}
}

// TestTornHeaderOversizedSegmentSalvages: the geometry witness dies with the
// header, so the capacity is rebuilt from the file's own length — an oversized
// segment comes back oversized (and re-flagged), record intact, and the store
// still opens under its configured SegmentSize.
func TestTornHeaderOversizedSegmentSalvages(t *testing.T) {
	dir := t.TempDir()
	m := func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil }
	u := func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
	w, err := New[[]byte](dir, m, u, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte{0xAB}, 8000) // larger than the segment geometry
	if err := w.Add(big); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 2) // the oversized segment; the empty first was reclaimed

	w2, err := New[[]byte](dir, m, u, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatalf("reopen with a torn oversized header: %v, want a salvage", err)
	}
	defer func() { _ = w2.Close() }()
	if st := w2.Stats(); st.Corruptions != 1 || st.LostRecords != 0 {
		t.Fatalf("Corruptions=%d LostRecords=%d, want 1 and 0", st.Corruptions, st.LostRecords)
	}
	if af := w2.st.active(); !af.oversized() || af.capacity <= 4096 {
		t.Fatalf("oversized=%v capacity=%d: the rebuilt header lost the segment's geometry",
			af.oversized(), af.capacity)
	}
	r := w2.NewReader()
	if _, ok, err := r.TryTake(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("owed report: ok=%v err=%v, want ErrCorrupt first", ok, err)
	}
	v, ok, err := r.TryTake()
	if err != nil || !ok || !bytes.Equal(v, big) {
		t.Fatalf("oversized record after salvage: ok=%v err=%v len=%d, want the original %d bytes",
			ok, err, len(v), len(big))
	}
}

// TestDuplicateSegmentNumberLoadsOnce: "data.1" beside "data.00000001" parses
// to the same segment number, and filePath resolves both to the padded form —
// so without deduplication the same segment loaded twice, double-counting its
// records. The stray is inert and stays.
func TestDuplicateSegmentNumberLoadsOnce(t *testing.T) {
	dir := t.TempDir()
	w := openRecoveryQueue(t, dir)
	const n = 4
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "data.00000001"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.1"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	w2 := openRecoveryQueue(t, dir)
	defer func() { _ = w2.Close() }()
	if got := w2.Count(); got != n {
		t.Fatalf("Count=%d, want %d: the duplicate number was loaded twice", got, n)
	}
	got, events := drainQueue(t, w2.NewReader())
	if events != 0 || len(got) != n {
		t.Fatalf("drained %d records with %d events, want %d and 0", len(got), events, n)
	}
	for i, v := range got {
		if v != uint64(i) {
			t.Fatalf("record %d = %d: delivered out of order or twice", i, v)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "data.1")); err != nil {
		t.Fatalf("the unpadded stray should be left alone: %v", err)
	}
}
