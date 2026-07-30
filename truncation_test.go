package diskqueue

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The truncation contract, pinned end to end — including the two ways it used to
// go wrong. A cut segment's cost is decided at open (counted once, owed once),
// the published extent ends at the last WHOLE frame, and a store that lost bytes
// keeps working for everything written after the recovery. Every test here
// models damage the way a power loss leaves it: on disk, between a close and a
// reopen.

func truncSegPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%08d", filePrefix, num))
}

func truncRec(id byte, n int) []byte {
	p := make([]byte, n)
	p[0] = id
	return p
}

// truncDrain drains q, separating delivered record ids from corruption reports.
// Any error that is not ErrCorrupt fails the test: recovery may report loss, but
// it may not surface anything else on this path.
func truncDrain(t *testing.T, q *Queue[[]byte]) (ids []byte, reports int) {
	t.Helper()
	r := q.NewReader()
	for i := 0; i < 200; i++ {
		v, ok, err := r.TryTake()
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("drain: %v, want only ErrCorrupt reports", err)
			}
			reports++
			continue
		}
		if !ok {
			return ids, reports
		}
		ids = append(ids, v[0])
	}
	t.Fatal("drain did not terminate")
	return nil, 0
}

// TestTruncatedActiveAcceptsNewRecords is the regression test for the worst
// finding of the truncation audit: records accepted AFTER a clean recovery were
// destroyed by the recovery itself.
//
// The repair used to keep the cut record's partial frame inside the published
// extent and republish the write cursor beyond it. The next Add landed after the
// garbage, and the read walk then tripped over the stale partial frame,
// mis-framed everything behind it, and ground the fresh records into ~dozens of
// bogus corruption events (19 events and 18 "lost records" for five records and
// one physical fault, measured). Fresh records — fsynced, acknowledged — were
// never delivered.
//
// The contract now: the extent is clamped to the last whole frame, the partial
// bytes are counted in DiscardedBytes alongside the cut tail, and an append
// after the repair overwrites the garbage. One fault, one report, and every
// record that was ever acknowledged and not cut is delivered.
func TestTruncatedActiveAcceptsNewRecords(t *testing.T) {
	dir := t.TempDir()
	m, u := impCodec()
	q, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	for id := byte(0); id < 3; id++ { // 300 B payload => 310 B frame
		if err := q.Add(truncRec(id, 300)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	// Cut 150 bytes into the third record's frame: the tail past the cut is gone,
	// and 150 bytes of partial frame survive in front of it.
	if err := os.Truncate(truncSegPath(dir, 1), headerSize+2*310+150); err != nil {
		t.Fatal(err)
	}

	q2, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()
	st := q2.Stats()
	if q2.Count() != 2 || st.LostRecords != 1 || st.Corruptions != 1 {
		t.Fatalf("after reopen: count=%d lostRecords=%d corruptions=%d, want 2, 1, 1",
			q2.Count(), st.LostRecords, st.Corruptions)
	}
	// Everything past the last whole frame is discarded: the 160-byte cut tail
	// AND the 150 partial bytes in front of it — exactly the third record's frame.
	if st.DiscardedBytes != 310 {
		t.Fatalf("DiscardedBytes=%d, want 310: the partial frame must be discarded with the tail",
			st.DiscardedBytes)
	}

	// Fresh records deliberately a DIFFERENT size from the cut one: the repair's
	// walk warms the block cache with the pre-repair bytes, and a stale cached
	// length equal to the new record's would mask a cache that was never
	// invalidated. (It did, in this test's first version.)
	for id := byte(10); id < 12; id++ {
		if err := q2.Add(truncRec(id, 50)); err != nil {
			t.Fatalf("add after repair: %v", err)
		}
	}
	ids, reports := truncDrain(t, q2)
	if !bytes.Equal(ids, []byte{0, 1, 10, 11}) {
		t.Fatalf("delivered %v, want [0 1 10 11]: a record accepted after recovery must be delivered", ids)
	}
	if reports != 1 {
		t.Fatalf("%d corruption reports for one physical fault, want exactly the one owed", reports)
	}
	st = q2.Stats()
	if st.Corruptions != 1 || st.LostRecords != 1 || st.LostBytes != 0 || st.LostSegments != 0 {
		t.Fatalf("final stats corruptions=%d lostRecords=%d lostBytes=%d lostSegments=%d, "+
			"want 1, 1, 0, 0 — no phantom losses from the stale partial frame",
			st.Corruptions, st.LostRecords, st.LostBytes, st.LostSegments)
	}
	if q2.Count() != 0 || !q2.Empty() {
		t.Fatalf("count=%d empty=%v after draining", q2.Count(), q2.Empty())
	}
}

// truncOversizedStore leaves a store holding exactly one oversized record (a
// 20000-byte payload in a 4096-byte geometry: framed 3+20000+8 = 20011) and
// returns its segment number.
func truncOversizedStore(t *testing.T, dir string) uint64 {
	t.Helper()
	m, u := impCodec()
	q, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, MaxSegments: -1, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Add(truncRec(1, 20000)); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	ms, err := filepath.Glob(filepath.Join(dir, filePrefix+"*"))
	if err != nil || len(ms) != 1 {
		t.Fatalf("segments on disk: %v (err %v), want exactly the oversized one", ms, err)
	}
	var num uint64
	if _, err := fmt.Sscanf(filepath.Base(ms[0]), filePrefix+"%d", &num); err != nil {
		t.Fatal(err)
	}
	return num
}

// truncOversizedReopen reopens the store after damage and asserts the shared
// outcome of both oversized-truncation tests: the loss is counted AT OPEN and
// owed as exactly one report, the queue drains empty, and a record added after
// the repair round-trips — the segment came back usable.
func truncOversizedReopen(t *testing.T, dir string, wantDiscarded uint64) {
	t.Helper()
	m, u := impCodec()
	q, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, MaxSegments: -1, NoSync: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = q.Close() }()
	st := q.Stats()
	// The whole point: the truncation is visible at open — not silent, and not
	// deferred to whenever a reader happens to trip over it.
	if st.Corruptions != 1 || st.LostRecords != 1 || st.DiscardedBytes != wantDiscarded {
		t.Fatalf("at open: corruptions=%d lostRecords=%d discarded=%d, want 1, 1, %d",
			st.Corruptions, st.LostRecords, st.DiscardedBytes, wantDiscarded)
	}
	if q.Count() != 0 {
		t.Fatalf("count=%d, want 0: the header's record no longer exists", q.Count())
	}
	ids, reports := truncDrain(t, q)
	if len(ids) != 0 || reports != 1 {
		t.Fatalf("drain: ids=%v reports=%d, want none and exactly the one owed report", ids, reports)
	}
	if err := q.Add(truncRec(9, 100)); err != nil {
		t.Fatalf("add after repair: %v", err)
	}
	ids, reports = truncDrain(t, q)
	if !bytes.Equal(ids, []byte{9}) || reports != 0 {
		t.Fatalf("post-repair record: ids=%v reports=%d, want [9] and no new reports", ids, reports)
	}
}

// TestOversizedTruncatedMidIsCountedAtOpen: an oversized segment that lost 500
// bytes used to open as perfectly healthy — Count promised the record, Stats
// showed no damage, and the loss surfaced only when a reader tripped over it,
// classified as a lost segment with the 500 cut bytes counted nowhere.
func TestOversizedTruncatedMidIsCountedAtOpen(t *testing.T) {
	dir := t.TempDir()
	num := truncOversizedStore(t, dir)
	fi, err := os.Stat(truncSegPath(dir, num))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(truncSegPath(dir, num), fi.Size()-500); err != nil {
		t.Fatal(err)
	}
	// 500 cut bytes + 19511 partial-frame bytes = the whole 20011-byte frame.
	truncOversizedReopen(t, dir, 20011)
}

// TestOversizedTruncatedToHeaderIsObservable: cut to exactly the 64-byte header,
// the record used to vanish with NO event at all — count 0, stats clean, nothing
// owed. "Every loss path is observable" is the rule; this was the exception.
func TestOversizedTruncatedToHeaderIsObservable(t *testing.T) {
	dir := t.TempDir()
	num := truncOversizedStore(t, dir)
	if err := os.Truncate(truncSegPath(dir, num), headerSize); err != nil {
		t.Fatal(err)
	}
	truncOversizedReopen(t, dir, 20011)
}

// TestAllOversizedStoreWitnessesGeometry: every segment now states the
// SegmentSize its store was created under, so even a store whose only surviving
// segment is oversized — whose file length says nothing about the configured
// geometry — refuses to open under a different one.
func TestAllOversizedStoreWitnessesGeometry(t *testing.T) {
	dir := t.TempDir()
	truncOversizedStore(t, dir)
	m, u := impCodec()
	if q, err := New[[]byte](dir, m, u, Options{SegmentSize: 8192, MaxSegments: -1}); !errors.Is(err, ErrSegmentSizeMismatch) {
		if err == nil {
			_ = q.Close()
		}
		t.Fatalf("reopen at 8192 (created at 4096): %v, want ErrSegmentSizeMismatch", err)
	}
	// And the witness does not false-positive: the right geometry still opens,
	// with the record intact.
	q, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, MaxSegments: -1, NoSync: true})
	if err != nil {
		t.Fatalf("reopen at the created geometry: %v", err)
	}
	defer func() { _ = q.Close() }()
	v, ok, err := q.NewReader().TryTake()
	if !ok || err != nil || len(v) != 20000 || v[0] != 1 {
		t.Fatalf("record after reopen: len=%d ok=%v err=%v", len(v), ok, err)
	}
}

// TestOversizedBuffersReleased: one oversized record must not stay resident in
// the store and reader buffers for the queue's lifetime. The marshal scratch was
// already released; the frame buffer, the read block and the Reader's copy were
// not — three pins of up to a record each, invisible until a heap profile.
func TestOversizedBuffersReleased(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, make([]byte, 1<<20))
	if got := cap(s.writeBuf); got != 0 {
		t.Fatalf("cap(writeBuf)=%d after an oversized append, want 0 (released)", got)
	}
	if _, _, ok, err := s.takeHead(); !ok || err != nil {
		t.Fatalf("takeHead: ok=%v err=%v", ok, err)
	}
	if got := cap(s.readBuf); got < 1<<20 {
		t.Fatalf("cap(readBuf)=%d while the record is being served, want >= the record", got)
	}
	if err := s.commitTo(s.writeOffset()); err != nil {
		t.Fatal(err)
	}
	// The commit retires the record; the next cycle reclaims its segment, and
	// dropping the block is where the grown buffer goes with it.
	mustAppend(t, s, make([]byte, 100))
	if got := cap(s.readBuf); got > 4096 {
		t.Fatalf("cap(readBuf)=%d after the oversized segment was reclaimed, want the block released", got)
	}

	// The Reader's private copy follows the same rule, one layer up.
	m, u := impCodec()
	q, err := New[[]byte](t.TempDir(), m, u, Options{SegmentSize: 4096, MaxSegments: -1, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if err := q.Add(truncRec(1, 20000)); err != nil {
		t.Fatal(err)
	}
	r := q.NewReader()
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("take: ok=%v err=%v", ok, err)
	}
	if got := cap(r.scratch); got != 0 {
		t.Fatalf("cap(Reader.scratch)=%d after delivering an oversized record, want 0 (released)", got)
	}
}
