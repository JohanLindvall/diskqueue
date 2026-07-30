package diskqueue

import (
	"bytes"
	"errors"
	"testing"
)

// Tests for the behaviours ported from the sibling project spool: an exposure
// gauge, a byte cap, oversized records, and requeueing a poison record.

func impCodec() (MarshalFunc[[]byte], UnmarshalFunc[[]byte]) {
	return func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil },
		func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
}

// TestUnsyncedBytesTracksExposure: the deferred sync policies exist to escape one
// fsync per record, and the price is a window where accepted records are only in
// the page cache. Without a gauge for it an operator cannot tell a queue that is
// keeping up from one whose SyncInterval backstop has stalled — the backlog looks
// identical either way.
func TestUnsyncedBytesTracksExposure(t *testing.T) {
	// Per-op: Add fsyncs before returning, so the exposure is always zero.
	m0, u0 := impCodec()
	q, err := New[[]byte](t.TempDir(), m0, u0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	for i := 0; i < 5; i++ {
		if err := q.Add([]byte("durable")); err != nil {
			t.Fatal(err)
		}
	}
	if got := q.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d under the per-op policy, want 0: Add fsyncs before it returns", got)
	}

	// NoSync: every record accumulates, and Sync brings it back to zero.
	m, u := impCodec()
	q2, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()
	if got := q2.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d on a fresh queue, want 0", got)
	}
	for i := 0; i < 5; i++ {
		if err := q2.Add([]byte("exposed")); err != nil {
			t.Fatal(err)
		}
	}
	exposed := q2.Stats().UnsyncedBytes
	if exposed <= 0 {
		t.Fatal("UnsyncedBytes=0 under NoSync after 5 adds: the exposure is not being reported")
	}
	if want := q2.Size(); exposed != want {
		t.Fatalf("UnsyncedBytes=%d, want %d — every appended byte is unsynced here", exposed, want)
	}
	if err := q2.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := q2.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d after Sync, want 0", got)
	}

	// Batched: it climbs within a batch and is cleared by the flush that ends one.
	m3, u3 := impCodec()
	q3, err := New[[]byte](t.TempDir(), m3, u3, Options{SyncEvery: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q3.Close() }()
	if err := q3.Add([]byte("batched")); err != nil {
		t.Fatal(err)
	}
	if got := q3.Stats().UnsyncedBytes; got <= 0 {
		t.Fatalf("UnsyncedBytes=%d mid-batch, want > 0", got)
	}
	for i := 0; i < 8; i++ { // cross the batch boundary
		if err := q3.Add([]byte("batched")); err != nil {
			t.Fatal(err)
		}
	}
	if got := q3.Stats().UnsyncedBytes; got >= exposed*2 {
		t.Fatalf("UnsyncedBytes=%d after crossing a batch boundary: the flush did not clear it", got)
	}
}

// TestUnsyncedBytesIgnoresRejectedAdd: an Add that failed published nothing, so it
// must not add to the exposure — the bytes it wrote are unreferenced and a reopen
// cannot see them.
func TestUnsyncedBytesIgnoresRejectedAdd(t *testing.T) {
	m, u := impCodec()
	q, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true, SegmentSize: 4096, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	before := q.Stats().UnsyncedBytes
	if err := q.Add(make([]byte, 8192)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Add of a record past the byte cap: %v, want ErrRecordTooLarge", err)
	}
	if got := q.Stats().UnsyncedBytes; got != before {
		t.Fatalf("UnsyncedBytes %d -> %d after a rejected Add", before, got)
	}
}

// TestMaxBytesCapsBacklog: the segment cap bounds the file count; this bounds the
// thing operators actually budget. Both apply, whichever binds first.
func TestMaxBytesCapsBacklog(t *testing.T) {
	m, u := impCodec()
	const cap0 = 4096
	q, err := New[[]byte](t.TempDir(), m, u,
		Options{NoSync: true, SegmentSize: 8192, MaxSegments: -1, MaxBytes: cap0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if got := q.Stats().MaxBytes; got != cap0 {
		t.Fatalf("Stats().MaxBytes=%d, want %d", got, cap0)
	}

	rec := make([]byte, 512)
	n := 0
	for {
		err := q.Add(rec)
		if errors.Is(err, ErrFull) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
		if n > 100 {
			t.Fatal("the byte cap never bound: Add kept succeeding past it")
		}
	}
	if q.Size() > cap0 {
		t.Fatalf("backlog %d exceeded the cap %d", q.Size(), cap0)
	}
	if q.Stats().Full == 0 {
		t.Fatal("a refusal was not counted in Stats().Full")
	}
	// Refused, not partially applied: the queue is exactly as it was.
	before := q.Size()
	if err := q.Add(rec); !errors.Is(err, ErrFull) {
		t.Fatalf("Add past the cap: %v, want ErrFull", err)
	}
	if q.Size() != before {
		t.Fatalf("a refused Add changed the backlog: %d -> %d", before, q.Size())
	}

	// Transient, not permanent: draining clears it.
	r := q.NewReader()
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("take: ok=%v err=%v", ok, err)
	}
	if err := q.Add(rec); err != nil {
		t.Fatalf("Add after draining one record: %v, want it to fit again", err)
	}

	// A record that cannot fit the cap on an empty queue is permanent, and says so
	// with a different error — retrying ErrFull forever would never succeed.
	m2, u2 := impCodec()
	q2, err := New[[]byte](t.TempDir(), m2, u2, Options{NoSync: true, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q2.Close() }()
	if err := q2.Add(make([]byte, 4096)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Add of a record bigger than the whole cap: %v, want ErrRecordTooLarge", err)
	}
}

// TestRequeueMovesPoisonRecordToTheBack: without this, a record the consumer
// cannot process leaves only two options — discard it with Skip, or let it block
// the head forever. Requeue is the third.
func TestRequeueMovesPoisonRecordToTheBack(t *testing.T) {
	m, u := impCodec()
	q, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	for _, s := range []string{"poison", "b", "c"} {
		if err := q.Add([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	before := q.Count()

	r := q.NewReader()
	ok, err := r.Requeue()
	if err != nil || !ok {
		t.Fatalf("Requeue: ok=%v err=%v", ok, err)
	}
	if got := q.Count(); got != before {
		t.Fatalf("Count=%d after Requeue, want %d: the record was lost, not moved", got, before)
	}

	// The rest drains first, and the poison record comes back last — intact.
	var order []string
	for i := 0; i < 3; i++ {
		v, ok, err := r.TryTake()
		if !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
		order = append(order, string(v))
	}
	want := []string{"b", "c", "poison"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
	if !q.Empty() {
		t.Fatal("queue not drained")
	}
	if ok, err := r.Requeue(); ok || err != nil {
		t.Fatalf("Requeue on an empty queue: ok=%v err=%v, want false and nil", ok, err)
	}
}

// TestRequeueFailedAppendLeavesRecordAtHead: the append comes first precisely so
// a failure loses nothing. If it could commit the head and then fail to re-append,
// Requeue would be a data-loss path wearing the name of a recovery one.
func TestRequeueFailedAppendLeavesRecordAtHead(t *testing.T) {
	m, u := impCodec()
	// One segment, already full: the re-append has nowhere to go.
	q, err := New[[]byte](t.TempDir(), m, u,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	rec := make([]byte, 900)
	for {
		if err := q.Add(rec); err != nil {
			if errors.Is(err, ErrFull) {
				break
			}
			t.Fatal(err)
		}
	}
	before, beforeBytes := q.Count(), q.Size()

	r := q.NewReader()
	ok, err := r.Requeue()
	if ok || !errors.Is(err, ErrFull) {
		t.Fatalf("Requeue with no room: ok=%v err=%v, want false and ErrFull", ok, err)
	}
	if got := q.Count(); got != before {
		t.Fatalf("Count=%d after a failed Requeue, want %d", got, before)
	}
	if got := q.Size(); got != beforeBytes {
		t.Fatalf("Size=%d after a failed Requeue, want %d", got, beforeBytes)
	}
	// And the record is still at the head, deliverable.
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("the record was not left at the head: ok=%v err=%v", ok, err)
	}
}

// TestOversizedSegmentSurvivesReopen is the load-bearing case for oversized
// records, and the reason the segment carries a flag instead of being inferred.
//
// A segment longer than headerSize+SegmentSize is ambiguous on its face: it
// equally describes a store created with a LARGER SegmentSize and reopened with a
// smaller one, which must still be refused with ErrSegmentSizeMismatch rather than
// half-read. The flag is what separates the two, so this test checks both
// directions — the oversized store reopens and delivers, and a genuine geometry
// change is still rejected.
func TestOversizedSegmentSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	m, u := impCodec()
	big := make([]byte, 20000) // way past the 4 KiB geometry below
	for i := range big {
		big[i] = byte(i * 7)
	}
	q, err := New[[]byte](dir, m, u, Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Add(big); err != nil {
		t.Fatalf("oversized add: %v", err)
	}
	if err := q.Add([]byte("after")); err != nil {
		t.Fatal(err)
	}
	wantCount, wantBytes := q.Count(), q.Size()
	if got := q.Stats().DiskBytes; got < int64(len(big)) {
		t.Fatalf("DiskBytes=%d, want at least the oversized record's %d bytes: "+
			"the footprint is no longer segments × SegmentSize", got, len(big))
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	// Same geometry: the oversized segment is recognised, not rejected.
	m2, u2 := impCodec()
	q2, err := New[[]byte](dir, m2, u2, Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatalf("reopen: %v — an oversized segment was mistaken for a geometry change", err)
	}
	defer func() { _ = q2.Close() }()
	if q2.Count() != wantCount || q2.Size() != wantBytes {
		t.Fatalf("after reopen: count=%d bytes=%d, want %d and %d",
			q2.Count(), q2.Size(), wantCount, wantBytes)
	}
	if st := q2.Stats(); st.Corruptions != 0 || st.LostBytes != 0 {
		t.Fatalf("reopening an oversized segment reported damage: %+v", st)
	}
	r := q2.NewReader()
	got, ok, err := r.TryTake()
	if !ok || err != nil {
		t.Fatalf("take: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("oversized record came back as %d bytes, want %d", len(got), len(big))
	}

	// A real geometry change is still refused: the ordinary segments do not match.
	_ = q2.Close()
	m3, u3 := impCodec()
	if bad, err := New[[]byte](dir, m3, u3, Options{SegmentSize: 8192, MaxSegments: -1}); !errors.Is(err, ErrSegmentSizeMismatch) {
		if err == nil {
			_ = bad.Close()
		}
		t.Fatalf("reopen with a different SegmentSize: %v, want ErrSegmentSizeMismatch", err)
	}
}
