package diskqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Ack is the per-record acknowledgement that makes Reserve/Commit usable from
// several workers at once. Commit is a prefix operation, so with competing
// consumers one worker's commit retires another's in-flight record — these pin
// that Ack does not, that it still reaches disk only across a contiguous run, and
// that it stays consistent with every other path that can move the commit cursor.

func openAckQueue(t *testing.T, opts Options) (*Queue[uint64], string) {
	t.Helper()
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

func ackOpts() Options {
	return Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}
}

// reserveN reserves n records and returns their offsets in read order.
func reserveN(t *testing.T, r *Reader[uint64], n int) []int64 {
	t.Helper()
	offs := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		_, ok, off, err := r.TryReserve()
		if err != nil || !ok {
			t.Fatalf("reserve %d: ok=%v err=%v", i, ok, err)
		}
		offs = append(offs, off)
	}
	return offs
}

func addN(t *testing.T, w *Queue[uint64], n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := w.Add(uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAckDoesNotRetireEarlierRecords is the whole point: acknowledging the last
// of three reserved records must leave the first two outstanding. Commit(off[2])
// would retire all three, which is what silently loses a competing consumer's
// in-flight work.
func TestAckDoesNotRetireEarlierRecords(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)

	if err := r.Ack(offs[2]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("Count after acking only the last = %d, want 3 (nothing may retire yet)", got)
	}
	// Acking the remaining two completes the run and all three retire together.
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count after acking the first = %d, want 2", got)
	}
	if err := r.Ack(offs[1]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count after acking all three = %d, want 0", got)
	}
}

// TestAckSurvivesReopen: only the contiguous acknowledged prefix is durable. An
// ack stranded behind a gap replays, which is the at-least-once contract.
func TestAckSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	addN(t, w, 4)
	r := w.NewReader()
	offs := reserveN(t, r, 4)
	// Acknowledge 0, 1 and 3 — 2 is the gap.
	for _, i := range []int{0, 1, 3} {
		if err := r.Ack(offs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	if got := w2.Count(); got != 2 {
		t.Fatalf("Count after reopen = %d, want 2 (records 2 and 3 replay)", got)
	}
	r2 := w2.NewReader()
	for _, want := range []uint64{2, 3} {
		v, ok, err := r2.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("replayed %v (ok=%v err=%v), want %d", v, ok, err, want)
		}
	}
}

// TestAckIdempotent: acking twice is accepted from both sides of the retire —
// once while the entry is still outstanding behind a gap, and once after the
// cursor has passed it.
func TestAckIdempotent(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 2)
	r := w.NewReader()
	offs := reserveN(t, r, 2)

	// Behind a gap: the second ack re-marks a flag that is already set.
	for i := 0; i < 2; i++ {
		if err := r.Ack(offs[1]); err != nil {
			t.Fatalf("ack behind a gap, attempt %d: %v", i, err)
		}
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count = %d, want 2", got)
	}
	// After the retire: the offset is now below the commit cursor.
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := r.Ack(offs[0]); err != nil {
			t.Fatalf("ack after retire, attempt %d: %v", i, err)
		}
		if err := r.Ack(offs[1]); err != nil {
			t.Fatalf("ack after retire (second offset), attempt %d: %v", i, err)
		}
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}
}

// TestAckRejectsUnknownOffset: an offset past the read cursor, and one that is
// inside a record rather than on a reservation boundary, are both refused. The
// second is the case Commit deliberately tolerates, and Ack must not — there is
// no "nearest earlier record" reading of a per-record acknowledgement.
func TestAckRejectsUnknownOffset(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 2)
	r := w.NewReader()
	offs := reserveN(t, r, 2)

	if err := r.Ack(offs[1] + 1); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("ack past the read cursor = %v, want ErrInvalidOffset", err)
	}
	if err := r.Ack(offs[0] - 1); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("ack inside a record = %v, want ErrInvalidOffset", err)
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("a refused ack must retire nothing; Count = %d, want 2", got)
	}
}

// TestAckAfterRewindRejected: Rewind returns every reserved record to the queue,
// so the offsets it invalidated must not retire anything — they now name records
// somebody else may be holding.
func TestAckAfterRewindRejected(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	// offs[0] is retired; 1 and 2 are still reserved and are what Rewind returns.
	n, err := w.Rewind()
	if err != nil {
		t.Fatal(err)
	}
	if n <= 0 {
		t.Fatalf("Rewind made %d bytes readable, want > 0", n)
	}
	for _, i := range []int{1, 2} {
		if err := r.Ack(offs[i]); !errors.Is(err, ErrInvalidOffset) {
			t.Fatalf("ack of pre-Rewind offset %d = %v, want ErrInvalidOffset", i, err)
		}
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count = %d, want 2 (both rewound records still queued)", got)
	}

	// The rewound records are reserved again — the same offsets, handed out a
	// second time. A ledger that kept its stale entries would now hold each offset
	// twice and no longer be sorted, so the bisect could match the dead entry and
	// the run walk step across a record nobody acknowledged.
	again := reserveN(t, r, 2)
	if err := r.Ack(again[1]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count after acking only the second = %d, want 2", got)
	}
	if err := r.Ack(again[0]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count after acking both = %d, want 0", got)
	}
}

// TestAckedRunStopsAtUnreservedHole pins the contiguity test in ackedRunEnd, at
// the store level because the hole it defends against needs a commit to fail: a
// Take that read a record and could not commit it leaves the read cursor past a
// record that is neither retired nor reserved, and its caller is promised the
// record replays. Acknowledging the reservation behind such a hole must not
// commit across it.
func TestAckedRunStopsAtUnreservedHole(t *testing.T) {
	s := &store{}
	s.reserved = []reservation{
		{start: 0, end: 10, acked: true},
		{start: 20, end: 30, acked: true}, // 10..20 reached the head some other way
	}
	if got := s.ackedRunEnd(); got != 10 {
		t.Fatalf("ackedRunEnd = %d, want 10 (the hole at 10..20 stops the run)", got)
	}
	// With the hole filled by a reservation of its own, the run continues.
	s.reserved = []reservation{
		{start: 0, end: 10, acked: true},
		{start: 10, end: 20, acked: true},
		{start: 20, end: 30, acked: true},
	}
	if got := s.ackedRunEnd(); got != 30 {
		t.Fatalf("ackedRunEnd = %d, want 30", got)
	}
	// An unacknowledged entry stops it just as a hole does.
	s.reserved[1].acked = false
	if got := s.ackedRunEnd(); got != 10 {
		t.Fatalf("ackedRunEnd = %d, want 10 (entry 1 is not acknowledged)", got)
	}
}

// TestAckMixedWithCommit: a Commit that passes reservations simply retires them,
// and the ledger must follow rather than keep stale entries that a later Ack
// could act on.
func TestAckMixedWithCommit(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 4)
	r := w.NewReader()
	offs := reserveN(t, r, 4)

	if err := r.Ack(offs[3]); err != nil { // stranded behind three gaps
		t.Fatal(err)
	}
	if err := r.Commit(offs[2]); err != nil { // retires 0, 1 and 2 wholesale
		t.Fatal(err)
	}
	// The commit completed the run: 3 was already acknowledged, so it goes too.
	if err := r.Ack(offs[3]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}
}

// TestAckAfterSkip: Skip moves the shared head and commits it, so a reservation
// it stepped over is retired and acking it is a no-op rather than an error.
func TestAckAfterSkip(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 2)
	r := w.NewReader()
	offs := reserveN(t, r, 1)

	// Reserve one, then skip the record behind it and commit across both.
	if err := r.Commit(offs[0]); err != nil {
		t.Fatal(err)
	}
	ok, err := r.Skip()
	if err != nil || !ok {
		t.Fatalf("Skip: ok=%v err=%v", ok, err)
	}
	if err := r.Ack(offs[0]); err != nil {
		t.Fatalf("ack of an already-retired reservation = %v, want nil", err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}
}

// TestAckConcurrentWorkers is the shape the whole feature exists for: several
// workers reserving and acknowledging in whatever order they finish. Every record
// must be delivered exactly once and the queue must end up fully retired.
func TestAckConcurrentWorkers(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	const n = 200
	addN(t, w, n)

	var mu sync.Mutex
	seen := make(map[uint64]int, n)

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			r := w.NewReader()
			for {
				v, ok, off, err := r.TryReserve()
				if err != nil || !ok {
					return
				}
				mu.Lock()
				seen[v]++
				mu.Unlock()
				// Stagger the acknowledgements so they land out of order across
				// workers, which is exactly what Commit cannot survive.
				if worker%2 == 0 {
					_, _, _, _ = r.TryReserve() // may return nothing; ignored
				}
				if err := r.Ack(off); err != nil {
					t.Errorf("worker %d ack: %v", worker, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()

	// Drain anything the staggering left reserved-but-unacked.
	r := w.NewReader()
	for {
		_, ok, err := r.TryTake()
		if err != nil || !ok {
			break
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for i := uint64(0); i < n; i++ {
		if seen[i] > 1 {
			t.Fatalf("record %d delivered %d times", i, seen[i])
		}
	}
}

// TestAckBlockedRunHoldsReclamation states the cost plainly, so a change that
// quietly retires past a gap fails here: an unacknowledged record keeps
// everything behind it uncommitted, and Stats reports it as in flight.
func TestAckBlockedRunHoldsReclamation(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 5)
	r := w.NewReader()
	offs := reserveN(t, r, 5)

	for _, i := range []int{1, 2, 3, 4} { // everything except the head
		if err := r.Ack(offs[i]); err != nil {
			t.Fatal(err)
		}
	}
	st := w.Stats()
	if st.Backlog != 5 {
		t.Fatalf("Backlog = %d, want 5 (one unacknowledged record holds the run)", st.Backlog)
	}
	if st.InFlightBytes <= 0 {
		t.Fatalf("InFlightBytes = %d, want > 0", st.InFlightBytes)
	}
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count after the head is acknowledged = %d, want 0", got)
	}
}

// TestAckClosed: Ack answers ErrClosed like every other consume op.
func TestAckClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	addN(t, w, 1)
	r := w.NewReader()
	offs := reserveN(t, r, 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Ack(offs[0]); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ack after Close = %v, want ErrClosed", err)
	}
}

// TestAckWakesBlockedProducer: the retire frees capacity, so a producer parked in
// AddWait on a full queue must wake — Ack signals space like Commit does.
func TestAckWakesBlockedProducer(t *testing.T) {
	w, _ := openAckQueue(t, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxBytes: 64})
	// Fill it until Add refuses.
	var offs []int64
	for {
		if err := w.Add(1); err != nil {
			if errors.Is(err, ErrFull) {
				break
			}
			t.Fatal(err)
		}
	}
	r := w.NewReader()
	_, ok, off, err := r.TryReserve()
	if err != nil || !ok {
		t.Fatalf("reserve: ok=%v err=%v", ok, err)
	}
	offs = append(offs, off)

	done := make(chan error, 1)
	go func() { done <- w.AddWait(context.Background(), 99) }()

	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("AddWait after Ack freed space: %v", err)
	}
}

// TestLedgerBoundedWithoutAck: a Reserve/Commit consumer never enters ackTo, so
// nothing there would drop retired entries — the ledger has to be trimmed on the
// reserve side too, or it grows for the lifetime of the queue. This is a leak, not
// a correctness bug, and nothing else in the suite would notice it.
func TestLedgerBoundedWithoutAck(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	r := w.NewReader()
	for i := 0; i < 2000; i++ {
		if err := w.Add(uint64(i)); err != nil {
			t.Fatal(err)
		}
		_, ok, off, err := r.TryReserve()
		if err != nil || !ok {
			t.Fatalf("reserve %d: ok=%v err=%v", i, ok, err)
		}
		if err := r.Commit(off); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(w.st.reserved); n > 8 {
		t.Fatalf("ledger holds %d entries after 2000 reserve+commit cycles; it must stay bounded by the outstanding reservations", n)
	}
}

// TestSkipRetiresOnlyHead is the ledger side of Skip: skipping the head while
// earlier records are reserved-but-unacknowledged must not retire them — the old
// prefix commit did, silently, and they were gone after a crash. The skip
// becomes durable only once the reservations ahead of it acknowledge.
func TestSkipRetiresOnlyHead(t *testing.T) {
	w, dir := openAckQueue(t, ackOpts())
	addN(t, w, 3) // A, B, C; C will be skipped while A and B are in flight
	r := w.NewReader()
	reserveN(t, r, 2) // A and B in flight, never acknowledged this session

	ok, err := r.Skip()
	if err != nil || !ok {
		t.Fatalf("Skip: ok=%v err=%v", ok, err)
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("Count = %d, want 3: a deferred skip retires nothing yet", got)
	}

	// Simulate a crash before the in-flight records acknowledge: all three must
	// replay — the skip was consumed this session but never durably committed.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	if got := w2.Count(); got != 3 {
		t.Fatalf("Count after reopen = %d, want 3: reserved records and the un-durable skip all replay", got)
	}

	// This time acknowledge the in-flight records after the skip: the whole run
	// — both acks and the skipped record — retires together.
	r2 := w2.NewReader()
	offs := reserveN(t, r2, 2)
	if ok, err := r2.Skip(); err != nil || !ok {
		t.Fatalf("Skip: ok=%v err=%v", ok, err)
	}
	for _, off := range offs {
		if err := r2.Ack(off); err != nil {
			t.Fatal(err)
		}
	}
	if got := w2.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0: the acks complete the run and the skip retires with it", got)
	}
}

// TestRequeueBehindReservation: the head rotation's retire goes through the
// ledger too, so a competing consumer's in-flight record survives a Requeue and
// the rotated original retires once that record acknowledges.
func TestRequeueBehindReservation(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 2) // A (reserved), B (rotated)
	r := w.NewReader()
	offs := reserveN(t, r, 1)

	ok, err := r.Requeue()
	if err != nil || !ok {
		t.Fatalf("Requeue: ok=%v err=%v", ok, err)
	}
	// A in flight + B's original (not yet retired) + B's tail copy.
	if got := w.Count(); got != 3 {
		t.Fatalf("Count = %d, want 3 before the ack", got)
	}
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	// A and the rotated original retire together; the tail copy remains.
	if got := w.Count(); got != 1 {
		t.Fatalf("Count = %d, want 1 after the ack", got)
	}
}

// TestAckBatch: one call, one commit, same per-offset contract as Ack.
func TestAckBatch(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 4)
	r := w.NewReader()
	offs := reserveN(t, r, 4)

	// Out-of-order and with a duplicate: idempotent, order-free, all retired.
	if err := r.AckBatch(offs[2], offs[0], offs[1], offs[0], offs[3]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0", got)
	}

	// A bogus offset reports ErrInvalidOffset but must not hold the valid
	// acknowledgements hostage.
	addN(t, w, 2)
	offs = reserveN(t, r, 2)
	err := r.AckBatch(offs[0], 1<<40, offs[1])
	if !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("AckBatch with a bogus offset = %v, want ErrInvalidOffset", err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0: valid offsets in the batch still retire", got)
	}

	// A batch blocked behind a gap acknowledges but commits nothing yet.
	addN(t, w, 3)
	offs = reserveN(t, r, 3)
	if err := r.AckBatch(offs[1], offs[2]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("Count = %d, want 3: the run is blocked behind the unacked head", got)
	}
	if err := r.AckBatch(offs[0]); err != nil {
		t.Fatal(err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count = %d, want 0 once the gap closes", got)
	}
}

// TestAddSizedAndLastBytesAgree: the producer- and consumer-side sizes are the
// same number, and it is the unit the byte gauges count.
func TestAddSizedAndLastBytesAgree(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	before := w.Stats().BacklogBytes
	n, err := w.AddSized(42)
	if err != nil {
		t.Fatal(err)
	}
	if n <= 0 {
		t.Fatalf("AddSized = %d, want > 0", n)
	}
	if got := w.Stats().BacklogBytes - before; got != n {
		t.Fatalf("BacklogBytes grew by %d, AddSized said %d", got, n)
	}

	r := w.NewReader()
	v, ok, off, err := r.TryReserve()
	if err != nil || !ok || v != 42 {
		t.Fatalf("TryReserve: v=%d ok=%v err=%v", v, ok, err)
	}
	if got := r.LastBytes(); got != n {
		t.Fatalf("LastBytes = %d, AddSized said %d", got, n)
	}
	if err := r.Ack(off); err != nil {
		t.Fatal(err)
	}
}
