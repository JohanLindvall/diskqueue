package diskqueue

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The three producer/inspection additions: TryPeek (look without consuming),
// AddBatch (amortized durability), AddWait (blocking backpressure) — plus the
// group-commit path that Add's default policy now runs on. The durability
// ordering itself is pinned in faults_test.go; these pin the semantics.

func openFeatureQueue(t *testing.T, opts Options) (*Queue[uint64], string) {
	t.Helper()
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

// TestTryPeekDoesNotConsume: peeking is stateless — same item on every peek,
// every cursor untouched, and the consume that follows gets the same record.
func TestTryPeekDoesNotConsume(t *testing.T) {
	w, _ := openFeatureQueue(t, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	r := w.NewReader()
	for i := 0; i < 2; i++ {
		v, ok, err := r.TryPeek()
		if err != nil || !ok || v != 0 {
			t.Fatalf("peek %d: v=%d ok=%v err=%v, want 0 both times", i, v, ok, err)
		}
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("Count=%d after peeking, want 3", got)
	}
	if w.Empty() {
		t.Fatal("Empty=true after peeking: the cursor moved")
	}
	if v, ok, err := r.TryTake(); err != nil || !ok || v != 0 {
		t.Fatalf("take after peek: v=%d ok=%v err=%v", v, ok, err)
	}
	if v, ok, err := r.TryPeek(); err != nil || !ok || v != 1 {
		t.Fatalf("peek after take: v=%d ok=%v err=%v", v, ok, err)
	}
}

// TestTryPeekEmptyAndClosed: the ordinary edges.
func TestTryPeekEmptyAndClosed(t *testing.T) {
	w, _ := openFeatureQueue(t, Options{NoSync: true})
	r := w.NewReader()
	if _, ok, err := r.TryPeek(); ok || err != nil {
		t.Fatalf("peek on empty: ok=%v err=%v, want a clean miss", ok, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.TryPeek(); !errors.Is(err, ErrClosed) {
		t.Fatalf("peek after close: %v, want ErrClosed", err)
	}
}

// TestTryPeekCorruptIsPreview: a damaged head previews as ErrCorrupt with
// NOTHING booked and nothing moved — the consume op that follows books the
// event exactly once. Peeking must never double-count a loss.
func TestTryPeekCorruptIsPreview(t *testing.T) {
	w, r, _ := openRecoveryTest(t)
	for i := uint64(0); i < 2; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	flipPayload(t, w, 0, 2)

	for i := 0; i < 2; i++ {
		if _, ok, err := r.TryPeek(); ok || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("peek %d over damage: ok=%v err=%v, want ErrCorrupt", i, ok, err)
		}
	}
	if st := w.Stats(); st.Corruptions != 0 || st.LostRecords != 0 {
		t.Fatalf("peek booked a loss: %+v", st)
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count=%d after corrupt peeks, want 2: nothing may be dropped", got)
	}
	// The consume books it once, and the queue moves.
	if _, ok, err := r.TryTake(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("take over damage: ok=%v err=%v", ok, err)
	}
	if st := w.Stats(); st.Corruptions != 1 || st.LostRecords != 1 {
		t.Fatalf("consume did not book the event exactly once: %+v", st)
	}
	if v, ok, err := r.TryTake(); err != nil || !ok || v != 1 {
		t.Fatalf("record after the damage: v=%d ok=%v err=%v", v, ok, err)
	}
}

// TestTryPeekCodecError: a decode failure previews as ErrCodec and leaves the
// record in place for Skip or a fixed codec — same stance as the consume path.
func TestTryPeekCodecError(t *testing.T) {
	dir := t.TempDir()
	reject := true
	m := func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil }
	u := func(data []byte) ([]byte, error) {
		if reject {
			return nil, errors.New("bad codec day")
		}
		return append([]byte(nil), data...), nil
	}
	w, err := New[[]byte](dir, m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add([]byte("x")); err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	if _, _, err := r.TryPeek(); !errors.Is(err, ErrCodec) {
		t.Fatalf("peek with failing codec: %v, want ErrCodec", err)
	}
	reject = false
	v, ok, err := r.TryPeek()
	if err != nil || !ok || !bytes.Equal(v, []byte("x")) {
		t.Fatalf("peek after codec recovered: %q ok=%v err=%v", v, ok, err)
	}
}

// TestAddBatchDurableAcrossSegments: the batch's records cross several
// segments, come back complete and in order after a reopen, and the count is
// exact — the amortized publication must not weaken what Add promises.
func TestAddBatchDurableAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SegmentSize: 4096, MaxSegments: -1}) // per-op durability
	if err != nil {
		t.Fatal(err)
	}
	const n = 700 // several 4 KiB segments at 17 bytes a record
	items := make([]uint64, n)
	for i := range items {
		items[i] = uint64(i)
	}
	got, err := w.AddBatch(items)
	if err != nil || got != n {
		t.Fatalf("AddBatch: n=%d err=%v, want %d and nil", got, err, n)
	}
	if c := w.Count(); c != n {
		t.Fatalf("Count=%d, want %d", c, n)
	}
	if w.Stats().Segments < 3 {
		t.Fatalf("Segments=%d: the batch should have crossed segments", w.Stats().Segments)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()
	for i := uint64(0); i < n; i++ {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != i {
			t.Fatalf("record %d after reopen: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
	if !w2.Empty() {
		t.Fatal("queue should be drained")
	}
}

// TestAddBatchPrefixOnFull: the batch stops at the cap with the leading n
// placed and durable, and says so — never all-or-nothing, never silent loss.
func TestAddBatchPrefixOnFull(t *testing.T) {
	const recLen = 17 // uvarint(8) + 8 payload + 8 checksum
	w, _ := openFeatureQueue(t,
		Options{SegmentSize: 4096, MaxSegments: -1, MaxBytes: 3 * recLen})
	items := []uint64{0, 1, 2, 3, 4}
	n, err := w.AddBatch(items)
	if !errors.Is(err, ErrFull) {
		t.Fatalf("AddBatch past the cap: %v, want ErrFull", err)
	}
	if n != 3 {
		t.Fatalf("n=%d, want 3: the prefix that fit", n)
	}
	r := w.NewReader()
	for i := uint64(0); i < 3; i++ {
		v, ok, terr := r.TryTake()
		if terr != nil || !ok || v != i {
			t.Fatalf("record %d: v=%d ok=%v err=%v", i, v, ok, terr)
		}
	}
	if _, ok, err := r.TryTake(); ok || err != nil {
		t.Fatalf("after the prefix: ok=%v err=%v, want empty", ok, err)
	}
}

// TestAddBatchMarshalErrorKeepsPrefix: a codec failure mid-batch publishes
// what preceded it and reports both the count and the error.
func TestAddBatchMarshalErrorKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	bad := errors.New("unserializable")
	m := func(dst []byte, v uint64) ([]byte, error) {
		if v == 2 {
			return nil, bad
		}
		return marshalU64(dst, v)
	}
	w, err := New[uint64](dir, m, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	n, err := w.AddBatch([]uint64{0, 1, 2, 3})
	if n != 2 || !errors.Is(err, bad) {
		t.Fatalf("n=%d err=%v, want 2 and the codec error", n, err)
	}
	if c := w.Count(); c != 2 {
		t.Fatalf("Count=%d, want the 2 published records", c)
	}
}

// TestAddBatchEmptyAndDeferredPolicies: the trivial batch, and the batch under
// NoSync/SyncEvery (which ride the plain append path).
func TestAddBatchEmptyAndDeferredPolicies(t *testing.T) {
	for _, opts := range []Options{
		{NoSync: true},
		{SyncEvery: 50},
	} {
		w, _ := openFeatureQueue(t, opts)
		if n, err := w.AddBatch(nil); n != 0 || err != nil {
			t.Fatalf("empty batch: n=%d err=%v", n, err)
		}
		n, err := w.AddBatch([]uint64{1, 2, 3})
		if n != 3 || err != nil {
			t.Fatalf("batch under %+v: n=%d err=%v", opts, n, err)
		}
		if c := w.Count(); c != 3 {
			t.Fatalf("Count=%d, want 3", c)
		}
	}
}

// TestAddWaitBlocksUntilCommitFrees: the producer half of backpressure. A full
// queue parks AddWait; the commit that frees capacity releases it.
func TestAddWaitBlocksUntilCommitFrees(t *testing.T) {
	const recLen = 17
	w, _ := openFeatureQueue(t,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxBytes: 2 * recLen})
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(2); !errors.Is(err, ErrFull) {
		t.Fatalf("Add past the cap: %v, want ErrFull (the setup is wrong otherwise)", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.AddWait(context.Background(), 2)
	}()
	select {
	case err := <-done:
		t.Fatalf("AddWait returned %v while the queue was full: it must block", err)
	case <-time.After(50 * time.Millisecond):
	}

	r := w.NewReader()
	if v, ok, err := r.TryTake(); err != nil || !ok || v != 0 {
		t.Fatalf("TryTake: v=%d ok=%v err=%v", v, ok, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AddWait after space freed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddWait still blocked after a commit freed capacity")
	}
	// FIFO held: 1 then 2.
	for _, want := range []uint64{1, 2} {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("drain: v=%d ok=%v err=%v, want %d", v, ok, err, want)
		}
	}
}

// TestAddWaitContextAndPermanentErrors: cancellation unblocks with ctx.Err,
// and a record no drain can ever admit fails fast instead of waiting forever.
func TestAddWaitContextAndPermanentErrors(t *testing.T) {
	w, _ := openFeatureQueue(t,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxBytes: 17})
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.AddWait(ctx, 1) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled AddWait: %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled AddWait never returned")
	}
}

// TestAddWaitTooLargeImmediate pins the permanent-refusal path with a payload
// that genuinely exceeds the cap.
func TestAddWaitTooLargeImmediate(t *testing.T) {
	dir := t.TempDir()
	m := func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil }
	u := func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
	w, err := New[[]byte](dir, m, u, Options{NoSync: true, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.AddWait(context.Background(), make([]byte, 128)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("AddWait with an inadmissible record: %v, want ErrRecordTooLarge immediately", err)
	}
}

// TestAddWaitClosedWhileWaiting: Close wakes a parked producer with ErrClosed
// instead of leaving it stranded.
func TestAddWaitClosedWhileWaiting(t *testing.T) {
	w, _ := openFeatureQueue(t,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxBytes: 17})
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.AddWait(context.Background(), 1) }()
	time.Sleep(20 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("AddWait across Close: %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddWait never noticed the Close")
	}
}

// TestConcurrentAddsAllDurable drives the group-commit path hard: many
// producers on the per-op policy, every Add individually durable, nothing
// lost, nothing duplicated — the sharing must change the cost, not the
// contract. Run under -race this also proves the leader/follower handoff.
func TestConcurrentAddsAllDurable(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SegmentSize: 1 << 20, MaxSegments: -1}) // per-op durability
	if err != nil {
		t.Fatal(err)
	}
	const producers, each = 8, 200
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := w.Add(uint64(p*10000 + i)); err != nil {
					t.Errorf("producer %d add %d: %v", p, i, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if c := w.Count(); c != producers*each {
		t.Fatalf("Count=%d, want %d", c, producers*each)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Every record survives the reopen: each Add was durable when it returned.
	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SegmentSize: 1 << 20, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	seen := make(map[uint64]bool, producers*each)
	r := w2.NewReader()
	for {
		v, ok, err := r.TryTake()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if seen[v] {
			t.Fatalf("record %d delivered twice", v)
		}
		seen[v] = true
	}
	if len(seen) != producers*each {
		t.Fatalf("recovered %d records, want %d", len(seen), producers*each)
	}
}

// TestConcurrentAddsWithConsumersAndCycles mixes producers, consumers and
// small segments (so cycling quiesces flushes) — the full interleaving the
// leader/follower protocol has to survive. Everything added is delivered
// exactly once across the live drain and a final sweep.
func TestConcurrentAddsWithConsumersAndCycles(t *testing.T) {
	w, _ := openFeatureQueue(t,
		Options{SegmentSize: 4096, MaxSegments: -1}) // per-op, tiny segments
	const producers, each = 4, 150
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := w.Add(uint64(p*10000 + i)); err != nil {
					t.Errorf("producer %d: %v", p, err)
					return
				}
			}
		}(p)
	}
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var cwg sync.WaitGroup
	stop := make(chan struct{})
	for c := 0; c < 2; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			r := w.NewReader()
			for {
				v, ok, err := r.TryTake()
				if err != nil {
					t.Errorf("consumer: %v", err)
					return
				}
				if !ok {
					select {
					case <-stop:
						return
					default:
						continue
					}
				}
				mu.Lock()
				if seen[v] {
					t.Errorf("record %d delivered twice", v)
				}
				seen[v] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	cwg.Wait()
	if t.Failed() {
		return
	}
	// Sweep the tail the consumers may have left.
	r := w.NewReader()
	for {
		v, ok, err := r.TryTake()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if seen[v] {
			t.Fatalf("record %d delivered twice", v)
		}
		seen[v] = true
	}
	if len(seen) != producers*each {
		t.Fatalf("delivered %d records, want %d", len(seen), producers*each)
	}
}

// TestRequeueDuringConcurrentAdds: Requeue quiesces the flush machinery and
// rotates atomically; hammered from the side it must neither lose nor
// duplicate beyond what at-least-once already allows for its own rotation.
func TestRequeueDuringConcurrentAdds(t *testing.T) {
	w, _ := openFeatureQueue(t, Options{SegmentSize: 1 << 20, MaxSegments: -1})
	for i := uint64(0); i < 10; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(100); i < 150; i++ {
			if err := w.Add(i); err != nil {
				t.Errorf("add: %v", err)
				return
			}
		}
	}()
	r := w.NewReader()
	for i := 0; i < 20; i++ {
		if _, err := r.Requeue(); err != nil {
			t.Fatalf("requeue %d: %v", i, err)
		}
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if c := w.Count(); c != 60 {
		t.Fatalf("Count=%d, want 60: rotation is backlog-neutral", c)
	}
}

// TestAddWaitManyProducers: several parked producers all eventually place
// their records as a consumer drains — no waiter is stranded by the shared
// wakeup.
func TestAddWaitManyProducers(t *testing.T) {
	const recLen = 17
	w, _ := openFeatureQueue(t,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxBytes: 2 * recLen})
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	const waiters = 4
	done := make(chan error, waiters)
	for p := 0; p < waiters; p++ {
		go func(p int) {
			done <- w.AddWait(context.Background(), uint64(100+p))
		}(p)
	}
	r := w.NewReader()
	delivered := 0
	deadline := time.After(10 * time.Second)
	finished := 0
	for finished < waiters {
		if _, ok, err := r.TryTake(); err != nil {
			t.Fatal(err)
		} else if ok {
			delivered++
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("AddWait: %v", err)
			}
			finished++
		case <-deadline:
			t.Fatalf("only %d of %d waiters finished; %d delivered", finished, waiters, delivered)
		default:
		}
	}
	for {
		_, ok, err := r.TryTake()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		delivered++
	}
	if delivered != 2+waiters {
		t.Fatalf("delivered %d records, want %d", delivered, 2+waiters)
	}
}
