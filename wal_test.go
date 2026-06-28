package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// uint64 codec used by the tests: zero-allocation, append-based marshal and a
// zero-copy unmarshal.
func marshalU64(dst []byte, v uint64) ([]byte, error) {
	return binary.LittleEndian.AppendUint64(dst, v), nil
}

func unmarshalU64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.New("bad length")
	}
	return binary.LittleEndian.Uint64(data), nil
}

func openTest(t *testing.T, maxSegments int) (*WAL[uint64], *Reader[uint64]) {
	t.Helper()
	if maxSegments == 0 {
		maxSegments = -1 // these tests pass 0 for "unbounded" (now a negative value)
	}
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{MaxSegments: maxSegments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, w.NewReader()
}

func TestAddReserveCommit(t *testing.T) {
	w, r := openTest(t, 0)
	if !w.Empty() {
		t.Fatal("new log should be empty")
	}
	for i := uint64(0); i < 5; i++ {
		if err := w.Add(i); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if w.Empty() {
		t.Fatal("log should not be empty")
	}

	// Reserve without commit returns successive items and offsets.
	v, ok, off, err := r.TryReserve()
	if err != nil || !ok || v != 0 {
		t.Fatalf("TryReserve got v=%d ok=%v err=%v", v, ok, err)
	}
	v, ok, _, _ = r.TryReserve()
	if !ok || v != 1 {
		t.Fatalf("second TryReserve got %d", v)
	}

	// Commit the first item only.
	if err := r.Commit(off); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Remaining items (2,3,4) come out via Take.
	for want := uint64(2); want < 5; want++ {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("TryTake got v=%d ok=%v err=%v want=%d", v, ok, err, want)
		}
	}
	if _, ok, _ := r.TryTake(); ok {
		t.Fatal("expected empty after draining")
	}
	if !w.Empty() {
		t.Fatal("should be empty")
	}
}

func TestDrain(t *testing.T) {
	w, r := openTest(t, 0)
	for i := uint64(10); i < 20; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	var got []uint64
	for v := range r.Drain(context.Background()) {
		got = append(got, v)
		// Calling other methods from within the yield must not deadlock.
		_ = w.Empty()
	}
	if len(got) != 10 || got[0] != 10 || got[9] != 19 {
		t.Fatalf("Drain returned %v", got)
	}
	// Drain commits each item, so the queue is drained afterwards.
	if !w.Empty() {
		t.Fatal("Drain should have drained the queue")
	}
}

func TestDrainCancelLeavesRemainder(t *testing.T) {
	w, r := openTest(t, 0)
	for i := uint64(0); i < 10; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	for range r.Drain(ctx) {
		seen++
		if seen == 3 {
			cancel()
		}
	}
	if seen != 3 {
		t.Fatalf("expected iteration to stop after cancel at 3, saw %d", seen)
	}
	// The first three were processed and committed; the remaining seven survive
	// for a later pass (at-least-once).
	remaining := 0
	for v := range r.Drain(context.Background()) {
		if v != uint64(3+remaining) {
			t.Fatalf("remainder out of order: got %d at %d", v, remaining)
		}
		remaining++
	}
	if remaining != 7 {
		t.Fatalf("expected 7 remaining, got %d", remaining)
	}
	if !w.Empty() {
		t.Fatal("queue should be drained after second pass")
	}
}

func TestFollow(t *testing.T) {
	w, r := openTest(t, 0)
	// Pre-existing items.
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan uint64, 16)
	go func() {
		for v := range r.Follow(ctx) {
			got <- v
		}
		close(got)
	}()

	// Drain the three existing items.
	for want := uint64(0); want < 3; want++ {
		select {
		case v := <-got:
			if v != want {
				t.Errorf("existing: got %d want %d", v, want)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out reading existing items")
		}
	}

	// Items added later must arrive too.
	for i := uint64(3); i < 6; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	for want := uint64(3); want < 6; want++ {
		select {
		case v := <-got:
			if v != want {
				t.Errorf("new: got %d want %d", v, want)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for new items")
		}
	}

	// Cancelling ends the iteration.
	cancel()
	select {
	case _, ok := <-got:
		// Either a final buffered value then close, or immediate close.
		if ok {
			select {
			case <-got:
			case <-time.After(time.Second):
				t.Fatal("Follow did not stop after cancel")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("Follow did not stop after cancel")
	}
}

func TestBlockingTake(t *testing.T) {
	w, r := openTest(t, 0)
	done := make(chan uint64, 1)
	go func() {
		v, ok, err := r.Take(context.Background())
		if err != nil || !ok {
			t.Errorf("Take: ok=%v err=%v", ok, err)
		}
		done <- v
	}()
	// Give the goroutine time to block.
	time.Sleep(20 * time.Millisecond)
	if err := w.Add(42); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-done:
		if v != 42 {
			t.Fatalf("got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Take did not wake")
	}
}

func TestBlockingReserveThenCommit(t *testing.T) {
	w, r := openTest(t, 0)
	go func() {
		time.Sleep(20 * time.Millisecond)
		w.Add(7)
	}()
	v, ok, off, err := r.Reserve(context.Background())
	if err != nil || !ok || v != 7 {
		t.Fatalf("Reserve: v=%d ok=%v err=%v", v, ok, err)
	}
	// Reserve advances the read cursor, so there is nothing more to read, but the
	// item is not committed yet: it still counts as in the log and would replay
	// after a crash until Commit acknowledges it.
	if !w.Empty() {
		t.Fatal("Reserve should advance past the only item")
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("uncommitted item should still count, Count=%d", got)
	}
	if err := r.Commit(off); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count after commit = %d, want 0", got)
	}
}

func TestSync(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// TestChurn drives many add/consume cycles through small segments, exercising
// segment cycling and the dropping of fully-committed segments under steady
// state.
func TestChurn(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r := w.NewReader()
	const n = 20000
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != i {
			t.Fatalf("TryTake %d: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
	if !w.Empty() {
		t.Fatal("should be empty after churn")
	}
}

func TestSizeAndCount(t *testing.T) {
	w, r := openTest(t, 0)
	if w.Count() != 0 || w.Size() != 0 {
		t.Fatalf("fresh: count=%d size=%d", w.Count(), w.Size())
	}
	for i := uint64(0); i < 5; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Each uint64 entry is a 1-byte uvarint length prefix, 8 payload bytes, and an
	// 8-byte checksum trailer.
	if got := w.Count(); got != 5 {
		t.Fatalf("count after adds = %d, want 5", got)
	}
	if want := int64(5 * (9 + checksumSize)); w.Size() != want {
		t.Fatalf("size after adds = %d, want %d", w.Size(), want)
	}
	// Consuming every item drops both Count and Size to zero, regardless of when
	// the disk space is actually reclaimed.
	for {
		if _, ok, err := r.TryTake(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("count after drain = %d, want 0", got)
	}
	if got := w.Size(); got != 0 {
		t.Fatalf("size after drain = %d, want 0", got)
	}
}

func countDataFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "data.") {
			n++
		}
	}
	return n
}

// TestReopenMultiFile exercises WAL-level recovery when the commit cursor lands
// inside a later segment (earlier ones fully consumed) and several files exist.
func TestReopenMultiFile(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	const n = 2000 // 9 bytes/record -> spans several 4 KiB segments
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if countDataFiles(t, dir) < 3 {
		t.Fatal("expected several data files")
	}
	// Consume and commit the first 1500, leaving 500 across the tail files.
	for i := uint64(0); i < 1500; i++ {
		if _, ok, err := r.TryTake(); err != nil || !ok {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	r2 := w2.NewReader()
	if got := w2.Count(); got != 500 {
		t.Fatalf("after reopen Count=%d, want 500", got)
	}
	for want := uint64(1500); want < n; want++ {
		v, ok, err := r2.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("after reopen take: v=%d ok=%v err=%v want=%d", v, ok, err, want)
		}
	}
	if !w2.Empty() {
		t.Fatal("should be drained")
	}
}

func TestRecordTooLarge(t *testing.T) {
	w, err := New[[]byte](t.TempDir(),
		func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil },
		func(data []byte) ([]byte, error) { return data, nil },
		Options{NoSync: true, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Add(make([]byte, 5000)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
	// A record that fits is still accepted.
	if err := w.Add(make([]byte, 100)); err != nil {
		t.Fatalf("small add: %v", err)
	}
}

func TestContextCancel(t *testing.T) {
	_, r := openTest(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, ok, _, err := r.Reserve(ctx)
	if ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got ok=%v err=%v", ok, err)
	}
}

func TestFull(t *testing.T) {
	// At most 2 small segments may be live at once.
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{MaxSegments: 2, NoSync: true, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r := w.NewReader()

	added := 0
	for {
		if err := w.Add(uint64(added)); err != nil {
			if errors.Is(err, ErrFull) {
				break
			}
			t.Fatal(err)
		}
		added++
		if added > 100000 {
			t.Fatal("never filled up")
		}
	}
	if added == 0 {
		t.Fatal("expected to add some items before ErrFull")
	}

	// Draining everything frees the segments; Add then succeeds again.
	for {
		if _, ok, err := r.TryTake(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
	}
	if err := w.Add(99999); err != nil {
		t.Fatalf("Add after draining should succeed: %v", err)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64)
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 6; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Consume and commit the first three.
	for i := 0; i < 3; i++ {
		if _, ok, err := r.TryTake(); err != nil || !ok {
			t.Fatalf("take: ok=%v err=%v", ok, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: committed items are gone, the rest replay in order.
	w2, err := New[uint64](dir, marshalU64, unmarshalU64)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	r2 := w2.NewReader()
	for want := uint64(3); want < 6; want++ {
		v, ok, err := r2.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("after reopen got v=%d ok=%v err=%v want=%d", v, ok, err, want)
		}
	}
	if !w2.Empty() {
		t.Fatal("should be drained after reopen")
	}
}

func TestCommitInvalid(t *testing.T) {
	w, r := openTest(t, 0)
	w.Add(1)
	if err := r.Commit(0); err != nil {
		t.Fatalf("Commit(0) should be no-op: %v", err)
	}
	if err := r.Commit(1 << 30); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("expected ErrInvalidOffset, got %v", err)
	}
}

// TestZeroAlloc asserts that the steady-state Add/Take cycle does not
// allocate on the heap.
func TestZeroAlloc(t *testing.T) {
	w, r := openTest(t, 0)
	// Warm up the scratch buffer and internal segment buffers.
	for i := 0; i < 100; i++ {
		w.Add(uint64(i))
		r.TryTake()
	}
	allocs := testing.AllocsPerRun(1000, func() {
		w.Add(1)
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take failed ok=%v err=%v", ok, err)
		}
	})
	if allocs > 0 {
		t.Fatalf("expected zero allocations per Add/Take cycle, got %v", allocs)
	}
}

func BenchmarkAddTake(b *testing.B) {
	w, err := New[uint64](b.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	r := w.NewReader()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Add(uint64(i))
		r.TryTake()
	}
}

// TestStressConcurrent runs several producers and consumers against one WAL and
// checks every value is delivered exactly once (and the WAL stays race-free).
func TestStressConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	const producers = 4
	const perProducer = 10000
	const total = producers * perProducer
	seen := make([]int32, total)

	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for k := 0; k < perProducer; k++ {
				v := uint64(p*perProducer + k)
				for {
					err := w.Add(v)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrFull) {
						t.Errorf("Add(%d): %v", v, err)
						return
					}
					runtime.Gosched()
				}
			}
		}(p)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var consumed int64
	var cwg sync.WaitGroup
	for c := 0; c < 3; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			r := w.NewReader()
			for {
				v, ok, err := r.Take(ctx)
				if err != nil || !ok {
					return
				}
				if atomic.AddInt32(&seen[v], 1) != 1 {
					t.Errorf("value %d delivered more than once", v)
				}
				if atomic.AddInt64(&consumed, 1) == total {
					cancel() // wake the other consumers
				}
			}
		}()
	}

	pwg.Wait()
	cwg.Wait()

	if got := atomic.LoadInt64(&consumed); got != total {
		t.Fatalf("consumed %d, want %d", got, total)
	}
	for i, c := range seen {
		if c != 1 {
			t.Fatalf("value %d delivered %d time(s)", i, c)
		}
	}
}

// TestStressConcurrentReserveCommit interleaves a producer with a single
// Reserve/Commit consumer plus periodic Drain passes, checking FIFO delivery
// and that nothing is lost.
func TestStressConcurrentReserveCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{NoSync: true, SegmentSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r := w.NewReader()

	const total = 50000
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < total; i++ {
			for {
				if err := w.Add(uint64(i)); err == nil {
					break
				} else if !errors.Is(err, ErrFull) {
					t.Errorf("Add: %v", err)
					return
				}
				runtime.Gosched()
			}
		}
	}()

	// Single consumer: values must arrive strictly in order.
	var next uint64
	ctx := context.Background()
	for next < total {
		v, ok, off, err := r.Reserve(ctx)
		if err != nil || !ok {
			t.Fatalf("Reserve: ok=%v err=%v", ok, err)
		}
		if v != next {
			t.Fatalf("out of order: got %d want %d", v, next)
		}
		next++
		if next%64 == 0 { // commit in batches
			if err := r.Commit(off); err != nil {
				t.Fatalf("Commit: %v", err)
			}
		}
	}
	<-done
	if next != total {
		t.Fatalf("consumed %d, want %d", next, total)
	}
}

// TestSegmentSizeMismatch verifies that reopening a store with a different
// SegmentSize is rejected instead of truncating and discarding records.
func TestSegmentSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 100; i++ { // span more than one 8 KiB segment
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening with a different size must fail loudly, not corrupt the files.
	if _, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 16384}); !errors.Is(err, ErrSegmentSizeMismatch) {
		t.Fatalf("reopen with wrong size: got %v, want ErrSegmentSizeMismatch", err)
	}

	// Reopening with the original size still works and preserves the data.
	w2, err := New[uint64](dir, marshalU64, unmarshalU64, Options{SegmentSize: 8192})
	if err != nil {
		t.Fatalf("reopen with original size: %v", err)
	}
	defer w2.Close()
	r := w2.NewReader()
	for want := uint64(0); want < 100; want++ {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("take: v=%d ok=%v err=%v want=%d", v, ok, err, want)
		}
	}
}

// TestConcurrentDrainCooperates verifies that two Drain iterations running
// concurrently on the same WAL split the stream without loss or duplication —
// safe now that Drain commits each item under the lock as it is read.
func TestConcurrentDrainCooperates(t *testing.T) {
	w, _ := openTest(t, 0)
	const n = 5000
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	var mu sync.Mutex
	seen := make(map[uint64]bool, n)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := w.NewReader()
			for v := range r.Drain(ctx) {
				mu.Lock()
				if seen[v] {
					t.Errorf("item %d delivered twice", v)
				}
				seen[v] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("saw %d distinct items, want %d", len(seen), n)
	}
	if !w.Empty() {
		t.Fatal("queue should be fully drained")
	}
}

// TestDrainCommitsBeforeYield verifies the at-most-once contract: the item the
// loop stops on has already been committed and does not replay.
func TestDrainCommitsBeforeYield(t *testing.T) {
	w, r := openTest(t, 0)
	for i := uint64(0); i < 10; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}

	// Break on the very first item. Because Drain commits before the body runs,
	// that item is already committed and must not be seen again.
	for range r.Drain(context.Background()) {
		break
	}

	v, ok, err := r.TryTake()
	if err != nil || !ok {
		t.Fatalf("next take: ok=%v err=%v", ok, err)
	}
	if v != 1 {
		t.Fatalf("after break-on-0 the next item is %d, want 1 (item 0 was committed)", v)
	}
}

// TestSyncEveryBatchedDurable exercises the batched sync policy: records written
// with SyncEvery>1 must survive a clean Close+reopen and stay in order.
func TestSyncEveryBatchedDurable(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SyncEvery: 64, SegmentSize: 4096}) // many records per flush, several segments
	if err != nil {
		t.Fatal(err)
	}
	const n = 5000
	for i := uint64(0); i < n; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	r := w.NewReader()
	for i := uint64(0); i < 2000; i++ { // consume+commit some, leaving a remainder
		if _, ok, err := r.TryTake(); err != nil || !ok {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := w.Close(); err != nil { // Close must flush the pending batch
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SyncEvery: 64, SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if got := w2.Count(); got != n-2000 {
		t.Fatalf("after reopen Count=%d, want %d", got, n-2000)
	}
	r2 := w2.NewReader()
	for want := uint64(2000); want < n; want++ {
		v, ok, err := r2.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("after reopen: v=%d ok=%v err=%v want=%d", v, ok, err, want)
		}
	}
	if !w2.Empty() {
		t.Fatal("should be drained")
	}
}

// TestSyncIntervalFlushes verifies that the background syncer durably persists
// batched writes within the interval, and that Close stops it cleanly.
func TestSyncIntervalFlushes(t *testing.T) {
	dir := t.TempDir()
	w, err := New[uint64](dir, marshalU64, unmarshalU64,
		Options{SyncEvery: 1 << 20, SyncInterval: 20 * time.Millisecond}) // batch huge; rely on the timer
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 100; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for the background syncer to flush (no explicit Sync/Close yet).
	time.Sleep(80 * time.Millisecond)

	// The unsynced counter should have been reset by a timer-driven flush.
	w.mu.Lock()
	unsynced := w.st.unsynced
	w.mu.Unlock()
	if unsynced != 0 {
		t.Fatalf("background syncer did not flush: unsynced=%d", unsynced)
	}
	if err := w.Close(); err != nil { // must stop the goroutine and not hang
		t.Fatal(err)
	}

	// Data is intact on reopen.
	w2, err := New[uint64](dir, marshalU64, unmarshalU64)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if got := w2.Count(); got != 100 {
		t.Fatalf("after reopen Count=%d, want 100", got)
	}
}
