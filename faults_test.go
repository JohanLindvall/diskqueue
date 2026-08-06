//go:build diskqueue_faults

package diskqueue

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Run with: go test -tags diskqueue_faults ./...
//
// These are the tests that cannot be written any other way. The durability
// argument for the per-record policy rests entirely on append issuing its two
// fsyncs in a particular order relative to the header write, and on what each
// arm does when its fsync fails. No descriptor trick produces "this fsync fails
// while the write before it succeeded", so the injection points in append are
// the only way to reach them.
//
// CLAUDE.md used to claim the recovery-fault tests pinned this. They do not:
// those forge on-disk residue and check what a reopen makes of it, which is a
// different property. Nothing observed the ordering itself, and removing one
// fsync makes the suite noticeably faster — an attractive-looking change for a
// future contributor to make.

// injectAt fails exactly once at the named point, and records the order in which
// every point was reached.
func injectAt(t *testing.T, fail string, err error) *[]string {
	t.Helper()
	var seen []string
	fired := false
	faultHook = func(name string) error {
		seen = append(seen, name)
		if name == fail && !fired {
			fired = true
			return err
		}
		return nil
	}
	t.Cleanup(func() { faultHook = nil })
	return &seen
}

var errInjected = errors.New("injected fault")

// TestAppendOrdersDataBeforeHeader is the invariant itself: the record's bytes
// are synced before the header that publishes them is written. Get this backwards
// and a power loss can leave a header advertising a record whose payload never
// landed — the one residue the per-record checksum has to catch instead of a
// clean truncation.
func TestAppendOrdersDataBeforeHeader(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0) // per-op policy: fsync per record
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()

	seen := injectAt(t, "", nil) // record only
	mustAppend(t, s, idxRec(0))

	want := []string{"append.writeRecord", "append.syncData", "append.writeHeader", "append.syncHeader"}
	if len(*seen) != len(want) {
		t.Fatalf("append passed %v, want %v", *seen, want)
	}
	for i, w := range want {
		if (*seen)[i] != w {
			t.Fatalf("append order %v, want %v", *seen, want)
		}
	}
}

// TestAppendNoDoubleSyncWhenBatched: the second fsync is the per-op policy's
// price, and the batched policy must not be paying it.
func TestAppendNoDoubleSyncWhenBatched(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 100, 0) // SyncEvery: 100
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()

	seen := injectAt(t, "", nil)
	mustAppend(t, s, idxRec(0))
	for _, name := range *seen {
		if name == "append.syncData" || name == "append.syncHeader" {
			t.Fatalf("batched append reached %s: it should defer every fsync to the batch", name)
		}
	}
}

// TestDataSyncFailureAppendsNothing: the record's bytes could not be made
// durable, so nothing is published. Add reports it, the queue is unchanged, and a
// reopen has never heard of the record.
func TestDataSyncFailureAppendsNothing(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	before := s.count()

	injectAt(t, "append.syncData", errInjected)
	err = s.append(idxRec(1))
	if !errors.Is(err, ErrIO) {
		t.Fatalf("append with a failing data fsync: %v, want ErrIO", err)
	}
	if got := s.count(); got != before {
		t.Fatalf("count=%d, want %d: a refused append must not land", got, before)
	}
	if err := s.close(); err != nil && !errors.Is(err, ErrIO) {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != before {
		t.Fatalf("count=%d after reopen, want %d: the refused record became visible", got, before)
	}
}

// TestHeaderSyncFailurePublishesTheRecord is the other arm, and the asymmetry is
// deliberate: by this point the header is in the page cache, so the record is
// real to everything short of a power loss. Add reports the durability failure
// AND the record is in the log — which is exactly what Add's doc comment
// promises, and the reason it says so.
func TestHeaderSyncFailurePublishesTheRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	before := s.count()

	injectAt(t, "append.syncHeader", errInjected)
	if err := s.append(idxRec(7)); !errors.Is(err, ErrIO) {
		t.Fatalf("append with a failing header fsync: %v, want ErrIO", err)
	}
	if got := s.count(); got != before+1 {
		t.Fatalf("count=%d, want %d: the record reached the page cache and must stay", got, before+1)
	}
	p, _, ok, rerr := s.takeHead()
	if rerr != nil || !ok || recIdx(p) != 7 {
		t.Fatalf("reading the record back: idx=%d ok=%v err=%v", recIdx(p), ok, rerr)
	}
}

// TestHeaderWriteFailureRollsBack: the header never reached even the page cache,
// so the record is invisible to a reopen and the in-memory view must match. This
// is the arm that keeps "Add failed" from meaning "Add failed but it is queued",
// and unlike the fsync arms it must NOT poison the store — a failed pwrite is
// retriable.
func TestHeaderWriteFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	mustAppend(t, s, idxRec(0))
	count, size, woff := s.count(), s.size(), s.writeOffset()

	injectAt(t, "append.writeHeader", errInjected)
	if err := s.append(idxRec(1)); !errors.Is(err, errInjected) {
		t.Fatalf("append with a failing header write: %v, want the injected error", err)
	}
	if errors.Is(s.failure(), ErrIO) {
		t.Fatal("a failed header WRITE must not poison the store: it is retriable")
	}
	if s.count() != count || s.size() != size || s.writeOffset() != woff {
		t.Fatalf("rollback incomplete: count=%d/%d size=%d/%d off=%d/%d",
			s.count(), count, s.size(), size, s.writeOffset(), woff)
	}
	// And the retry succeeds, into the same space.
	faultHook = nil
	mustAppend(t, s, idxRec(1))
	if got := s.count(); got != count+1 {
		t.Fatalf("count=%d after the retry, want %d", got, count+1)
	}
	for i := 0; i < 2; i++ {
		p, off, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		if err := s.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWriteRecordFailureAppendsNothing: the first arm. Nothing was advanced and
// nothing published, so the store is untouched and the error is retriable.
func TestWriteRecordFailureAppendsNothing(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	mustAppend(t, s, idxRec(0))
	count, woff := s.count(), s.writeOffset()

	injectAt(t, "append.writeRecord", errInjected)
	if err := s.append(idxRec(1)); !errors.Is(err, errInjected) {
		t.Fatalf("append with a failing record write: %v", err)
	}
	if s.count() != count || s.writeOffset() != woff {
		t.Fatalf("a failed record write moved the cursors: count=%d/%d off=%d/%d",
			s.count(), count, s.writeOffset(), woff)
	}
	if errors.Is(s.failure(), ErrIO) {
		t.Fatal("a failed record write must not poison the store")
	}
}

// TestCrashBetweenDataSyncAndHeader simulates the power loss the ordering exists
// for: the record's bytes are durable, the header that publishes them is not.
// The reopen must see a clean truncation — no error, no phantom record.
func TestCrashBetweenDataSyncAndHeader(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}

	// Fail at the header write: the record bytes are on disk and fsynced, the
	// header still points before them. That is precisely the residue a crash
	// between the two leaves.
	injectAt(t, "append.writeHeader", errInjected)
	if err := s.append(idxRec(99)); !errors.Is(err, errInjected) {
		t.Fatalf("append: %v", err)
	}
	faultHook = nil
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen after a lost header write: %v, want a clean truncation", err)
	}
	defer func() { _ = s2.close() }()
	if got := s2.count(); got != n {
		t.Fatalf("count=%d, want %d: the unpublished record must be invisible", got, n)
	}
	if got := s2.corruptionCount(); got != 0 {
		t.Fatalf("corruptions=%d: a lost header write is a clean truncation, not damage", got)
	}
	for i := 0; i < n; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		if err := s2.commitTo(off); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("after the truncation point: ok=%v err=%v, want a clean empty", ok, err)
	}
	// The orphaned bytes really are on disk — this is not a test that passes
	// because nothing was ever written.
	fi, err := os.Stat(filepath.Join(dir, "data.00000001"))
	if err != nil || fi.Size() != headerSize+4096 {
		t.Fatalf("segment stat: size=%v err=%v", fi.Size(), err)
	}
}

// The group-commit path (Queue.Add under the per-op policy) and the batch path
// (Queue.AddBatch) restate append's ordering invariant span-wide: every staged
// record's bytes are fsync'd before the header that publishes the span is
// written. The same injection point names fire in the same order, so the
// contract is pinned identically for all three shapes of durable append.

// TestGroupAppendOrdersDataBeforeHeader: a solo Add through the Queue leads a
// span of one, and the four points pass in exactly the solo order.
func TestGroupAppendOrdersDataBeforeHeader(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	seen := injectAt(t, "", nil)
	if err := w.Add(7); err != nil {
		t.Fatal(err)
	}
	want := []string{"append.writeRecord", "append.syncData", "append.writeHeader", "append.syncHeader"}
	if len(*seen) != len(want) {
		t.Fatalf("group append passed %v, want %v", *seen, want)
	}
	for i, s := range want {
		if (*seen)[i] != s {
			t.Fatalf("group append order %v, want %v", *seen, want)
		}
	}
}

// TestBatchOrdersDataBeforeHeader: the batch writes every record first, then
// one data fsync, one header write, one header fsync — the amortization the
// method exists for, with the ordering intact.
func TestBatchOrdersDataBeforeHeader(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	seen := injectAt(t, "", nil)
	if n, err := w.AddBatch([]uint64{1, 2, 3}); n != 3 || err != nil {
		t.Fatalf("AddBatch: n=%d err=%v", n, err)
	}
	want := []string{
		"append.writeRecord", "append.writeRecord", "append.writeRecord",
		"append.syncData", "append.writeHeader", "append.syncHeader",
	}
	if len(*seen) != len(want) {
		t.Fatalf("batch passed %v, want %v", *seen, want)
	}
	for i, s := range want {
		if (*seen)[i] != s {
			t.Fatalf("batch order %v, want %v", *seen, want)
		}
	}
}

// TestGroupHeaderWriteFailureRollsBack: the span's header never reached the
// page cache, so nothing is published, nothing latches, and the retry lands in
// the same space — the solo arm's contract, span-wide.
func TestGroupHeaderWriteFailureRollsBack(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}

	injectAt(t, "append.writeHeader", errInjected)
	if err := w.Add(1); !errors.Is(err, errInjected) {
		t.Fatalf("Add with a failing header write: %v, want the injected error", err)
	}
	if w.Err() != nil {
		t.Fatalf("Err=%v: a failed header WRITE must not poison the queue", w.Err())
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: the failed span must not be visible", got)
	}
	faultHook = nil
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	for i := uint64(0); i < 2; i++ {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != i {
			t.Fatalf("record %d: v=%d ok=%v err=%v", i, v, ok, err)
		}
	}
}

// TestGroupDataSyncFailureLatches: the span's bytes may already be gone, so
// nothing is published and the store poisons — and the record is NOT in the
// queue, exactly as a solo append's data-fsync arm promises.
func TestGroupDataSyncFailureLatches(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(0); err != nil {
		t.Fatal(err)
	}

	injectAt(t, "append.syncData", errInjected)
	if err := w.Add(1); !errors.Is(err, ErrIO) {
		t.Fatalf("Add with a failing data fsync: %v, want ErrIO", err)
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: the unfsync'd record must not be visible", got)
	}
	if !errors.Is(w.Err(), ErrIO) {
		t.Fatalf("Err=%v, want the latched ErrIO", w.Err())
	}
}

// TestGroupHeaderSyncFailureKeepsRecord: the header reached the page cache, so
// the record is real to everything short of a power loss — it stays, the error
// reports the durability gap, and the store poisons.
func TestGroupHeaderSyncFailureKeepsRecord(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	injectAt(t, "append.syncHeader", errInjected)
	if err := w.Add(7); !errors.Is(err, ErrIO) {
		t.Fatalf("Add with a failing header fsync: %v, want ErrIO", err)
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: the page-cache-published record stays", got)
	}
	v, ok, err := w.NewReader().TryTake()
	if !ok || v != 7 {
		t.Fatalf("reading the record back: v=%d ok=%v err=%v", v, ok, err)
	}
}

// TestBatchDataSyncFailureLatches: the batch's span could not be made durable,
// so none of it is published — n says zero, the error says latched.
func TestBatchDataSyncFailureLatches(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	injectAt(t, "append.syncData", errInjected)
	n, berr := w.AddBatch([]uint64{1, 2, 3})
	if n != 0 || !errors.Is(berr, ErrIO) {
		t.Fatalf("AddBatch with a failing data fsync: n=%d err=%v, want 0 and ErrIO", n, berr)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("Count=%d, want 0: an unpublished span must not be visible", got)
	}
}

// TestBatchHeaderWriteFailureDiscards: the span's header never reached the
// page cache — the whole staged batch is discarded, nothing latches, and the
// retry succeeds into the same space.
func TestBatchHeaderWriteFailureDiscards(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	injectAt(t, "append.writeHeader", errInjected)
	n, berr := w.AddBatch([]uint64{1, 2, 3})
	if n != 0 || !errors.Is(berr, errInjected) {
		t.Fatalf("AddBatch with a failing header write: n=%d err=%v, want 0 and the injected error", n, berr)
	}
	if w.Err() != nil {
		t.Fatalf("Err=%v: a failed header WRITE must not poison the queue", w.Err())
	}
	faultHook = nil
	n, berr = w.AddBatch([]uint64{1, 2, 3})
	if n != 3 || berr != nil {
		t.Fatalf("retry: n=%d err=%v", n, berr)
	}
	r := w.NewReader()
	for _, want := range []uint64{1, 2, 3} {
		v, ok, err := r.TryTake()
		if err != nil || !ok || v != want {
			t.Fatalf("record %d: v=%d ok=%v err=%v", want, v, ok, err)
		}
	}
}

// TestBatchHeaderSyncFailureKeepsRecords: the header reached the page cache,
// so the span's records are real short of a power loss — n counts them, the
// latched error carries the durability gap.
func TestBatchHeaderSyncFailureKeepsRecords(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{SegmentSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	injectAt(t, "append.syncHeader", errInjected)
	n, berr := w.AddBatch([]uint64{1, 2, 3})
	if n != 3 || !errors.Is(berr, ErrIO) {
		t.Fatalf("AddBatch with a failing header fsync: n=%d err=%v, want 3 and ErrIO", n, berr)
	}
	if got := w.Count(); got != 3 {
		t.Fatalf("Count=%d, want 3: page-cache-published records stay", got)
	}
	r := w.NewReader()
	for _, want := range []uint64{1, 2, 3} {
		v, ok, err := r.TryTake()
		if !ok || v != want {
			t.Fatalf("record %d: v=%d ok=%v err=%v", want, v, ok, err)
		}
	}
}
