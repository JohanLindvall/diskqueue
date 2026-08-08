package diskqueue

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Regression tests for the defects found in the correctness/API/performance
// review. Each one FAILED before its fix; the comment on each says what it is
// standing guard over, because that is the part a future refactor will not
// rediscover on its own.

// ---------------------------------------------------------------------------
// Recovery: what the open path concludes must reach the disk.
// ---------------------------------------------------------------------------

// hdrFields preads a segment's header and returns the four recovery-relevant
// fields, so a test can assert what a REOPEN would believe rather than what this
// process happens to hold in memory.
func hdrFields(t *testing.T, dir string, num uint64) (writeCursor, commitCursor, written, committed int64) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "data."+pad8(num)))
	if err != nil {
		t.Fatal(err)
	}
	u := func(i int) int64 { return int64(binary.LittleEndian.Uint64(b[i : i+8])) }
	return u(16), u(8), u(24), u(32)
}

func pad8(n uint64) string {
	s := strconv.FormatUint(n, 10)
	for len(s) < 8 {
		s = "0" + s
	}
	return s
}

// bigCodec frames each record at 109 bytes, so segment arithmetic in these tests
// is legible: a 4096-byte segment holds exactly 37 of them.
func bigCodec() (MarshalFunc[uint64], UnmarshalFunc[uint64]) {
	return func(dst []byte, v uint64) ([]byte, error) {
			dst = binary.LittleEndian.AppendUint64(dst, v)
			return append(dst, make([]byte, 92)...), nil
		}, func(b []byte) (uint64, error) {
			return binary.LittleEndian.Uint64(b), nil
		}
}

// A truncated segment's repair must republish the COMMIT cursor, not only the
// write cursor. Leaving the old one on disk is invisible until later appends grow
// the segment past it — and then the next open believes it and silently retires
// records that were acknowledged after the recovery.
func TestTruncationRepairRepublishesCommitCursor(t *testing.T) {
	dir := t.TempDir()
	m, u := bigCodec()
	opts := Options{NoSync: true, SegmentSize: 8192, MaxSegments: -1}

	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 30; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	r := w.NewReader()
	for i := 0; i < 20; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// A power cut leaves only the first 10 records.
	if err := os.Truncate(filepath.Join(dir, "data.00000001"), headerSize+10*109); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Sync(); err != nil {
		t.Fatal(err)
	}
	wc, cc, _, cn := hdrFields(t, dir, 1)
	if cc > wc {
		t.Fatalf("repaired header has commitCursor=%d past writeCursor=%d: a reopen will "+
			"believe the stale cursor once appends grow the segment past it", cc, wc)
	}
	if cn > 10 {
		t.Fatalf("repaired header claims %d committed of 10 surviving records", cn)
	}
	// 25 fresh, acknowledged records after a clean recovery.
	for i := uint64(110); i < 135; i++ {
		if err := w2.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	w3, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w3.Close() }()
	var got []uint64
	r3 := w3.NewReader()
	for i := 0; i < 200; i++ {
		v, ok, err := r3.TryTake()
		if err != nil {
			continue
		}
		if !ok {
			break
		}
		got = append(got, v)
	}
	if len(got) != 25 {
		t.Fatalf("delivered %d of 25 records acknowledged after the recovery (%v): "+
			"a stale on-disk commit cursor retired them silently", len(got), got)
	}
}

// load's second pass overrides a segment header that claims records the recovered
// global cursor says must replay. That correction has to be written back: once the
// segments ahead are reclaimed the stale header becomes the leading one and is
// believed outright.
func TestCommittedCountReconciliationIsPersisted(t *testing.T) {
	dir := t.TempDir()
	m, u := bigCodec()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}

	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 60; i++ { // 37 per segment -> 2 segments
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash residue: unordered header writeback left segment 2 claiming it is fully
	// committed while segment 1 still says nothing is.
	p := filepath.Join(dir, "data.00000002")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	h := make([]byte, headerSize)
	copy(h, b[:headerSize])
	binary.LittleEndian.PutUint64(h[8:16], binary.LittleEndian.Uint64(h[16:24]))
	binary.LittleEndian.PutUint64(h[32:40], binary.LittleEndian.Uint64(h[24:32]))
	binary.LittleEndian.PutUint64(h[56:64], xxhash.Sum64(h[:hdrSumCovered]))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(h, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	total := w2.Count()
	if total != 60 {
		t.Fatalf("Count=%d after recovery, want 60", total)
	}
	// Drain exactly segment 1 so it is reclaimed and segment 2 leads.
	r := w2.NewReader()
	for i := 0; i < 37; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
	}
	left := w2.Count()
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	w3, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w3.Close() }()
	if got := w3.Count(); got != left {
		t.Fatalf("session 2 closed cleanly holding %d records, session 3 sees %d: "+
			"an in-memory-only reconciliation let the stale header win", left, got)
	}
}

// Only the TAIL can be an unfinished create. A zero-length segment with a live
// sibling above it lost every byte it had, and unlinking it silently is exactly
// the unreported loss the design forbids.
func TestZeroLengthMiddleSegmentIsReported(t *testing.T) {
	dir := t.TempDir()
	m, u := bigCodec()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}

	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 150; i++ { // 37 per segment -> 5 segments
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(dir, "data.00000002"), 0); err != nil {
		t.Fatal(err)
	}

	w2, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()
	got, reports := 0, 0
	for i := 0; i < 500; i++ {
		_, ok, err := r.TryTake()
		if err != nil {
			if errors.Is(err, ErrCorrupt) {
				reports++
				continue
			}
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got++
	}
	if got == 150 {
		t.Skip("no records were lost; nothing to report")
	}
	s := w2.Stats()
	if s.Corruptions == 0 || s.LostSegments == 0 {
		t.Fatalf("%d records lost but Corruptions=%d LostSegments=%d: a middle segment "+
			"that lost every byte was unlinked as if it were an unfinished create",
			150-got, s.Corruptions, s.LostSegments)
	}
	if reports == 0 {
		t.Fatal("the loss was counted but never surfaced as an ErrCorrupt")
	}
}

// A tail-position zero-length segment IS an unfinished create and must stay
// silent — the other half of the rule above.
func TestZeroLengthTailSegmentIsSilent(t *testing.T) {
	dir := t.TempDir()
	m, u := bigCodec()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}
	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The stub a create interrupted between link and header write leaves behind.
	if err := os.WriteFile(filepath.Join(dir, "data.00000002"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w2, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	if s := w2.Stats(); s.Corruptions != 0 || s.LostSegments != 0 {
		t.Fatalf("an unfinished create at the tail was booked as damage: %+v", s)
	}
}

// Recovery addresses segments by NUMBER and rebuilds the name with %08d padding.
// An unpadded stray parses to a number whose padded file does not exist, and used
// to fail every later open with an ENOENT naming a file that never existed.
func TestStrayNumericFileDoesNotBlockOpen(t *testing.T) {
	for _, stray := range []string{"data.2024", "data.7", "data.007", "data.notanumber", "data."} {
		t.Run(stray, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{NoSync: true, SegmentSize: 4096}
			w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Add(42); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, stray), []byte("unrelated"), 0o644); err != nil {
				t.Fatal(err)
			}
			w2, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
			if err != nil {
				t.Fatalf("a stray file named %q locked the queue out: %v", stray, err)
			}
			defer func() { _ = w2.Close() }()
			v, ok, err := w2.NewReader().TryTake()
			if !ok || err != nil || v != 42 {
				t.Fatalf("record lost behind the stray: v=%d ok=%v err=%v", v, ok, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Framing arithmetic.
// ---------------------------------------------------------------------------

// fitsInRecord must bound the whole FRAME, not just the payload length. On a
// 32-bit build avail can exceed maxInt, so bounding only v let the caller's
// n+int(v)+checksumSize wrap negative and panic reslicing readBuf.
//
// Driven through fitsWithin so the 32-bit bound is exercised on ANY architecture:
// with the real maxInt this assertion is unfalsifiable on 64-bit, because avail is
// bounded by segmentSize and never approaches 2^63.
func TestFitsInRecordBoundsTheWholeFrame(t *testing.T) {
	for _, word := range []struct {
		bits   int
		maxInt uint64
	}{{32, 1<<31 - 1}, {64, 1<<63 - 1}} {
		for _, avail := range []int64{4096, 8 << 20, 1<<31 - 1, 1 << 31, 1 << 32, 1 << 40} {
			for _, v := range []uint64{
				0, 1, 7,
				uint64(avail), uint64(avail) - checksumSize, uint64(avail) - checksumSize - 1,
				word.maxInt - 1, word.maxInt, word.maxInt + 1,
				1<<31 - 1, 1 << 31, 1 << 32, 1<<63 - 1, 1<<64 - 1,
			} {
				for n := 1; n <= 10; n++ {
					if !fitsWithin(v, n, avail, word.maxInt) {
						continue
					}
					// Accepted, so all three must hold: the frame fits the segment,
					// the total is representable in the target word, and the payload
					// length itself is addressable.
					total := uint64(n) + v + checksumSize
					if total > uint64(avail) {
						t.Fatalf("%d-bit: fitsWithin(v=%d,n=%d,avail=%d) accepted a frame of %d",
							word.bits, v, n, avail, total)
					}
					if total > word.maxInt {
						t.Fatalf("%d-bit: fitsWithin(v=%d,n=%d,avail=%d) accepted total=%d, "+
							"which wraps negative when the caller narrows it to int",
							word.bits, v, n, avail, total)
					}
				}
			}
		}
	}
	// And the real guard must agree with the model for this machine's word size.
	const maxInt = uint64(^uint(0) >> 1)
	for _, avail := range []int64{4096, 1 << 20} {
		for v := uint64(0); v < 5000; v += 7 {
			for n := 1; n <= 3; n++ {
				if got, want := fitsInRecord(v, n, avail), fitsWithin(v, n, avail, maxInt); got != want {
					t.Fatalf("fitsInRecord(%d,%d,%d)=%v but fitsWithin says %v", v, n, avail, got, want)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Accounting.
// ---------------------------------------------------------------------------

// A record dropped for a bad checksum has been reported destroyed, so it must not
// keep the backlog non-empty forever. Nothing else would ever retire it: every
// consume op returns on the error without committing.
func TestCorruptTailRecordIsRetired(t *testing.T) {
	dir := t.TempDir()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}
	w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 3; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Corrupt only the PAYLOAD of the last record: its framing stays intact, so
	// exactly one record is dropped.
	f, err := os.OpenFile(filepath.Join(dir, "data.00000001"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xDE}, headerSize+2*17+1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	r := w2.NewReader()
	reports := 0
	for i := 0; i < 20; i++ {
		_, ok, err := r.TryTake()
		if err != nil {
			reports++
			continue
		}
		if !ok {
			break
		}
	}
	if reports != 1 {
		t.Fatalf("got %d ErrCorrupt reports, want exactly 1", reports)
	}
	if got := w2.Count(); got != 0 {
		t.Fatalf("Count=%d after the damaged record was reported lost: it stays in the "+
			"backlog forever and pins its segment", got)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	// And it must not be re-reported on every restart.
	w3, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w3.Close() }()
	if got := w3.Stats().Corruptions; got != 0 {
		t.Fatalf("reopen booked %d fresh corruptions for damage already reported", got)
	}
	if got := w3.Count(); got != 0 {
		t.Fatalf("reopen Count=%d, want 0", got)
	}
}

// With records reserved behind the cursor the commit position may NOT jump past a
// dropped record: that would retire work no consumer has acknowledged.
func TestCorruptRecordDoesNotRetireReservedWork(t *testing.T) {
	dir := t.TempDir()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}
	w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 4; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "data.00000001"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xDE}, headerSize+2*17+1); err != nil { // record 2
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()
	// Reserve records 0 and 1 without committing.
	for i := 0; i < 2; i++ {
		if _, ok, _, err := r.TryReserve(); !ok || err != nil {
			t.Fatalf("reserve %d: ok=%v err=%v", i, ok, err)
		}
	}
	if _, _, _, err := r.TryReserve(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt at record 2, got %v", err)
	}
	// Records 0 and 1 are still uncommitted, so they must still be in the backlog.
	if got := w2.Count(); got < 2 {
		t.Fatalf("Count=%d: the quarantine retired reserved-but-unacknowledged work", got)
	}
}

// Close flushes everything, so the power-loss exposure it leaves behind is zero —
// and Stats stays readable after Close, so it has to say so.
func TestUnsyncedBytesZeroAfterClose(t *testing.T) {
	for _, opts := range []Options{
		{NoSync: true, SegmentSize: 4096},
		{SyncEvery: 1000, SegmentSize: 4096},
	} {
		w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 20; i++ {
			if err := w.Add(i); err != nil {
				t.Fatal(err)
			}
		}
		if w.Stats().UnsyncedBytes == 0 {
			t.Fatalf("%+v: nothing accumulated, so the test proves nothing", opts)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if got := w.Stats().UnsyncedBytes; got != 0 {
			t.Errorf("%+v: UnsyncedBytes=%d after Close, want 0", opts, got)
		}
	}
}

// DiskBytes is maintained incrementally now rather than summed per call; it must
// still equal the sum of the live segments' capacities through cycles and
// reclamation, including an oversized segment.
func TestDiskBytesTracksTheLiveSegments(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	check := func(stage string) {
		t.Helper()
		var want int64
		for _, df := range w.st.files {
			want += headerSize + df.capacity
		}
		if got := w.Stats().DiskBytes; got != want {
			t.Fatalf("%s: DiskBytes=%d, want %d (%d segments)", stage, got, want, len(w.st.files))
		}
	}
	check("fresh")
	small := make([]byte, 100)
	for i := 0; i < 200; i++ { // many cycles
		if err := w.Add(small); err != nil {
			t.Fatal(err)
		}
	}
	check("after cycling")
	if err := w.Add(make([]byte, 9000)); err != nil { // an oversized segment
		t.Fatal(err)
	}
	check("after an oversized record")
	r := w.NewReader()
	for {
		_, ok, err := r.TryTake()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	check("after draining and reclaiming")
}

// ---------------------------------------------------------------------------
// The staged-span machinery.
// ---------------------------------------------------------------------------

// A latch discovered mid-batch must DISCARD the staged tail. Returning with
// pendingBytes still set left a span no leader would ever settle, so every later
// quiesce — Close included — blocked forever.
func TestAddBatchLatchDoesNotStrandStagedBytes(t *testing.T) {
	var w *Queue[uint64]
	marshal := func(dst []byte, v uint64) ([]byte, error) {
		if v == 3 && w != nil {
			// Latch the way evictOpen's discarded flushFile error does: inside the
			// batch loop, with the queue lock held.
			_ = w.st.failIO(errors.New("injected writeback failure"))
		}
		return binary.LittleEndian.AppendUint64(dst, v), nil
	}
	var err error
	w, err = New[uint64](t.TempDir(), marshal, unmarshalU64,
		Options{SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	n, berr := w.AddBatch([]uint64{0, 1, 2, 3, 4})
	if !errors.Is(berr, ErrIO) {
		t.Fatalf("AddBatch err=%v, want ErrIO", berr)
	}
	if st := w.st; st.pendingBytes != 0 || st.inFlight != 0 {
		t.Fatalf("staged tail stranded after the latch: pendingBytes=%d inFlight=%d "+
			"(n=%d) — every later quiesce will block forever", st.pendingBytes, st.inFlight, n)
	}
	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("DEADLOCK: Close did not return; the staged tail was never discarded")
	}
}

// AddBatch stages and publishes one span per segment on EVERY policy, so the
// deferred policies stop paying a header write per record. The observable
// contract must not change: every record lands, in order, and survives a reopen.
func TestAddBatchIsCorrectUnderEveryPolicy(t *testing.T) {
	for _, opts := range []Options{
		{SegmentSize: 4096, MaxSegments: -1},                // per-op
		{NoSync: true, SegmentSize: 4096, MaxSegments: -1},  // no fsync
		{SyncEvery: 8, SegmentSize: 4096, MaxSegments: -1},  // batched
		{SyncEvery: 64, SegmentSize: 4096, MaxSegments: -1}, // batched, coarse
	} {
		dir := t.TempDir()
		w, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
		if err != nil {
			t.Fatal(err)
		}
		const n = 500 // several segments' worth
		items := make([]uint64, n)
		for i := range items {
			items[i] = uint64(i)
		}
		got, aerr := w.AddBatch(items)
		if got != n || aerr != nil {
			t.Fatalf("%+v: AddBatch n=%d err=%v", opts, got, aerr)
		}
		if c := w.Count(); c != n {
			t.Fatalf("%+v: Count=%d, want %d", opts, c, n)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		w2, err := New[uint64](dir, marshalU64, unmarshalU64, opts)
		if err != nil {
			t.Fatalf("%+v: reopen: %v", opts, err)
		}
		want := uint64(0)
		for v := range w2.NewReader().Drain(context.Background()) {
			if v != want {
				t.Fatalf("%+v: out of order: got %d want %d", opts, v, want)
			}
			want++
		}
		if want != n {
			t.Fatalf("%+v: %d of %d records survived the reopen", opts, want, n)
		}
		if err := w2.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// SyncEvery counts WRITES. A published batch span covers many records with one
// header, but each record is still a write, so collapsing the span into a single
// tick would silently widen the power-loss window the option exists to bound.
func TestBatchedSyncEveryCountsRecordsNotSpans(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{SyncEvery: 10, SegmentSize: 1 << 20, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if n, err := w.AddBatch([]uint64{1, 2, 3, 4, 5}); n != 5 || err != nil {
		t.Fatalf("AddBatch: n=%d err=%v", n, err)
	}
	if got := w.st.unsynced; got != 5 {
		t.Fatalf("unsynced=%d after a 5-record batch, want 5", got)
	}
	if n, err := w.AddBatch([]uint64{6, 7, 8, 9, 10}); n != 5 || err != nil {
		t.Fatalf("AddBatch: n=%d err=%v", n, err)
	}
	if got := w.st.unsynced; got != 0 {
		t.Fatalf("unsynced=%d, want 0: 10 writes should have tripped SyncEvery=10", got)
	}
	if got := w.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d after the flush, want 0", got)
	}
}

// The quiesce barrier hands the lock to a waiter instead of letting a producer
// stream keep the flush leader looping. Its hazard is the opposite of starvation:
// a barrier that parks producers can deadlock. This asserts progress for every
// quiescing op under sustained concurrent load.
func TestQuiesceBarrierMakesProgressUnderProducers(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{SegmentSize: 1 << 20, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 50; i++ {
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for p := 0; p < 6; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = w.Add(1)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r := w.NewReader()
		for i := 0; i < 20; i++ {
			if err := w.Sync(); err != nil {
				t.Error(err)
				return
			}
			if _, err := w.AddBatch([]uint64{7, 8}); err != nil {
				t.Error(err)
				return
			}
			if _, err := r.Requeue(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		cancel()
		wg.Wait()
		t.Fatal("Sync/AddBatch/Requeue made no progress against 6 producers in 60s")
	}
	cancel()
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Coverage the suite was missing (behaviour was already correct).
// ---------------------------------------------------------------------------

// "dirty" means HAS UNSYNCED BYTES, not "is open". An evicted handle must keep
// the flag so a later Sync reopens the file and flushes it — otherwise
// UnsyncedBytes is a lie after Sync returns.
func TestEvictedDirtyFileIsFlushedBySync(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u,
		Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	payload := make([]byte, 500)
	for i := 0; i < 80; i++ { // many segments, only 3 handles allowed open
		if err := w.Add(payload); err != nil {
			t.Fatal(err)
		}
	}
	st := w.st
	if len(st.files) < 5 {
		t.Fatalf("only %d segments; the eviction path is not exercised", len(st.files))
	}
	if st.nOpen > 3 {
		t.Fatalf("nOpen=%d exceeds MaxOpenFiles=3", st.nOpen)
	}
	dirty, closed := 0, 0
	for _, df := range st.files {
		if df.dirty {
			dirty++
		}
		if df.f == nil {
			closed++
		}
	}
	if closed == 0 {
		t.Fatal("no handle was evicted; the test proves nothing")
	}
	if dirty != len(st.files) {
		t.Fatalf("%d of %d files dirty: eviction cleared the flag, so Sync will skip them",
			dirty, len(st.files))
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, df := range st.files {
		if df.dirty {
			t.Fatalf("segment %d still dirty after Sync", df.num)
		}
	}
	if got := w.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d after Sync, want 0", got)
	}
}

// Every consume op on an empty queue answers (false, nil) — the most ordinary
// answer in the API, and the one arm the suite never executed for four of them.
func TestEveryConsumeOpOnAnEmptyQueue(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()

	if _, ok, err := r.TryTake(); ok || err != nil {
		t.Errorf("TryTake: ok=%v err=%v", ok, err)
	}
	if _, ok, off, err := r.TryReserve(); ok || err != nil || off != 0 {
		t.Errorf("TryReserve: ok=%v off=%d err=%v", ok, off, err)
	}
	if _, ok, err := r.TryPeek(); ok || err != nil {
		t.Errorf("TryPeek: ok=%v err=%v", ok, err)
	}
	if ok, err := r.Skip(); ok || err != nil {
		t.Errorf("Skip: ok=%v err=%v", ok, err)
	}
	if ok, err := r.Requeue(); ok || err != nil {
		t.Errorf("Requeue: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok, _, err := r.Reserve(ctx); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Reserve on an empty queue: ok=%v err=%v", ok, err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if _, ok, err := r.Take(ctx2); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Take on an empty queue: ok=%v err=%v", ok, err)
	}
}

// A header whose checksum fails AND whose version is unknown takes the drop path,
// not the salvage path: the checksum case is matched first, and the salvage is
// gated on the version being known.
func TestTornHeaderWithUnknownVersionIsDropped(t *testing.T) {
	dir := t.TempDir()
	m, u := bigCodec()
	opts := Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1}
	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 120; i++ { // >= 3 segments
		if err := w.Add(i); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "data.00000002")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	h := make([]byte, headerSize)
	copy(h, b[:headerSize])
	h[40] = 99                                        // a version this build cannot read
	binary.LittleEndian.PutUint64(h[56:64], 0xBADBAD) // and a checksum that fails
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(h, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w2, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	s := w2.Stats()
	if s.LostSegments != 1 {
		t.Fatalf("LostSegments=%d, want 1: a bad checksum is matched before the version, "+
			"so this is damage rather than a foreign format", s.LostSegments)
	}
	if s.ForeignSegments != 0 {
		t.Fatalf("ForeignSegments=%d, want 0", s.ForeignSegments)
	}
	if s.Corruptions != 1 {
		t.Fatalf("Corruptions=%d, want 1", s.Corruptions)
	}
}

// Stats.Delivered has two arms that are easy to state backwards, and its godoc did
// state one of them backwards. A record whose COMMIT failed stays counted (the read
// happened, and TestDrainCommitFailureDoesNotYieldTheItem pins that); a record the
// CODEC rejected does not, because Reader.read puts it back at the head and
// rewindHead un-counts the delivery with it.
func TestDeliveredCountsReadsNotHandovers(t *testing.T) {
	boom := errors.New("codec says no")
	reject := true
	u := func(b []byte) (uint64, error) {
		if reject {
			return 0, boom
		}
		return unmarshalU64(b)
	}
	w, err := New[uint64](t.TempDir(), marshalU64, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(7); err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()

	// A codec rejection leaves the record at the head and must not count.
	if _, _, err := r.TryTake(); !errors.Is(err, ErrCodec) {
		t.Fatalf("TryTake: %v, want ErrCodec", err)
	}
	if got := w.Stats().Delivered; got != 0 {
		t.Fatalf("Delivered=%d after a codec rejection, want 0: the record was put back", got)
	}
	if got := w.Count(); got != 1 {
		t.Fatalf("Count=%d, want 1: a codec error must not consume the record", got)
	}
	// And a repeat rejection must not accumulate either.
	if _, _, err := r.TryTake(); !errors.Is(err, ErrCodec) {
		t.Fatalf("second TryTake: %v", err)
	}
	if got := w.Stats().Delivered; got != 0 {
		t.Fatalf("Delivered=%d after two codec rejections, want 0", got)
	}
	// Once the codec accepts it, the read counts.
	reject = false
	if v, ok, err := r.TryTake(); !ok || err != nil || v != 7 {
		t.Fatalf("TryTake after the codec recovers: v=%d ok=%v err=%v", v, ok, err)
	}
	if got := w.Stats().Delivered; got != 1 {
		t.Fatalf("Delivered=%d after a successful read, want 1", got)
	}
}

// The quiesce barrier, pinned deterministically rather than by a latency bar. The
// liveness test above proves the barrier does not deadlock; this one proves it
// EXISTS — with the park deleted, the Add below completes immediately and this
// fails, which is what "reproduced by a test that fails without its fix" requires.
func TestQuiesceBarrierParksProducers(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64,
		Options{SegmentSize: 1 << 20, MaxSegments: -1}) // per-op: addDurableLocked
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(0); err != nil { // warm the active file
		t.Fatal(err)
	}

	// Stand in for a goroutine inside waitFlushQuiescedLocked.
	w.mu.Lock()
	w.quiesceWant++
	w.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- w.Add(1) }()

	select {
	case err := <-done:
		t.Fatalf("Add returned (%v) while a quiesce was pending: producers are not "+
			"yielding, so Sync/AddBatch/Requeue/Close can be starved by a producer stream", err)
	case <-time.After(300 * time.Millisecond):
		// Parked, as it must be.
	}

	// Release, exactly as waitFlushQuiescedLocked's defer does.
	w.mu.Lock()
	w.quiesceWant--
	if w.quiesceWant == 0 && w.quiesceRelease != nil {
		close(w.quiesceRelease)
		w.quiesceRelease = nil
	}
	w.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Add after the quiesce cleared: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("DEADLOCK: the parked producer never resumed after the quiesce cleared")
	}
	if got := w.Count(); got != 2 {
		t.Fatalf("Count=%d, want 2", got)
	}
}

// AddBatch's amortization is the point of the unification, and nothing was guarding
// it: the correctness tests pass just as well against the old per-record append
// loop. Observe the staging directly — MarshalFunc runs under the queue mutex inside
// the staging loop, so it can see pendingBytes accumulating.
func TestAddBatchStagesOnEveryPolicy(t *testing.T) {
	for _, opts := range []Options{
		{SegmentSize: 1 << 20, MaxSegments: -1},                // per-op
		{NoSync: true, SegmentSize: 1 << 20, MaxSegments: -1},  // deferred
		{SyncEvery: 64, SegmentSize: 1 << 20, MaxSegments: -1}, // batched
	} {
		var w *Queue[uint64]
		sawStaged := 0
		m := func(dst []byte, v uint64) ([]byte, error) {
			if w != nil && w.st.pendingBytes > 0 {
				sawStaged++
			}
			return binary.LittleEndian.AppendUint64(dst, v), nil
		}
		var err error
		w, err = New[uint64](t.TempDir(), m, unmarshalU64, opts)
		if err != nil {
			t.Fatal(err)
		}
		items := make([]uint64, 16)
		for i := range items {
			items[i] = uint64(i)
		}
		if n, aerr := w.AddBatch(items); n != len(items) || aerr != nil {
			t.Fatalf("%+v: AddBatch n=%d err=%v", opts, n, aerr)
		}
		// Items 2..16 must each have observed the previous ones staged but not yet
		// published. A per-record append loop publishes as it goes and never stages.
		if sawStaged < len(items)-1 {
			t.Errorf("%+v: only %d of %d items saw a staged predecessor — AddBatch is "+
				"not amortizing the header write on this policy", opts, sawStaged, len(items)-1)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// SyncEvery bounds the unsynced WRITES, and that bound has to hold inside a batch:
// publishing a whole batch as one span would make the peak exposure len(items).
//
// The quantity to assert is the PEAK mid-batch, not the state afterwards — the final
// publish trips the counter and flushes either way, so an end-state assertion here is
// vacuous (it was, on the first attempt).
func TestAddBatchRespectsSyncEveryInsideTheBatch(t *testing.T) {
	const every = 8
	const items = 100
	var w *Queue[uint64]
	peak := 0
	m := func(dst []byte, v uint64) ([]byte, error) {
		// Runs under the queue mutex inside the staging loop, so it sees the window
		// as it grows: records staged but not yet covered by a flush.
		if w != nil {
			if n := w.st.unsynced + int(w.st.pendingRecs); n > peak {
				peak = n
			}
		}
		return binary.LittleEndian.AppendUint64(dst, v), nil
	}
	var err error
	w, err = New[uint64](t.TempDir(), m, unmarshalU64,
		Options{SyncEvery: every, SegmentSize: 1 << 20, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	batch := make([]uint64, items)
	for i := range batch {
		batch[i] = uint64(i)
	}
	if n, aerr := w.AddBatch(batch); n != items || aerr != nil {
		t.Fatalf("AddBatch: n=%d err=%v", n, aerr)
	}
	t.Logf("peak unsynced writes during the batch: %d (SyncEvery=%d, batch=%d)", peak, every, items)
	if peak > every {
		t.Fatalf("peak unsynced writes reached %d inside a %d-record batch at SyncEvery=%d: "+
			"the batch never flushed inside itself, so the power-loss window was the whole "+
			"batch rather than the %d writes Options.SyncEvery promises", peak, items, every, every)
	}
}

// The oversized-release rule has to apply on the codec-error exits too, or one huge
// record the codec rejects pins its buffer for the Reader's lifetime.
func TestScratchReleasedOnCodecError(t *testing.T) {
	big := make([]byte, 9000) // larger than the 4096 segment: an oversized record
	m := func(dst []byte, v uint64) ([]byte, error) { return append(dst, big...), nil }
	boom := errors.New("nope")
	u := func(b []byte) (uint64, error) { return 0, boom }
	w, err := New[uint64](t.TempDir(), m, u, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		run  func(*Reader[uint64]) error
	}{
		{"TryTake", func(r *Reader[uint64]) error { _, _, e := r.TryTake(); return e }},
		{"TryPeek", func(r *Reader[uint64]) error { _, _, e := r.TryPeek(); return e }},
	} {
		r := w.NewReader()
		if e := tc.run(r); !errors.Is(e, ErrCodec) {
			t.Fatalf("%s: %v, want ErrCodec", tc.name, e)
		}
		if got := int64(cap(r.scratch)); got > w.st.segmentSize {
			t.Errorf("%s: Reader scratch still %d bytes after a codec rejection of an "+
				"oversized record (segmentSize=%d): the buffer is pinned for the "+
				"Reader's lifetime", tc.name, got, w.st.segmentSize)
		}
	}
}

// AddBatch is the one entry point that runs the caller's MarshalFunc under the queue
// mutex, so a panicking codec unwinds THROUGH the staging loop, past every publish
// and discard site. Left staged, that tail is a span no leader will ever settle —
// and on the deferred policies there is no leader at all — so every later quiesce,
// Close included, blocks forever on a store the caller can still Add to.
func TestAddBatchPanicDoesNotStrandStagedBytes(t *testing.T) {
	for _, opts := range []Options{
		{NoSync: true, SegmentSize: 1 << 20, MaxSegments: -1},
		{SyncEvery: 8, SegmentSize: 1 << 20, MaxSegments: -1},
		{SegmentSize: 1 << 20, MaxSegments: -1}, // per-op
	} {
		m := func(dst []byte, v uint64) ([]byte, error) {
			if v == 3 {
				panic("codec exploded")
			}
			return binary.LittleEndian.AppendUint64(dst, v), nil
		}
		w, err := New[uint64](t.TempDir(), m, unmarshalU64, opts)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%+v: the codec panic did not reach the caller", opts)
				}
			}()
			_, _ = w.AddBatch([]uint64{0, 1, 2, 3, 4})
		}()
		if st := w.st; st.pendingBytes != 0 || st.inFlight != 0 {
			t.Errorf("%+v: staged tail stranded after the panic: pendingBytes=%d inFlight=%d",
				opts, st.pendingBytes, st.inFlight)
		}
		// The store must still be usable and, above all, closable.
		if err := w.Add(99); err != nil {
			t.Errorf("%+v: Add after the panic: %v", opts, err)
		}
		done := make(chan error, 1)
		go func() { done <- w.Close() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%+v: Close after the panic: %v", opts, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("%+v: DEADLOCK — Close never returned after a codec panic left a "+
				"staged tail; the directory flock is held forever", opts)
		}
	}
}
