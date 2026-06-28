package diskqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// These tests fault-inject on disk between close and reopen — the residue a power
// loss leaves — to pin down the data-then-header flush invariant. The store
// flushes a record's bytes before the header that publishes them, so a crash can
// leave the header either lagging the data (extra bytes on disk, header unaware)
// or — if that ordering were ever violated — leading it (header advertises a
// record whose bytes never landed). The first must truncate cleanly; the second
// must be caught by the per-record checksum, never returned as a real record.

// forgeHeader rewrites segment num's 64-byte header on disk: it applies mutate,
// then recomputes the header checksum so the header still validates on open. This
// produces an internally consistent header that disagrees with the record region,
// which is what a crash mid-flush can leave behind.
func forgeHeader(t *testing.T, dir string, num uint64, mutate func(h []byte)) {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("data.%08d", num))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < headerSize {
		t.Fatalf("short file: %d bytes", len(b))
	}
	mutate(b[:headerSize])
	binary.LittleEndian.PutUint64(b[56:64], xxhash.Sum64(b[:hdrSumCovered]))
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// idxRecLen is the on-disk size of an idxRec(i) record: uvarint length prefix,
// the 2-byte payload, and the 8-byte checksum trailer.
var idxRecLen = int64(uvarintLen(2) + 2 + checksumSize)

// TestStoreHeaderOverpromiseCaughtByChecksum forges a header that advertises one
// more record than was durably written (its bytes are still preallocated zeros) —
// the exact residue a header-before-data flush would leave after a crash. The open
// succeeds (the header validates), the real records read back, and the phantom is
// surfaced as ErrCorrupt rather than returned as a bogus record.
func TestStoreHeaderOverpromiseCaughtByChecksum(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	_, w, written, _ := readFileHeader(t, dir, 1)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[16:24], uint64(w+idxRecLen)) // write cursor past real data
		binary.LittleEndian.PutUint64(h[24:32], uint64(written+1))   // claim one extra record
	})

	s2, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatalf("reopen (header is valid, so open must succeed): %v", err)
	}
	defer s2.close()
	if got := s2.count(); got != n+1 {
		t.Fatalf("count=%d, want %d (header over-promises by one)", got, n+1)
	}
	for i := 0; i < n; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("real record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		s2.commitTo(off)
	}
	if _, _, _, err := s2.takeHead(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("phantom record: err=%v, want ErrCorrupt", err)
	}
}

// TestStoreHeaderOverpromiseRecovered is the recoverCorrupt counterpart: instead
// of surfacing ErrCorrupt forever, the phantom segment tail is quarantined, the
// queue drains, and the event is counted.
func TestStoreHeaderOverpromiseRecovered(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	_, w, written, _ := readFileHeader(t, dir, 1)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	forgeHeader(t, dir, 1, func(h []byte) {
		binary.LittleEndian.PutUint64(h[16:24], uint64(w+idxRecLen))
		binary.LittleEndian.PutUint64(h[24:32], uint64(written+1))
	})

	s2, err := openStore(dir, 4096, 0, false, 0, 0, true) // recoverCorrupt
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.close()
	for i := 0; i < n; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("real record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		s2.commitTo(off)
	}
	// The phantom is skipped, not returned; the store then reads empty.
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("after phantom: ok=%v err=%v, want empty/no-error", ok, err)
	}
	if got := s2.corruptionCount(); got != 1 {
		t.Fatalf("corruptionCount=%d, want 1", got)
	}
}

// TestStoreDataBeyondHeaderInvisible forges the header the other way: it rolls the
// cursor back so a record whose bytes are on disk is no longer published — the
// benign residue of data-then-header when the header flush is lost after the data
// flush. The orphaned bytes must be invisible (clean truncation), with no error.
func TestStoreDataBeyondHeaderInvisible(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 4
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	forgeHeader(t, dir, 1, func(h []byte) {
		w := int64(binary.LittleEndian.Uint64(h[16:24]))
		written := int64(binary.LittleEndian.Uint64(h[24:32]))
		binary.LittleEndian.PutUint64(h[16:24], uint64(w-idxRecLen)) // drop the last record from the cursor
		binary.LittleEndian.PutUint64(h[24:32], uint64(written-1))
	})

	s2, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.close()
	if got := s2.count(); got != n-1 {
		t.Fatalf("count=%d, want %d (last record orphaned beyond the cursor)", got, n-1)
	}
	for i := 0; i < n-1; i++ {
		p, off, ok, err := s2.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		s2.commitTo(off)
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("orphaned tail: ok=%v err=%v, want clean empty", ok, err)
	}
	if !s2.empty() {
		t.Fatal("store should be drained after the truncated records")
	}
}

// TestStoreTornRecordTailBatched simulates a power loss between batched flushes:
// records are written with SyncEvery batching, then the last record's payload is
// scribbled on disk. Strict open surfaces the tear as ErrCorrupt on read; with
// recovery the prefix survives and the torn tail is dropped.
func TestStoreTornRecordTailBatched(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 8, 0, false) // syncEvery=8 (batched)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	_, w, _, _ := readFileHeader(t, dir, 1)
	if err := s.close(); err != nil { // Close flushes the pending batch
		t.Fatal(err)
	}

	// Corrupt the last record's payload byte (its checksum no longer matches),
	// leaving the header — which already counts all n records — untouched.
	path := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lastPayload := int(w-idxRecLen) + uvarintLen(2) // start of the last record's 2-byte payload
	b[lastPayload] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// Strict: the prefix reads, then the torn record is ErrCorrupt. Read without
	// committing so the on-disk state is untouched for the recovery reopen below
	// (a flushed commit cursor would otherwise skip the prefix on the next open).
	strict, err := openStore(dir, 4096, 0, false, 8, 0, false)
	if err != nil {
		t.Fatalf("strict reopen: %v", err)
	}
	for i := 0; i < n-1; i++ {
		p, _, ok, err := strict.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("strict record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
	}
	if _, _, _, err := strict.takeHead(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("strict torn tail: err=%v, want ErrCorrupt", err)
	}
	_ = strict.close()

	// Recovery: prefix survives, torn record dropped, one event counted.
	rec, err := openStore(dir, 4096, 0, false, 8, 0, true)
	if err != nil {
		t.Fatalf("recover reopen: %v", err)
	}
	defer rec.close()
	for i := 0; i < n-1; i++ {
		p, off, ok, err := rec.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("recover record %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		rec.commitTo(off)
	}
	if _, _, ok, err := rec.takeHead(); ok || err != nil {
		t.Fatalf("recover after tear: ok=%v err=%v, want clean empty", ok, err)
	}
	if got := rec.corruptionCount(); got != 1 {
		t.Fatalf("corruptionCount=%d, want 1", got)
	}
}
