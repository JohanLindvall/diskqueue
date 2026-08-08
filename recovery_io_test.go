//go:build diskqueue_faults

package diskqueue

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
)

func bigM() MarshalFunc[uint64]   { m, _ := bigCodec(); return m }
func bigU() UnmarshalFunc[uint64] { _, u := bigCodec(); return u }

// Recovery reads records in exactly two licensed places, and both are reached only
// for a segment already known to be damaged: surviveCount walks a truncated segment,
// countVerified walks a torn-header one. Both walk through readBlock, so a bad
// sector can fail them PART WAY — after some frames have been proven and before the
// rest have.
//
// That case has no natural seam (no descriptor trick makes a pread fail mid-walk
// while the file stays intact), which is what the diskqueue_faults build is for.
// Both tests below failed against the first version of these fixes: one silently
// over-promised a backlog it could never drain, the other UNLINKED a segment full of
// verified records.

// failReadsAfter fails the Nth and every later read.block, so a walk can be cut off
// mid-run rather than refused outright.
func failReadsAfter(t *testing.T, n int, err error) {
	t.Helper()
	seen := 0
	faultHook = func(name string) error {
		if name != "read.block" {
			return nil
		}
		seen++
		if seen > n {
			return err
		}
		return nil
	}
	t.Cleanup(func() { faultHook = nil })
}

func manyRecords(t *testing.T, dir string, n int) Options {
	t.Helper()
	// 128 KiB segments so the walk crosses several 64 KiB readAhead blocks and a
	// mid-walk failure is reachable.
	opts := Options{NoSync: true, SegmentSize: 128 << 10, MaxSegments: -1}
	m, u := bigCodec()
	w, err := New[uint64](dir, m, u, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := w.Add(uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return opts
}

// A read error during the truncation walk must FAIL THE OPEN. Skipping the survivor
// correction is not a safe default: df.size would keep the raw truncation point,
// leaving a partial frame inside the published extent for the repair to republish
// and the next append to land behind — destroying records accepted afterwards.
func TestTruncationWalkIOErrorFailsTheOpen(t *testing.T) {
	dir := t.TempDir()
	const n = 700
	opts := manyRecords(t, dir, n)

	// A power cut cuts the segment mid-frame — above the 64 KiB readAhead, so the
	// surviving region needs more than one block and the walk can be cut off after
	// it has already proven some frames.
	p := filepath.Join(dir, "data.00000001")
	if err := os.Truncate(p, headerSize+70003); err != nil {
		t.Fatal(err)
	}

	failReadsAfter(t, 1, errors.New("injected EIO"))
	_, err := New[uint64](dir, bigM(), bigU(), opts)
	if err == nil {
		t.Fatal("open SUCCEEDED after the truncation walk failed mid-run: recovery " +
			"published an extent it never verified, and the next append will land " +
			"behind a partial frame")
	}
	t.Logf("open refused, as it must: %v", err)

	// Nothing may have been deleted, and a retry once the error clears must work.
	if _, serr := os.Stat(p); serr != nil {
		t.Fatalf("the segment was removed on a failed read: %v", serr)
	}
	faultHook = nil
	w, rerr2 := New[uint64](dir, bigM(), bigU(), opts)
	if rerr2 != nil {
		t.Fatalf("retry after the transient error cleared: %v", rerr2)
	}
	defer func() { _ = w.Close() }()
	got := 0
	r := w.NewReader()
	for i := 0; i < n+50; i++ {
		_, ok, rerr := r.TryTake()
		if rerr != nil {
			continue
		}
		if !ok {
			break
		}
		got++
	}
	t.Logf("retry recovered %d of %d records", got, n)
	if got == 0 {
		t.Fatal("the retry recovered nothing")
	}
}

// A read error during the SALVAGE walk must not unlink the segment. salvageTornHeader
// answers ok=false when the walk could not run, and loadFile's response to ok=false is
// dropSegment -> os.Remove; folding an I/O failure into that answer deletes a segment
// full of checksum-verified records on the strength of a read that failed.
func TestSalvageWalkIOErrorDoesNotUnlinkTheSegment(t *testing.T) {
	dir := t.TempDir()
	const n = 700
	opts := manyRecords(t, dir, n)

	// Tear the header: magic and version intact, checksum bad — the salvage path.
	p := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	h := make([]byte, headerSize)
	copy(h, b[:headerSize])
	binary.LittleEndian.PutUint64(h[56:64], xxhash.Sum64(h[:hdrSumCovered])^0xDEADBEEF)
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(h, 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	failReadsAfter(t, 1, errors.New("injected EIO"))
	_, err = New[uint64](dir, bigM(), bigU(), opts)
	if err == nil {
		t.Fatal("open SUCCEEDED after the salvage walk failed mid-run")
	}
	t.Logf("open refused, as it must: %v", err)

	if _, serr := os.Stat(p); serr != nil {
		t.Fatalf("THE SEGMENT WAS UNLINKED because a read failed: %v — a transient error "+
			"destroyed records that were verified and durable", serr)
	}

	faultHook = nil
	w, err := New[uint64](dir, bigM(), bigU(), opts)
	if err != nil {
		t.Fatalf("retry after the transient error cleared: %v", err)
	}
	defer func() { _ = w.Close() }()
	got := 0
	r := w.NewReader()
	for i := 0; i < n+50; i++ {
		_, ok, rerr := r.TryTake()
		if rerr != nil {
			continue
		}
		if !ok {
			break
		}
		got++
	}
	t.Logf("retry salvaged %d of %d records", got, n)
	if got == 0 {
		t.Fatal("the retry salvaged nothing")
	}
}
