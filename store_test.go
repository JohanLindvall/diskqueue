package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// Test helpers for the raw store.

func newTestStore(t *testing.T, segmentSize int64, maxSegments int) (*store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := openStore(dir, segmentSize, maxSegments, true, 0, 0, false)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { s.close() })
	return s, dir
}

func mustAppend(t *testing.T, s *store, p []byte) {
	t.Helper()
	if err := s.append(p); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// idxRec encodes a record index into a 2-byte payload so order can be verified.
func idxRec(i int) []byte { return []byte{byte(i), byte(i >> 8)} }
func recIdx(p []byte) int { return int(p[0]) | int(p[1])<<8 }

// readFileHeader reads the four header words straight off disk. mmap MAP_SHARED
// writes are visible through the page cache, so this works without msync.
func readFileHeader(t *testing.T, dir string, num uint64) (commit, write, written, committed int64) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("data.%08d", num)))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < headerSize {
		t.Fatalf("short header: %d bytes", len(b))
	}
	g := func(i int) int64 { return int64(binary.LittleEndian.Uint64(b[i:])) }
	return g(8), g(16), g(24), g(32) // commit, write, written, committed (after the 8-byte magic)
}

func TestStoreAppendReadCommit(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)

	if !s.empty() || s.count() != 0 || s.size() != 0 {
		t.Fatalf("fresh store: empty=%v count=%d size=%d", s.empty(), s.count(), s.size())
	}

	for i := 0; i < 3; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if s.empty() {
		t.Fatal("should not be empty after appends")
	}
	if got := s.count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	// takeHead reads in order and advances the head, but does not commit.
	var last int64
	for i := 0; i < 3; i++ {
		p, off, ok, _ := s.takeHead()
		if !ok || recIdx(p) != i {
			t.Fatalf("takeHead %d: idx=%d ok=%v", i, recIdx(p), ok)
		}
		last = off
	}
	if _, _, ok, _ := s.takeHead(); ok {
		t.Fatal("takeHead past the tail should report empty")
	}
	if !s.empty() {
		t.Fatal("head caught up: should be empty")
	}
	if got := s.count(); got != 3 {
		t.Fatalf("uncommitted count = %d, want 3", got)
	}

	// Commit everything; count and size go to zero.
	s.commitTo(last)
	if got := s.count(); got != 0 {
		t.Fatalf("count after commit = %d, want 0", got)
	}
	if got := s.size(); got != 0 {
		t.Fatalf("size after commit = %d, want 0", got)
	}
}

func TestStorePartialCommit(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	offs := make([]int64, 5)
	for i := range offs {
		mustAppend(t, s, idxRec(i))
	}
	for i := range offs {
		_, off, ok, _ := s.takeHead()
		if !ok {
			t.Fatalf("takeHead %d not ok", i)
		}
		offs[i] = off
	}
	// Commit through the second record (index 1).
	s.commitTo(offs[1])
	if got := s.count(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
	// size = uncommitted bytes = records 2,3,4 = 3 * (uvarint(2)+2+checksum).
	if want := int64(3 * (3 + checksumSize)); s.size() != want {
		t.Fatalf("size = %d, want %d", s.size(), want)
	}
	// Committing an earlier offset is a no-op.
	s.commitTo(offs[0])
	if got := s.count(); got != 3 {
		t.Fatalf("count after stale commit = %d, want 3", got)
	}
}

func TestStoreCycleAndOrder(t *testing.T) {
	s, dir := newTestStore(t, 64, 0) // ~5 eleven-byte records per segment
	const n = 200
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if got := countDataFiles(t, dir); got < 2 {
		t.Fatalf("expected multiple segments, got %d", got)
	}
	for i := 0; i < n; i++ {
		p, off, ok, _ := s.takeHead()
		if !ok || recIdx(p) != i {
			t.Fatalf("record %d: idx=%d ok=%v (spans files)", i, recIdx(p), ok)
		}
		s.commitTo(off)
	}
	if !s.empty() {
		t.Fatal("should be empty after reading all")
	}
}

func TestStoreReadsNeverDeleteWritesDrop(t *testing.T) {
	s, dir := newTestStore(t, 64, 0)
	const n = 200
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	peak := countDataFiles(t, dir)
	if peak < 3 {
		t.Fatalf("expected several segments, got %d", peak)
	}

	// Drain and commit everything via reads — this must not delete any file.
	for i := 0; i < n; i++ {
		_, off, ok, _ := s.takeHead()
		if !ok {
			t.Fatalf("takeHead %d not ok", i)
		}
		s.commitTo(off)
	}
	if got := countDataFiles(t, dir); got != peak {
		t.Fatalf("reads/commits deleted files: %d != %d", got, peak)
	}

	// A write cycles and reclaims the fully-committed files.
	for i := 0; i < 100; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if got := countDataFiles(t, dir); got >= peak {
		t.Fatalf("expected files dropped on cycle, got %d (peak %d)", got, peak)
	}
}

func TestStoreMaxSegments(t *testing.T) {
	s, dir := newTestStore(t, 64, 2)
	added := 0
	for {
		if err := s.append(idxRec(added)); err != nil {
			if errors.Is(err, ErrFull) {
				break
			}
			t.Fatal(err)
		}
		added++
		if n := countDataFiles(t, dir); n > 2 {
			t.Fatalf("file count %d exceeds maxSegments 2", n)
		}
		if added > 100000 {
			t.Fatal("never hit ErrFull")
		}
	}
	if added == 0 {
		t.Fatal("expected to append some records before ErrFull")
	}
	// Draining and committing frees segments; appends resume.
	for {
		_, off, ok, _ := s.takeHead()
		if !ok {
			break
		}
		s.commitTo(off)
	}
	if err := s.append(idxRec(0)); err != nil {
		t.Fatalf("append after draining: %v", err)
	}
}

func TestStoreRecordTooLarge(t *testing.T) {
	s, _ := newTestStore(t, 64, 0)
	if err := s.append(make([]byte, 64)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
	if err := s.append(make([]byte, 50)); err != nil { // 1 + 50 + 8 checksum = 59 <= 64
		t.Fatalf("50-byte record should fit: %v", err)
	}
}

func TestStoreHeaderOnDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	const recSize = 3 + checksumSize // uvarint(2)=1 + 2 payload + 8 checksum
	for i := 0; i < 5; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Commit the first two records.
	_, _, _, _ = s.takeHead()
	_, off2, _, _ := s.takeHead()
	s.commitTo(off2)

	commit, write, written, committed := readFileHeader(t, dir, 1)
	if written != 5 {
		t.Errorf("written count on disk = %d, want 5", written)
	}
	if committed != 2 {
		t.Errorf("committed count on disk = %d, want 2", committed)
	}
	if write != headerSize+5*recSize {
		t.Errorf("write cursor = %d, want %d", write, headerSize+5*recSize)
	}
	if commit != headerSize+2*recSize {
		t.Errorf("commit cursor = %d, want %d", commit, headerSize+2*recSize)
	}
}

func TestStoreRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 64, 0, true, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Commit the first 20.
	for i := 0; i < 20; i++ {
		_, off, ok, _ := s.takeHead()
		if !ok {
			t.Fatalf("take %d not ok", i)
		}
		s.commitTo(off)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: counts come from the header (no record scan), the read cursor is
	// reset to the commit cursor, and the remaining records replay in order.
	s2, err := openStore(dir, 64, 0, true, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	if got := s2.count(); got != n-20 {
		t.Fatalf("recovered count = %d, want %d", got, n-20)
	}
	for i := 20; i < n; i++ {
		p, off, ok, _ := s2.takeHead()
		if !ok || recIdx(p) != i {
			t.Fatalf("after reopen record %d: idx=%d ok=%v", i, recIdx(p), ok)
		}
		s2.commitTo(off)
	}
	if !s2.empty() {
		t.Fatal("should be drained after replay")
	}
}

func TestStoreReopenFullyDrained(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustAppend(t, s, idxRec(i))
		_, off, _, _ := s.takeHead()
		s.commitTo(off)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	s2, err := openStore(dir, 4096, 0, true, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.close()
	if !s2.empty() || s2.count() != 0 {
		t.Fatalf("fully-drained reopen: empty=%v count=%d", s2.empty(), s2.count())
	}
}

// --- Model-based stress and fuzzing ---------------------------------------

func genPayload(length int, fill byte) []byte {
	p := make([]byte, length)
	for i := range p {
		p[i] = fill + byte(i)
	}
	return p
}

func checkPayload(t *testing.T, got []byte, length int, fill byte) {
	t.Helper()
	if len(got) != length {
		t.Fatalf("payload len=%d want=%d", len(got), length)
	}
	for i := 0; i < length; i++ {
		if got[i] != fill+byte(i) {
			t.Fatalf("payload[%d]=%d want=%d", i, got[i], fill+byte(i))
		}
	}
}

// runStoreProgram interprets prog as a stream of store operations, applying each
// to a real store and a reference model, and asserting they stay in agreement:
// payloads round-trip byte-for-byte and in FIFO order, count/size/empty match,
// and reopens preserve committed state while replaying the uncommitted tail.
func runStoreProgram(t *testing.T, segSize int64, maxSeg int, prog []byte) {
	t.Helper()
	dir := t.TempDir()
	s, err := openStore(dir, segSize, maxSeg, true, 0, 0, false)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	defer func() { _ = s.close() }()

	type recMeta struct {
		length int
		fill   byte
	}
	type readEnt struct {
		idx int
		off int64
	}
	var (
		recs   []recMeta
		head   int // next record index to read
		commit int // next record index to commit
		reads  []readEnt
		ubytes int64 // uncommitted encoded bytes (== store.size())
	)
	enc := func(n int) int64 { return int64(uvarintLen(uint64(n)) + n + checksumSize) }

	check := func() {
		if got, want := s.count(), int64(len(recs)-commit); got != want {
			t.Fatalf("count=%d want=%d", got, want)
		}
		if got := s.size(); got != ubytes {
			t.Fatalf("size=%d want=%d", got, ubytes)
		}
		if got, want := s.empty(), head >= len(recs); got != want {
			t.Fatalf("empty=%v want=%v (head=%d recs=%d)", got, want, head, len(recs))
		}
	}

	pos := 0
	next := func() byte {
		if pos >= len(prog) {
			return 0
		}
		b := prog[pos]
		pos++
		return b
	}

	for pos < len(prog) {
		switch b := next(); {
		case b < 128: // append (≈50%)
			L := int(next())
			fill := next()
			switch err := s.append(genPayload(L, fill)); {
			case err == nil:
				recs = append(recs, recMeta{L, fill})
				ubytes += enc(L)
			case errors.Is(err, ErrFull), errors.Is(err, ErrRecordTooLarge):
				// expected backpressure / oversize; model unchanged
			default:
				t.Fatalf("append: %v", err)
			}
		case b < 176: // reserve: read without committing (≈19%)
			p, off, ok, _ := s.takeHead()
			if ok {
				if head >= len(recs) {
					t.Fatalf("takeHead returned data but model head=%d recs=%d", head, len(recs))
				}
				checkPayload(t, p, recs[head].length, recs[head].fill)
				reads = append(reads, readEnt{head, off})
				head++
			} else if head < len(recs) {
				t.Fatalf("takeHead empty but model has record at head=%d", head)
			}
		case b < 240: // commit a random reserved record and everything before it (≈25%)
			if len(reads) > 0 {
				j := int(next()) % len(reads)
				s.commitTo(reads[j].off)
				newCommit := reads[j].idx + 1
				for k := commit; k < newCommit; k++ {
					ubytes -= enc(recs[k].length)
				}
				commit = newCommit
				reads = reads[j+1:]
			}
		default: // reopen (≈6%)
			if err := s.close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			ns, nerr := openStore(dir, segSize, maxSeg, true, 0, 0, false)
			if nerr != nil {
				t.Fatalf("reopen: %v", nerr)
			}
			s = ns
			head = commit // read cursor resets to the commit cursor
			reads = nil
		}
		check()
	}

	// Drain whatever remains, verifying order and payloads.
	for head < len(recs) {
		p, off, ok, _ := s.takeHead()
		if !ok {
			t.Fatalf("drain: expected record at head=%d", head)
		}
		checkPayload(t, p, recs[head].length, recs[head].fill)
		head++
		s.commitTo(off)
		for k := commit; k < head; k++ {
			ubytes -= enc(recs[k].length)
		}
		commit = head
		reads = nil
	}
	check()
}

func FuzzStore(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add([]byte{1, 3, 0, 5, 0, 0, 7, 100, 1, 200, 2})
	f.Add([]byte{10, 1, 0, 250, 9, 0, 250, 9, 200, 0, 200, 0, 200})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		// Keep segments modest and the file count bounded so a pathological input
		// cannot explode into hundreds of thousands of files.
		segSize := int64(256 + int(data[0])*4) // 256..1276
		maxSeg := 2 + int(data[1])%7           // 2..8
		runStoreProgram(t, segSize, maxSeg, data[2:])
	})
}

func TestStressStoreRandom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	cases := []struct {
		seg int64
		max int
	}{
		{64, 8},   // tiny segments: heavy cycling, frequent ErrRecordTooLarge/ErrFull
		{256, 2},  // very few segments: lots of backpressure
		{1024, 6}, // roomier
		{4096, 4},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("seg%d_max%d", tc.seg, tc.max), func(t *testing.T) {
			rng := rand.New(rand.NewSource(tc.seg*31 + int64(tc.max)))
			prog := make([]byte, 30000)
			rng.Read(prog)
			runStoreProgram(t, tc.seg, tc.max, prog)
		})
	}
}

// TestStoreCorruptLengthNoPanic verifies that a record with a corrupt length
// prefix decodes as "not ok" rather than panicking past the mapping.
func TestStoreCorruptLengthNoPanic(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(1))

	// Overwrite the first record's uvarint length with a huge value (0xFF…),
	// which would slice far past the data region without the bounds guard.
	for i := 0; i < 10; i++ {
		s.files[0].data[headerSize+i] = 0xFF
	}

	if _, _, _, ok, _ := s.read(0); ok {
		t.Fatal("corrupt record should not decode ok")
	}
	// A commit walking the corrupt record must also stop cleanly, not panic.
	s.commitTo(s.writeOff)
}

// TestStoreBatchedCommitAcrossSegments commits many records spanning several
// segments in a single commitTo (with sync on, exercising the per-file header
// flush and directory sync), then reopens to confirm the batch is durable.
func TestStoreBatchedCommitAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 16, 0, false, 0, 0, false) // tiny segments, sync enabled
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if got := s.count(); got != n {
		t.Fatalf("count before commit = %d, want %d", got, n)
	}
	s.commitTo(s.writeOff) // one call crossing every segment
	if got := s.count(); got != 0 {
		t.Fatalf("count after commit = %d, want 0", got)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := openStore(dir, 16, 0, false, 0, 0, false)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.close()
	if got := s2.count(); got != 0 {
		t.Fatalf("count after reopen = %d, want 0 (commit not durable)", got)
	}
	if !s2.empty() {
		t.Fatal("store should be empty after reopen")
	}
}

// TestStoreChecksumDetectsCorruption flips a payload byte in the mapping and
// verifies the read fails with ErrCorrupt without advancing the head cursor.
func TestStoreChecksumDetectsCorruption(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(0))
	mustAppend(t, s, idxRec(1))

	// Corrupt the first record's payload (just past its 1-byte length prefix).
	s.files[0].data[headerSize+1] ^= 0xFF

	if _, _, _, err := s.takeHead(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead on corrupt record: err=%v, want ErrCorrupt", err)
	}
	if s.headOff != 0 {
		t.Fatalf("head advanced past corrupt record: headOff=%d, want 0", s.headOff)
	}
	// Repairing the byte lets the read succeed again — proof it was the checksum.
	s.files[0].data[headerSize+1] ^= 0xFF
	p, _, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("after repair: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
}

// TestStoreHeaderChecksumDetected corrupts a header field on disk and verifies
// that reopening fails with ErrCorrupt rather than trusting a bad cursor.
func TestStoreHeaderChecksumDetected(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[16] ^= 0xFF // flip a write-cursor byte, leaving magic/version intact
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openStore(dir, 4096, 0, false, 0, 0, false); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reopen with corrupt header: got %v, want ErrCorrupt", err)
	}
}

// TestStoreBadFormatRejected verifies that a file with the wrong magic is
// rejected with ErrBadFormat (and that magic is checked before the checksum).
func TestStoreBadFormatRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "data.00000001")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF // corrupt the magic
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openStore(dir, 4096, 0, false, 0, 0, false); !errors.Is(err, ErrBadFormat) {
		t.Fatalf("reopen with bad magic: got %v, want ErrBadFormat", err)
	}
}

// TestStoreLazyMappingBounded checks that with MaxMapped set, a deep backlog
// keeps at most that many segments mapped while every record stays readable in
// order (old segments are remapped on demand).
func TestStoreLazyMappingBounded(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 64, 0, true, 0, 2, false) // tiny segments, cap 2 mappings
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })

	const n = 500
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if got := countDataFiles(t, dir); got < 10 {
		t.Fatalf("expected many segments, got %d", got)
	}
	if s.mappedLen > 2 {
		t.Fatalf("after writes %d segments mapped, cap is 2", s.mappedLen)
	}

	for i := 0; i < n; i++ {
		p, off, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("read %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		s.commitTo(off)
		if s.mappedLen > 2 {
			t.Fatalf("during read %d: %d segments mapped, cap is 2", i, s.mappedLen)
		}
	}
	if !s.empty() {
		t.Fatal("should be drained")
	}
}

// highestDataFileNum returns the largest data.* segment number in dir.
func highestDataFileNum(t *testing.T, dir string) uint64 {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "data.*"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob: %v (n=%d)", err, len(paths))
	}
	var max uint64
	for _, p := range paths {
		var n uint64
		if _, err := fmt.Sscanf(filepath.Base(p), "data.%08d", &n); err == nil && n > max {
			max = n
		}
	}
	return max
}

// TestStoreRecoverTornTail corrupts the highest segment's header and verifies
// that, with recovery, the open drops it instead of failing and the earlier
// segments stay readable in order.
func TestStoreRecoverTornTail(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 64, 0, false, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	if countDataFiles(t, dir) < 3 {
		t.Fatal("need several segments")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	last := highestDataFileNum(t, dir)
	path := filepath.Join(dir, fmt.Sprintf("data.%08d", last))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[16] ^= 0xFF // corrupt the write-cursor byte of the tail header
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	// Strict open fails.
	if _, err := openStore(dir, 64, 0, false, 0, 0, false); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("strict reopen: %v, want ErrCorrupt", err)
	}
	// Recovering open drops the torn tail and reports it.
	s2, err := openStore(dir, 64, 0, false, 0, 0, true)
	if err != nil {
		t.Fatalf("recovering reopen: %v", err)
	}
	t.Cleanup(func() { s2.close() })
	if got := s2.corruptionCount(); got != 1 {
		t.Fatalf("corruptions=%d, want 1", got)
	}
	prev := -1
	for {
		p, off, ok, err := s2.takeHead()
		if err != nil {
			t.Fatalf("read after recovery: %v", err)
		}
		if !ok {
			break
		}
		if recIdx(p) <= prev {
			t.Fatalf("out of order: %d after %d", recIdx(p), prev)
		}
		prev = recIdx(p)
		s2.commitTo(off)
	}
	if prev < 0 {
		t.Fatal("expected earlier segments to survive")
	}
}

// TestStoreRecoverSkipsCorruptSegment corrupts a record in an early segment and
// verifies that, with recovery, the reader quarantines that segment's remainder
// and continues delivering later records in order (last record still arrives).
func TestStoreRecoverSkipsCorruptSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 128, 0, false, 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	const n = 60
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Corrupt a record's bytes inside the first segment (records after the first).
	s.files[0].data[headerSize+20] ^= 0xFF

	delivered := 0
	prev := -1
	var last int
	for {
		p, off, ok, err := s.takeHead()
		if err != nil {
			t.Fatalf("read with recovery returned error: %v", err)
		}
		if !ok {
			break
		}
		if recIdx(p) <= prev {
			t.Fatalf("out of order: %d after %d", recIdx(p), prev)
		}
		prev = recIdx(p)
		last = recIdx(p)
		delivered++
		s.commitTo(off)
	}
	if s.corruptionCount() < 1 {
		t.Fatal("expected at least one quarantined segment")
	}
	if delivered >= n {
		t.Fatalf("expected some records dropped, delivered=%d of %d", delivered, n)
	}
	if last != n-1 {
		t.Fatalf("last delivered=%d, want %d (later segments must survive)", last, n-1)
	}
}
