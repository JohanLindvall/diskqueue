package diskqueue

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
	s, err := openStore(dir, segmentSize, maxSegments, true, 0, 0)
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

// corruptData overwrites len(b) bytes of segment fileIdx's data region starting at
// dataOff, writing through the store's open handle so the store's reads see it.
//
// It drops the store's read block as well. These injectors model damage that was
// already on disk — bit-rot, a torn write, a truncation — not a rewrite that
// happens between a read and its commit. Without this the store would keep
// serving the bytes it had read before the test scribbled on the file, which is
// correct behaviour (published records are immutable) but the wrong scenario.
func corruptData(t *testing.T, s *store, fileIdx, dataOff int, b []byte) {
	t.Helper()
	if _, err := s.files[fileIdx].f.WriteAt(b, int64(headerSize+dataOff)); err != nil {
		t.Fatal(err)
	}
	s.dropBlock()
}

// flipData XORs mask into the data-region byte at dataOff of segment fileIdx.
func flipData(t *testing.T, s *store, fileIdx, dataOff int, mask byte) {
	t.Helper()
	one := make([]byte, 1)
	if _, err := s.files[fileIdx].f.ReadAt(one, int64(headerSize+dataOff)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= mask
	corruptData(t, s, fileIdx, dataOff, one) // drops the read block for us
}

// readFileHeader reads the four header words straight off disk. The store writes
// each header to page 0 with WriteAt, so the bytes are visible through the page
// cache here without an fsync.
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

func TestStoreCommitsReclaimFiles(t *testing.T) {
	s, dir := newTestStore(t, 64, 0)
	const n = 200
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	peak := countDataFiles(t, dir)
	if peak < 3 {
		t.Fatalf("expected several segments, got %d", peak)
	}

	// Draining and committing via reads reclaims fully-committed files as they
	// empty — no append required — leaving only the active (write) segment.
	for i := 0; i < n; i++ {
		_, off, ok, _ := s.takeHead()
		if !ok {
			t.Fatalf("takeHead %d not ok", i)
		}
		s.commitTo(off)
	}
	if !s.empty() {
		t.Fatal("should be empty after reading all")
	}
	if got := countDataFiles(t, dir); got != 1 {
		t.Fatalf("commits did not reclaim to the active file: %d files left (peak %d)", got, peak)
	}

	// The surviving active file still accepts appends and stays correct.
	mustAppend(t, s, idxRec(999))
	p, off, ok, _ := s.takeHead()
	if !ok || recIdx(p) != 999 {
		t.Fatalf("post-reclaim append/read: idx=%d ok=%v", recIdx(p), ok)
	}
	s.commitTo(off)
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

func TestStoreOversizedRecordGetsOwnSegment(t *testing.T) {
	s, _ := newTestStore(t, 64, 0)
	// Too large for the 64-byte geometry: it gets a segment sized to itself, whose
	// capacity is the record's framed length and nothing more, so the segment can
	// never take a second record.
	if err := s.append(make([]byte, 64)); err != nil {
		t.Fatalf("oversized append: %v", err)
	}
	big := s.active()
	if !big.oversized() {
		t.Fatal("the segment holding an oversized record is not flagged oversized")
	}
	if big.capacity != big.size {
		t.Fatalf("oversized segment: capacity=%d size=%d, want them equal", big.capacity, big.size)
	}
	if err := s.append(make([]byte, 50)); err != nil { // 1 + 50 + 8 checksum = 59 <= 64
		t.Fatalf("50-byte record should fit: %v", err)
	}
	if s.active() == big {
		t.Fatal("a second record landed in the oversized segment")
	}
}

func TestStoreHeaderOnDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, true, 0, 0)
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
	s, err := openStore(dir, 64, 0, true, 0, 0)
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
	s2, err := openStore(dir, 64, 0, true, 0, 0)
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
	s, err := openStore(dir, 4096, 0, true, 0, 0)
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

	s2, err := openStore(dir, 4096, 0, true, 0, 0)
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
	s, err := openStore(dir, segSize, maxSeg, true, 0, 0)
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
			ns, nerr := openStore(dir, segSize, maxSeg, true, 0, 0)
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
// TestStoreCorruptLengthNoPanic walks the length-prefix bounds guard with the
// values that break it in different ways. The 1<<63-1 case is the one that used
// to kill the process: int(v) is a large *positive* int, so the old `L < 0` guard
// let it through, and n+L+checksumSize then wrapped negative — reslicing readBuf
// to a negative length inside growBuf. The commit path shared the same guard and
// drove the commit cursor to ≈ -9.2e18.
func TestStoreCorruptLengthNoPanic(t *testing.T) {
	uvarint := func(v uint64) []byte {
		b := make([]byte, binary.MaxVarintLen64)
		return b[:binary.PutUvarint(b, v)]
	}
	cases := []struct {
		name   string
		prefix []byte
	}{
		{"unterminated varint", unframeable},
		{"wraps int when summed", uvarint(1<<63 - 1)},
		{"negative when narrowed", uvarint(1<<64 - 1)},
		{"larger than the segment", uvarint(1 << 40)},
		{"one byte too long", uvarint(4096)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestStore(t, 4096, 0)
			mustAppend(t, s, idxRec(1))
			corruptData(t, s, 0, 0, tc.prefix)

			if _, _, _, ok, _ := s.read(0); ok {
				t.Fatal("corrupt record should not decode ok")
			}
			if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("takeHead: ok=%v err=%v, want ErrCorrupt", ok, err)
			}
			// The framing is unusable, so the segment goes — and the cursors move
			// past it. Standing still here is what wedges a queue forever.
			if s.headOff != s.writeOff {
				t.Fatalf("headOff=%d, want %d: the queue must advance past unusable framing",
					s.headOff, s.writeOff)
			}
			if _, _, ok, err := s.takeHead(); ok || err != nil {
				t.Fatalf("after the skip: ok=%v err=%v, want a clean empty", ok, err)
			}
			// A commit walking the corrupt record must also make progress, not panic
			// and not move the cursor anywhere absurd.
			if err := s.commitTo(s.writeOff); err != nil {
				t.Fatalf("commitTo: %v", err)
			}
			if s.commitOff < 0 || s.commitOff > s.writeOff {
				t.Fatalf("commitOff=%d outside [0,%d]", s.commitOff, s.writeOff)
			}
			// A record that fails the guard is never fetched, so the buffer must
			// never have been grown to the claimed length — only to the read-ahead
			// block the first pread legitimately asks for.
			if cap(s.readBuf) > readAhead {
				t.Fatalf("readBuf grew to %d: a corrupt length escaped the bound", cap(s.readBuf))
			}
		})
	}
}

// TestStoreBatchedCommitAcrossSegments commits many records spanning several
// segments in a single commitTo (with sync on, exercising the per-file header
// flush and directory sync), then reopens to confirm the batch is durable.
func TestStoreBatchedCommitAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 16, 0, false, 0, 0) // tiny segments, sync enabled
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

	s2, err := openStore(dir, 16, 0, false, 0, 0)
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

// TestStoreChecksumDetectsCorruption flips a payload byte and verifies the blast
// radius: the record's length still framed it inside the segment, so exactly that
// one record is dropped and the queue moves on to the next.
func TestStoreChecksumDetectsCorruption(t *testing.T) {
	s, _ := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(0))
	mustAppend(t, s, idxRec(1))

	// Corrupt the first record's payload (just past its 1-byte length prefix).
	flipData(t, s, 0, 1, 0xFF)

	if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("takeHead on corrupt record: ok=%v err=%v, want ErrCorrupt", ok, err)
	}
	if s.headOff == 0 {
		t.Fatal("the damaged record was not dropped: the queue would never move again")
	}
	// The neighbouring record is untouched — one bad byte costs one record.
	p, _, ok, err := s.takeHead()
	if err != nil || !ok || recIdx(p) != 1 {
		t.Fatalf("record after the damaged one: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if got := s.lostRecords; got != 1 {
		t.Fatalf("lostRecords=%d, want 1", got)
	}
	if s.lostSegments != 0 {
		t.Fatalf("lostSegments=%d: a payload flip must not cost a whole segment", s.lostSegments)
	}
	if s.lostBytes == 0 {
		t.Fatal("the dropped record's bytes were not counted")
	}
}

// TestStoreHeaderChecksumDetected corrupts a header field on disk, leaving
// magic and version intact — the signature of a rewrite torn by a power cut.
// The header is the only casualty: the record beneath it carries its own
// checksum, so the open rebuilds the header from a verified walk instead of
// dropping the segment. One event is owed to the reader; nothing is lost.
func TestStoreHeaderChecksumDetected(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
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
	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen with a torn header: %v, want the segment salvaged", err)
	}
	defer func() { _ = s2.close() }()
	if s2.corruptionCount() != 1 || s2.lostSegments != 0 || s2.lostRecords != 0 {
		t.Fatalf("corruptions=%d lostSegments=%d lostRecords=%d, want 1/0/0: the record survived",
			s2.corruptionCount(), s2.lostSegments, s2.lostRecords)
	}
	// The event is reported once, then the salvaged record is delivered.
	if _, _, ok, err := s2.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("owed report: ok=%v err=%v, want ErrCorrupt", ok, err)
	}
	p, _, ok, err := s2.takeHead()
	if err != nil || !ok || recIdx(p) != 0 {
		t.Fatalf("salvaged record: idx=%d ok=%v err=%v", recIdx(p), ok, err)
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("after the salvage: ok=%v err=%v, want a clean empty", ok, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the salvaged segment must stay on disk: %v", err)
	}
}

// TestStoreBadMagicDropped: a file carrying the right name but not our magic
// cannot be framed, so it goes the same way as a damaged header.
func TestStoreBadMagicDropped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
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
	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen with bad magic: %v, want the segment dropped", err)
	}
	defer func() { _ = s2.close() }()
	assertSegmentLoss(t, s2, 1)
}

// TestStoreForeignVersionDropped: a segment written by a build whose framing this
// one does not know is unreadable but undamaged, so it is dropped silently and
// counted apart from corruption — no data-loss alarm for a format change.
func TestStoreForeignVersionDropped(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, idxRec(0))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	// A valid header naming a version from the future.
	forgeHeader(t, dir, 1, func(h []byte) { h[40] = formatVersion + 7 })

	s2, err := openStore(dir, 4096, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen with a foreign version: %v, want the segment dropped", err)
	}
	defer func() { _ = s2.close() }()
	if s2.foreignSegments != 1 {
		t.Fatalf("foreignSegments=%d, want 1", s2.foreignSegments)
	}
	if s2.foreignBytes == 0 {
		t.Fatal("foreignBytes not counted")
	}
	if s2.corruptionCount() != 0 {
		t.Fatalf("corruptions=%d: an unknown version is not damage", s2.corruptionCount())
	}
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("after a foreign drop: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// assertSegmentLoss checks that n segments were dropped as damaged, and that the
// loss is reported to a reader exactly n times before the queue reads clean.
func assertSegmentLoss(t *testing.T, s *store, n int) {
	t.Helper()
	if got := s.lostSegments; got != uint64(n) {
		t.Fatalf("lostSegments=%d, want %d", got, n)
	}
	if got := s.corruptionCount(); got != uint64(n) {
		t.Fatalf("corruptions=%d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		if _, _, ok, err := s.takeHead(); ok || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("loss report %d: ok=%v err=%v, want ErrCorrupt", i, ok, err)
		}
	}
	if _, _, ok, err := s.takeHead(); ok || err != nil {
		t.Fatalf("after the loss reports: ok=%v err=%v, want a clean empty", ok, err)
	}
}

// TestStoreLazyMappingBounded checks that with MaxOpenFiles set, a deep backlog
// keeps at most that many segments mapped while every record stays readable in
// order (old segments are remapped on demand).
func TestStoreLazyMappingBounded(t *testing.T) {
	dir := t.TempDir()
	// A cap below the floor is raised to it: the write, read and commit cursors
	// can each be in a different segment, so 3 is the smallest working set.
	const cap = 3
	s, err := openStore(dir, 64, 0, true, 0, cap) // tiny segments, capped handles
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
	if s.nOpen > cap {
		t.Fatalf("after writes %d segments mapped, cap is %d", s.nOpen, cap)
	}

	for i := 0; i < n; i++ {
		p, off, ok, err := s.takeHead()
		if err != nil || !ok || recIdx(p) != i {
			t.Fatalf("read %d: idx=%d ok=%v err=%v", i, recIdx(p), ok, err)
		}
		s.commitTo(off)
		if s.nOpen > cap {
			t.Fatalf("during read %d: %d segments mapped, cap is %d", i, s.nOpen, cap)
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

// TestStoreRecoverTornTail corrupts the highest segment's header (magic and
// version intact — a torn rewrite) and verifies the open salvages it: every
// record in the store, the torn tail's included, is still delivered in order,
// with the one event reported.
func TestStoreRecoverTornTail(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 64, 0, false, 0, 0)
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

	// The open drops the torn tail and reports it; the earlier segments survive.
	s2, err := openStore(dir, 64, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s2.close() })
	if got := s2.corruptionCount(); got != 1 {
		t.Fatalf("corruptions=%d, want 1", got)
	}
	got, events := drainRecovering(t, s2)
	if events != 1 {
		t.Fatalf("%d corruption events reported to the reader, want 1", events)
	}
	if len(got) != n {
		t.Fatalf("drained %d records, want all %d: the torn tail's records vouch for themselves", len(got), n)
	}
	assertAscending(t, got)
}

// drainRecovering reads a store dry the way a consumer is meant to: a corruption
// event reports one loss and the queue has already advanced, so the loop simply
// goes round again. It returns the records delivered and the number of events.
func drainRecovering(t *testing.T, s *store) (delivered []int, events int) {
	t.Helper()
	for i := 0; ; i++ {
		if i > 100000 {
			t.Fatal("drain made no progress: the queue is wedged")
		}
		p, off, ok, err := s.takeHead()
		switch {
		case errors.Is(err, ErrCorrupt):
			events++
			continue
		case err != nil:
			t.Fatalf("drain: %v", err)
		case !ok:
			return delivered, events
		}
		delivered = append(delivered, recIdx(p))
		if err := s.commitTo(off); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
}

func assertAscending(t *testing.T, got []int) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("out of order: %d after %d", got[i], got[i-1])
		}
	}
}

// TestStoreCorruptPayloadCostsOneRecord pins the blast-radius rule on a live
// store: a damaged payload whose length still frames it inside the segment costs
// exactly that record. Everything else, in every segment, is delivered.
func TestStoreCorruptPayloadCostsOneRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	const n = 60
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Flip a byte inside the payload of the second record of the first segment
	// (records are 11 bytes: 1 length + 2 payload + 8 checksum).
	flipData(t, s, 0, 12, 0xFF)

	got, events := drainRecovering(t, s)
	if events != 1 {
		t.Fatalf("%d corruption events, want 1", events)
	}
	if len(got) != n-1 {
		t.Fatalf("delivered %d records, want %d: one bad byte cost more than one record", len(got), n-1)
	}
	assertAscending(t, got)
	if got[len(got)-1] != n-1 {
		t.Fatalf("last delivered=%d, want %d", got[len(got)-1], n-1)
	}
	if s.lostRecords != 1 || s.lostSegments != 0 {
		t.Fatalf("lostRecords=%d lostSegments=%d, want 1 and 0", s.lostRecords, s.lostSegments)
	}
}

// TestStoreCorruptFramingCostsOneSegment is the other half: damage the length
// prefix and the record boundaries behind it are gone with it, so the rest of
// that segment is abandoned — but no more than that segment.
func TestStoreCorruptFramingCostsOneSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 128, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.close() })
	const n = 60
	for i := 0; i < n; i++ {
		mustAppend(t, s, idxRec(i))
	}
	segRecords := int(s.files[0].written)
	// Overwrite the second record's length prefix with an unusable one.
	corruptData(t, s, 0, 11, unframeable)

	got, events := drainRecovering(t, s)
	if events != 1 {
		t.Fatalf("%d corruption events, want 1", events)
	}
	assertAscending(t, got)
	if len(got) != n-(segRecords-1) {
		t.Fatalf("delivered %d records, want %d: the loss should stop at the segment boundary",
			len(got), n-(segRecords-1))
	}
	if got[len(got)-1] != n-1 {
		t.Fatalf("last delivered=%d, want %d (later segments must survive)", got[len(got)-1], n-1)
	}
	if s.lostSegments != 1 {
		t.Fatalf("lostSegments=%d, want 1", s.lostSegments)
	}
}

// TestForceCommitAllQuarantineSurvivesReopen drives skipCorruptSegment's
// "cursor addresses no live segment" arm and reopens the store.
//
// That arm squares every counter so Count() and Empty() agree — but the squaring
// is only true if it reaches the headers. When it was applied in memory alone the
// next open recovered the untouched commit cursors and replayed every record,
// while nCommittedTotal had already counted them, so Stats().Committed counted
// them a second time as they were re-consumed. The in-process assertions below
// all passed in that state; only the reopen catches it.
func TestForceCommitAllQuarantineSurvivesReopen(t *testing.T) {
	const (
		segSize = 4096
		n       = 12
	)
	dir := t.TempDir()
	s, err := openStore(dir, segSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	payload := make([]byte, 512) // 8 records per segment, so n spans several files
	for i := 0; i < n; i++ {
		mustAppend(t, s, payload)
	}
	if got := s.count(); got != n {
		t.Fatalf("count before = %d, want %d", got, n)
	}
	if len(s.files) < 2 {
		t.Fatalf("want a multi-segment store, got %d files", len(s.files))
	}

	// writeOff is the end of the active file's data, and files hold [base, base+size),
	// so no live segment contains it — which is the arm under test.
	if df := s.fileForOffset(s.writeOff); df != nil {
		t.Fatalf("writeOff %d still lands in a segment; the df == nil arm was not reached", s.writeOff)
	}
	if err := s.skipCorruptSegment(s.writeOff); err != nil {
		t.Fatalf("skipCorruptSegment: %v", err)
	}
	if got := s.count(); got != 0 {
		t.Fatalf("count after quarantine = %d, want 0", got)
	}
	if !s.empty() {
		t.Fatal("queue not empty after quarantining everything")
	}
	committed := s.stats().Committed
	if committed != n {
		t.Fatalf("Committed after quarantine = %d, want %d", committed, n)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := openStore(dir, segSize, 0, false, 0, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.close() }()

	if got := s2.count(); got != 0 {
		t.Fatalf("reopen replayed %d records the quarantine said were retired; "+
			"the force-commit never reached the headers", got)
	}
	if !s2.empty() {
		t.Fatal("reopened store is not empty")
	}
	// And the records must not be deliverable either — a cursor that says "empty"
	// while takeHead still hands records out is the same bug wearing a disguise.
	if _, _, ok, err := s2.takeHead(); ok || err != nil {
		t.Fatalf("takeHead after reopen = ok %v, err %v; want no record", ok, err)
	}
}
