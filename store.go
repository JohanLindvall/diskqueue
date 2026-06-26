package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sys/unix"
)

// On-disk format: numbered data files (data.00000001, …), each a 64-byte header
// (magic, cursors/counts, version, header checksum — see the dataFile accessors)
// followed by records (uvarint(len) || payload || xxhash64(payload) as 8 little-
// endian bytes).
//
// Everything recovery needs lives in the header, so it never scans records.
// Records never span files. A global byte offset addresses the stream: file F
// holds offsets [F.base, F.base+F.size). Files are dropped once fully committed,
// but only while writing — reads and commits never delete files. Each record and
// each header carries an xxhash64, verified on read/open to catch corruption.

const (
	headerSize    = 64 // [magic][cursors+counts][version][reserved][header checksum]
	checksumSize  = 8  // xxhash64 trailer per record
	formatVersion = 1
	hdrSumCovered = 56 // header bytes the header checksum is computed over ([0:56])
	filePrefix    = "data."
)

// headerMagic identifies a data file; mismatch means a foreign/garbage directory.
var headerMagic = binary.LittleEndian.Uint64([]byte("WALGOseg"))

type dataFile struct {
	num       uint64
	data      []byte // mmap of the whole file, or nil when not currently mapped
	base      int64  // global offset of this file's first data byte
	size      int64  // bytes of records written into the data region (excludes header)
	written   int64  // number of records written (mirrors the header)
	committed int64  // number of records committed (mirrors the header)
}

// Header layout (little-endian): [0:8] magic, [8:16] commit cursor, [16:24] write
// cursor, [24:32] written count, [32:40] committed count, [40] version, [41:56]
// reserved, [56:64] xxhash64 of [0:56]. The checksum is rewritten on every header
// update so torn/rotten headers are caught on open.
func (df *dataFile) magic() uint64           { return binary.LittleEndian.Uint64(df.data[0:8]) }
func (df *dataFile) version() byte           { return df.data[40] }
func (df *dataFile) commitCursor() int64     { return int64(binary.LittleEndian.Uint64(df.data[8:16])) }
func (df *dataFile) setCommitCursor(v int64) { binary.LittleEndian.PutUint64(df.data[8:16], uint64(v)) }
func (df *dataFile) writeCursor() int64      { return int64(binary.LittleEndian.Uint64(df.data[16:24])) }
func (df *dataFile) setWriteCursor(v int64)  { binary.LittleEndian.PutUint64(df.data[16:24], uint64(v)) }

func (df *dataFile) writtenCount() int64 { return int64(binary.LittleEndian.Uint64(df.data[24:32])) }
func (df *dataFile) setWrittenCount(v int64) {
	binary.LittleEndian.PutUint64(df.data[24:32], uint64(v))
}
func (df *dataFile) committedCount() int64 { return int64(binary.LittleEndian.Uint64(df.data[32:40])) }
func (df *dataFile) setCommittedCount(v int64) {
	binary.LittleEndian.PutUint64(df.data[32:40], uint64(v))
}

// initHeader stamps the magic and version into a fresh file.
func (df *dataFile) initHeader() {
	binary.LittleEndian.PutUint64(df.data[0:8], headerMagic)
	df.data[40] = formatVersion
}

// setHeaderChecksum recomputes the header checksum; call after any header update,
// before the msync that persists it.
func (df *dataFile) setHeaderChecksum() {
	binary.LittleEndian.PutUint64(df.data[56:64], xxhash.Sum64(df.data[:hdrSumCovered]))
}

func (df *dataFile) headerChecksumOK() bool {
	return binary.LittleEndian.Uint64(df.data[56:64]) == xxhash.Sum64(df.data[:hdrSumCovered])
}

// store is the raw, []byte-oriented file backend. Not safe for concurrent use;
// the WAL serializes access with its own mutex.
type store struct {
	dir            string
	segmentSize    int64 // capacity of each file's data region (excludes header)
	maxSegments    int   // max number of data files retained at once; 0 == unbounded
	noSync         bool
	syncEvery      int  // msync every N writes/commits; <=1 means every one
	maxMapped      int  // cap on simultaneously mapped segments; 0 == unbounded
	recoverCorrupt bool // drop torn tails / skip corrupt segments instead of erroring
	pageSize       int64

	files   []*dataFile // sorted by num ascending; last is the active write file
	mapped  []*dataFile // currently mapped files, least-recently-used first
	nextNum uint64

	writeOff  int64 // global offset of the next record to write (tail)
	headOff   int64 // global offset of the next record to read (in memory only)
	commitOff int64 // global offset of the next record to commit (persisted)

	nWritten   int64 // total records appended
	nCommitted int64 // total records committed

	unsynced    int   // writes/commits accumulated since the last batched flush
	corruptions int64 // corruption events recovered from (torn tails + skipped segments)
}

func openStore(dir string, segmentSize int64, maxSegments int, noSync bool, syncEvery, maxMapped int, recoverCorrupt bool) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if maxMapped > 0 && maxMapped < 2 {
		maxMapped = 2 // need the active file plus the one being read mapped at once
	}
	s := &store{
		dir:            dir,
		segmentSize:    segmentSize,
		maxSegments:    maxSegments,
		noSync:         noSync,
		syncEvery:      syncEvery,
		maxMapped:      maxMapped,
		recoverCorrupt: recoverCorrupt,
		pageSize:       int64(os.Getpagesize()),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// mmapByNum opens segment num, maps it, and closes the fd (the mapping outlives
// the descriptor on Linux, so retained segments don't each hold an open file).
func (s *store) mmapByNum(num uint64) ([]byte, error) {
	f, err := os.OpenFile(s.filePath(num), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	data, err := mmapFile(f, int(headerSize+s.segmentSize))
	_ = f.Close()
	return data, err
}

// ensureMapped maps df if needed and marks it most-recently-used; the active file
// stays mapped because every append touches it.
func (s *store) ensureMapped(df *dataFile) error {
	if df.data != nil {
		s.touchMapped(df)
		return nil
	}
	data, err := s.mmapByNum(df.num)
	if err != nil {
		return err
	}
	df.data = data
	s.trackMapped(df)
	return nil
}

// trackMapped records df as mapped (most-recently-used) and evicts down to the cap.
func (s *store) trackMapped(df *dataFile) {
	s.mapped = append(s.mapped, df)
	s.evictMapped(df)
}

func (s *store) touchMapped(df *dataFile) {
	for i, d := range s.mapped {
		if d == df {
			s.mapped = append(s.mapped[:i], s.mapped[i+1:]...)
			s.mapped = append(s.mapped, df)
			return
		}
	}
}

func (s *store) removeMapped(df *dataFile) {
	for i, d := range s.mapped {
		if d == df {
			s.mapped = append(s.mapped[:i], s.mapped[i+1:]...)
			return
		}
	}
}

// evictMapped unmaps least-recently-used segments until at most maxMapped remain,
// never evicting the active file or keep (the one just mapped). A victim's header
// may be dirty (a batched commit), so flush it before unmapping.
func (s *store) evictMapped(keep *dataFile) {
	if s.maxMapped <= 0 {
		return
	}
	active := s.active()
	for len(s.mapped) > s.maxMapped {
		victim := -1
		for i, df := range s.mapped {
			if df != active && df != keep {
				victim = i
				break
			}
		}
		if victim < 0 {
			return // only the active and just-mapped files remain
		}
		df := s.mapped[victim]
		if !s.noSync {
			_ = unix.Msync(df.data, unix.MS_SYNC)
		}
		_ = unix.Munmap(df.data)
		df.data = nil
		s.mapped = append(s.mapped[:victim], s.mapped[victim+1:]...)
	}
}

// batched reports whether the sync policy defers msync to a periodic flush
// rather than syncing after every write/commit.
func (s *store) batched() bool { return !s.noSync && s.syncEvery > 1 }

// recordOp counts one durable operation (a write or a commit) and flushes every
// segment once syncEvery have accumulated. Used only on the batched path.
func (s *store) recordOp() {
	s.unsynced++
	if s.unsynced >= s.syncEvery {
		s.flushBatch()
	}
}

// flushBatch msyncs every mapped file (dirty pages only) and resets the counter.
// A torn tail from a power loss between flushes is caught by the record checksum.
func (s *store) flushBatch() {
	for _, df := range s.files {
		if df.data != nil {
			_ = unix.Msync(df.data, unix.MS_SYNC)
		}
	}
	s.unsynced = 0
}

func (s *store) filePath(num uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s%08d", filePrefix, num))
}

// load opens the existing files (or creates the first) and recovers the cursors.
// The read cursor resets to the commit cursor, so uncommitted records replay.
func (s *store) load() error {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var nums []uint64
	for _, e := range ents {
		if e.IsDir() || !strings.HasPrefix(e.Name(), filePrefix) {
			continue
		}
		num, perr := strconv.ParseUint(e.Name()[len(filePrefix):], 10, 64)
		if perr != nil {
			continue
		}
		nums = append(nums, num)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	if len(nums) == 0 {
		df, err := s.createFile(1, 0)
		if err != nil {
			return err
		}
		s.files = []*dataFile{df}
		s.trackMapped(df)
		s.nextNum = 2
		if !s.noSync {
			if err := s.syncDir(); err != nil {
				return err
			}
		}
		return nil
	}

	// Files are preallocated to headerSize+segmentSize, so the largest reveals
	// the stored segment size; reopening with a different size would discard
	// records. Reject it. (Max ignores torn files.)
	var storedFileSize int64
	for _, num := range nums {
		fi, serr := os.Stat(s.filePath(num))
		if serr != nil {
			return serr
		}
		if fi.Size() > storedFileSize {
			storedFileSize = fi.Size()
		}
	}
	if storedFileSize > 0 && storedFileSize != headerSize+s.segmentSize {
		return fmt.Errorf("%w: store created with segment size %d, opened with %d",
			ErrSegmentSizeMismatch, storedFileSize-headerSize, s.segmentSize)
	}

	// Recover from each file's header alone (no record scan, no mapping): read the
	// 64-byte header with pread, validate it, and cache the cursors/counts.
	s.nextNum = nums[len(nums)-1] + 1
	var base int64
	commitCurs := make([]int64, 0, len(nums))
	for idx, num := range nums {
		isLast := idx == len(nums)-1
		h, herr := s.readHeader(num)
		var verr error
		var hdr *dataFile
		if herr != nil {
			verr = herr
		} else {
			hdr = &dataFile{data: h}
			switch {
			case hdr.magic() != headerMagic || hdr.version() != formatVersion:
				verr = fmt.Errorf("%w: %s", ErrBadFormat, s.filePath(num))
			case !hdr.headerChecksumOK():
				verr = fmt.Errorf("%w: header of %s", ErrCorrupt, s.filePath(num))
			}
		}
		if verr != nil {
			// Only the highest-numbered segment may be a torn tail from a crash
			// mid-cycle; with recovery enabled, drop it (it holds only being-written
			// data — earlier segments carry all committed records). Anything else is
			// a hard error.
			if s.recoverCorrupt && isLast {
				_ = os.Remove(s.filePath(num))
				s.corruptions++
				if !s.noSync {
					_ = s.syncDir()
				}
				break
			}
			return verr
		}
		w := hdr.writeCursor()
		if w < headerSize {
			w = headerSize
		}
		if w > headerSize+s.segmentSize {
			w = headerSize + s.segmentSize
		}
		df := &dataFile{num: num, base: base, size: w - headerSize}
		df.written = max64(hdr.writtenCount(), 0)
		df.committed = hdr.committedCount()
		if df.committed < 0 {
			df.committed = 0
		}
		if df.committed > df.written {
			df.committed = df.written
		}
		cc := hdr.commitCursor()
		if cc < headerSize {
			cc = headerSize
		}
		if cc > headerSize+df.size {
			cc = headerSize + df.size
		}
		commitCurs = append(commitCurs, cc)
		base += df.size
		s.nWritten += df.written
		s.nCommitted += df.committed
		s.files = append(s.files, df)
	}

	// Every segment was a dropped torn tail: start fresh like an empty directory.
	if len(s.files) == 0 {
		df, err := s.createFile(s.nextNum, 0)
		if err != nil {
			return err
		}
		s.nextNum++
		s.files = append(s.files, df)
		s.trackMapped(df)
		if !s.noSync {
			return s.syncDir()
		}
		return nil
	}

	s.writeOff = base

	// Commit cursor: the first file whose commit cursor is short of its end.
	s.commitOff = s.writeOff
	for i, df := range s.files {
		if commitCurs[i] < headerSize+df.size {
			s.commitOff = df.base + (commitCurs[i] - headerSize)
			break
		}
	}
	s.headOff = s.commitOff
	// Map the active file so appends can write into it; the rest map on demand.
	return s.ensureMapped(s.active())
}

// readHeader preads a file's fixed-size header without mapping it.
func (s *store) readHeader(num uint64) ([]byte, error) {
	f, err := os.Open(s.filePath(num))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	h := make([]byte, headerSize)
	if _, err := io.ReadFull(f, h); err != nil {
		return nil, fmt.Errorf("%w: reading header of %s: %v", ErrCorrupt, s.filePath(num), err)
	}
	return h, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (s *store) createFile(num uint64, base int64) (*dataFile, error) {
	f, err := os.OpenFile(s.filePath(num), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(headerSize + s.segmentSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	data, err := mmapFile(f, int(headerSize+s.segmentSize))
	_ = f.Close() // the mapping outlives the descriptor
	if err != nil {
		return nil, err
	}
	df := &dataFile{num: num, data: data, base: base}
	df.initHeader()
	df.setCommitCursor(headerSize)
	df.setWriteCursor(headerSize)
	df.setHeaderChecksum()
	// Persist the header so a freshly cycled segment is a valid file on disk
	// (magic/checksum) even before its first record is written.
	if !s.noSync {
		_ = unix.Msync(df.data, unix.MS_SYNC)
	}
	return df, nil
}

func mmapFile(f *os.File, length int) ([]byte, error) {
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
}

func (s *store) active() *dataFile {
	if len(s.files) == 0 {
		return nil
	}
	return s.files[len(s.files)-1]
}

// append writes payload as a new record at the tail, cycling to a new file when
// the active one is full.
func (s *store) append(payload []byte) error {
	L := len(payload)
	recLen := int64(uvarintLen(uint64(L)) + L + checksumSize)
	if recLen > s.segmentSize {
		return ErrRecordTooLarge
	}

	af := s.active()
	if af == nil || af.size+recLen > s.segmentSize {
		if err := s.cycle(); err != nil {
			return err
		}
		af = s.active()
	}
	// The active file stays mapped; this also marks it most-recently-used so the
	// LRU never evicts it.
	if err := s.ensureMapped(af); err != nil {
		return err
	}

	p := headerSize + int(af.size)
	m := binary.PutUvarint(af.data[p:], uint64(L))
	copy(af.data[p+m:], payload)
	binary.LittleEndian.PutUint64(af.data[p+m+L:], xxhash.Sum64(payload))
	old := af.size
	af.size += recLen
	af.written++
	s.writeOff += recLen
	s.nWritten++
	// Publish the data end and count after the record bytes, so recovery only
	// sees fully-written records.
	af.setWriteCursor(headerSize + af.size)
	af.setWrittenCount(af.written)
	af.setHeaderChecksum()
	switch {
	case s.noSync:
	case s.batched():
		s.recordOp()
	default:
		s.msync(af, headerSize+int(old), headerSize+int(af.size)) // record bytes
		s.msync(af, 0, headerSize)                                // header: write cursor + written count
	}
	return nil
}

// cycle drops any now fully-committed files and starts a fresh active file. It
// fails with ErrFull if creating the new file would exceed maxSegments.
func (s *store) cycle() error {
	s.dropCommitted()
	if s.maxSegments > 0 && len(s.files) >= s.maxSegments {
		return ErrFull
	}
	df, err := s.createFile(s.nextNum, s.writeOff)
	if err != nil {
		return err
	}
	s.nextNum++
	s.files = append(s.files, df)
	s.trackMapped(df)
	// Persist the new (and removed) entries before records land in the file.
	if !s.noSync {
		if err := s.syncDir(); err != nil {
			return err
		}
	}
	return nil
}

// dropCommitted removes (and unmaps) every fully-committed file — the only place
// files are deleted. Called from cycle under the WAL lock, so no store op races
// it. A just-delivered record's file may be unmapped here; that's safe only
// because read copied the payload into the Reader's scratch under the lock.
func (s *store) dropCommitted() {
	keep := s.files[:0]
	for _, df := range s.files {
		if df.base+df.size <= s.commitOff {
			// written == committed here, so this keeps Count exact.
			s.nWritten -= df.written
			s.nCommitted -= df.committed
			if df.data != nil {
				_ = unix.Munmap(df.data)
				df.data = nil
				s.removeMapped(df)
			}
			_ = os.Remove(s.filePath(df.num))
			continue
		}
		keep = append(keep, df)
	}
	s.files = keep
}

// fileForOffset returns the file holding the record that starts at the global
// offset off (base <= off < base+size).
func (s *store) fileForOffset(off int64) *dataFile {
	for _, df := range s.files {
		if off >= df.base && off < df.base+df.size {
			return df
		}
	}
	return nil
}

// recordAt decodes the record at off (which must lie in this file), returning its
// payload (a slice into the mmap), the stored payload checksum, the offset past
// the record, and whether it decoded.
func (df *dataFile) recordAt(off int64) ([]byte, uint64, int64, bool) {
	p := headerSize + int(off-df.base)
	if p < headerSize || p >= len(df.data) {
		return nil, 0, 0, false
	}
	v, n := binary.Uvarint(df.data[p:])
	if n <= 0 {
		return nil, 0, 0, false
	}
	L := int(v)
	start := p + n
	// A corrupt length must decode as "not ok", never panic (L wraps negative
	// when v exceeds maxInt). Need room for the payload and its checksum trailer.
	if L < 0 || L > len(df.data)-start-checksumSize {
		return nil, 0, 0, false
	}
	sum := binary.LittleEndian.Uint64(df.data[start+L:])
	return df.data[start : start+L], sum, off + int64(n+L+checksumSize), true
}

// read locates and decodes the record at global offset off, mapping its file on
// demand. ok is false only at the tail (off >= writeOff); a record that should be
// present but won't decode returns ErrCorrupt (distinct from empty). A mapping
// failure is returned as its own error.
func (s *store) read(off int64) ([]byte, uint64, int64, bool, error) {
	if off >= s.writeOff {
		return nil, 0, 0, false, nil
	}
	df := s.fileForOffset(off)
	if df == nil {
		return nil, 0, 0, false, ErrCorrupt
	}
	if err := s.ensureMapped(df); err != nil {
		return nil, 0, 0, false, err
	}
	p, sum, next, ok := df.recordAt(off)
	if !ok {
		return nil, 0, 0, false, ErrCorrupt
	}
	return p, sum, next, true, nil
}

// takeHead reads the record at the head cursor, verifies its checksum, and
// advances. By default a corrupt record (bad length or checksum) returns
// ErrCorrupt without advancing, so it surfaces on every read. With recoverCorrupt
// the affected segment's remainder is quarantined and the next valid record is
// returned instead.
func (s *store) takeHead() ([]byte, int64, bool, error) {
	for {
		payload, sum, next, ok, err := s.read(s.headOff)
		if err != nil {
			if errors.Is(err, ErrCorrupt) && s.recoverCorrupt {
				s.skipCorruptSegment(s.headOff)
				continue
			}
			return nil, 0, false, err
		}
		if !ok {
			return nil, 0, false, nil // empty
		}
		if xxhash.Sum64(payload) != sum {
			if !s.recoverCorrupt {
				return nil, 0, false, ErrCorrupt
			}
			s.skipCorruptSegment(s.headOff)
			continue
		}
		s.headOff = next
		return payload, next, true, nil
	}
}

// skipCorruptSegment quarantines the rest of the segment holding off: it advances
// the read cursor past the segment, and — when the commit cursor is already
// within that segment (the auto-committing read path) — force-commits the
// abandoned tail so it is reclaimed and never replayed. Corrupt records there are
// dropped (recovery is inherently lossy); each call counts one event.
func (s *store) skipCorruptSegment(off int64) {
	s.corruptions++
	df := s.fileForOffset(off)
	if df == nil {
		s.headOff = s.writeOff
		return
	}
	end := df.base + df.size
	if s.commitOff >= df.base {
		if abandoned := df.written - df.committed; abandoned > 0 {
			s.nCommitted += abandoned
			df.committed = df.written
		}
		if s.commitOff < end {
			s.commitOff = end
		}
		if df.data != nil {
			df.setCommitCursor(headerSize + df.size)
			df.setCommittedCount(df.committed)
			df.setHeaderChecksum()
			if !s.noSync {
				s.msync(df, 0, headerSize)
			}
		}
	}
	if s.headOff < end {
		s.headOff = end
	}
}

// commitTo advances the commit cursor to off, counting the records crossed and
// persisting the cursor and count into each file's header.
func (s *store) commitTo(off int64) {
	if off <= s.commitOff {
		return
	}
	if off > s.writeOff {
		off = s.writeOff
	}
	// Per-op policy flushes each file's header once, not once per record: commits
	// cross files in order, so flush a file when the commit leaves it, and the
	// last at the end. A crash before the flush replays the batch (at-least-once).
	perOp := !s.noSync && !s.batched()
	var dirty *dataFile
	for s.commitOff < off {
		df := s.fileForOffset(s.commitOff)
		if df == nil {
			break
		}
		if err := s.ensureMapped(df); err != nil {
			break // can't map the file to advance the cursor; replay later
		}
		_, _, next, ok := df.recordAt(s.commitOff)
		if !ok {
			break
		}
		if perOp && dirty != nil && dirty != df {
			s.msync(dirty, 0, headerSize)
		}
		s.commitOff = next
		s.nCommitted++
		df.committed++
		df.setCommitCursor(headerSize + (s.commitOff - df.base))
		df.setCommittedCount(df.committed)
		df.setHeaderChecksum()
		dirty = df
	}
	if dirty == nil {
		return // nothing committed
	}
	if perOp {
		s.msync(dirty, 0, headerSize)
	} else if s.batched() {
		s.recordOp()
	}
}

func (s *store) empty() bool            { return s.headOff >= s.writeOff }
func (s *store) size() int64            { return s.writeOff - s.commitOff }
func (s *store) count() int64           { return s.nWritten - s.nCommitted }
func (s *store) writeOffset() int64     { return s.writeOff }
func (s *store) headOffset() int64      { return s.headOff }
func (s *store) corruptionCount() int64 { return s.corruptions }

func (s *store) sync() error {
	s.unsynced = 0 // a full flush makes any batched-but-unsynced ops durable
	for _, df := range s.files {
		if df.data == nil {
			continue // unmapped segments hold no dirty pages
		}
		if err := unix.Msync(df.data, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return nil
}

// syncDir fsyncs the directory so segment creations/removals are durable: msync
// flushes a file's data and inode but never its directory entry, which a power
// loss would otherwise drop — stranding already-msync'd records.
func (s *store) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

func (s *store) close() error {
	var first error
	for _, df := range s.files {
		if df.data == nil {
			continue // not currently mapped
		}
		if !s.noSync {
			if err := unix.Msync(df.data, unix.MS_SYNC); err != nil && first == nil {
				first = err
			}
		}
		if err := unix.Munmap(df.data); err != nil && first == nil {
			first = err
		}
		df.data = nil
	}
	s.files = nil
	s.mapped = nil
	return first
}

// msync flushes [from,to) of df, aligning the start down to a page boundary. A
// no-op if df is currently unmapped (e.g. evicted after a batched commit, in
// which case eviction already flushed it).
func (s *store) msync(df *dataFile, from, to int) {
	if df.data == nil {
		return
	}
	start := from - from%int(s.pageSize)
	if start < 0 {
		start = 0
	}
	_ = unix.Msync(df.data[start:to], unix.MS_SYNC)
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
