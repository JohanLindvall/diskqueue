package diskqueue

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
//
// I/O is plain pread/pwrite/fsync (no mmap): records are written with WriteAt and
// read back with ReadAt into reused buffers, and durability is fsync. Each file's
// 64-byte header is kept resident in memory (dataFile.hdr) and written to its page
// 0 with WriteAt; recovery reads it back with a bare pread.

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
	f         *os.File // open handle, or nil when not currently open
	hdr       []byte   // resident copy of the 64-byte header (page 0)
	base      int64    // global offset of this file's first data byte
	size      int64    // bytes of records written into the data region (excludes header)
	written   int64    // number of records written (mirrors the header)
	committed int64    // number of records committed (mirrors the header)

	// Intrusive LRU links, valid only while open (f != nil). The store threads
	// open files from mru (most-recently-used) toward lru via prev.
	lruPrev *dataFile // toward the most-recently-used end
	lruNext *dataFile // toward the least-recently-used end

	// dirty is set when the file has page-cache writes (record bytes and/or header)
	// not yet fsync'd, so the batched/evict/sync/close paths fsync only files that
	// need it and skip a file that was merely read since its last flush.
	dirty bool
}

// Header layout (little-endian): [0:8] magic, [8:16] commit cursor, [16:24] write
// cursor, [24:32] written count, [32:40] committed count, [40] version, [41:56]
// reserved, [56:64] xxhash64 of [0:56]. The checksum is rewritten on every header
// update so torn/rotten headers are caught on open.
func (df *dataFile) magic() uint64         { return binary.LittleEndian.Uint64(df.hdr[0:8]) }
func (df *dataFile) version() byte         { return df.hdr[40] }
func (df *dataFile) commitCursor() int64   { return int64(binary.LittleEndian.Uint64(df.hdr[8:16])) }
func (df *dataFile) writeCursor() int64    { return int64(binary.LittleEndian.Uint64(df.hdr[16:24])) }
func (df *dataFile) writtenCount() int64   { return int64(binary.LittleEndian.Uint64(df.hdr[24:32])) }
func (df *dataFile) committedCount() int64 { return int64(binary.LittleEndian.Uint64(df.hdr[32:40])) }

// The setters return a header modifier (a func(*dataFile)) rather than writing
// in place, so they compose as arguments to header(), which applies them and
// then rebuilds the checksum. They write nothing until header() invokes them.
func setCommitCursor(v int64) func(*dataFile) {
	return func(df *dataFile) { binary.LittleEndian.PutUint64(df.hdr[8:16], uint64(v)) }
}
func setWriteCursor(v int64) func(*dataFile) {
	return func(df *dataFile) { binary.LittleEndian.PutUint64(df.hdr[16:24], uint64(v)) }
}
func setWrittenCount(v int64) func(*dataFile) {
	return func(df *dataFile) { binary.LittleEndian.PutUint64(df.hdr[24:32], uint64(v)) }
}
func setCommittedCount(v int64) func(*dataFile) {
	return func(df *dataFile) { binary.LittleEndian.PutUint64(df.hdr[32:40], uint64(v)) }
}

// initHeader stamps the magic and version into a fresh header.
func (df *dataFile) initHeader() {
	binary.LittleEndian.PutUint64(df.hdr[0:8], headerMagic)
	df.hdr[40] = formatVersion
}

// header applies field mutations to the in-memory header, then rebuilds the
// checksum. Every header change goes through here so the checksum can't be
// forgotten. The bytes are persisted separately by writeHeader; the durability
// (fsync) is left to the caller per the sync policy.
func (df *dataFile) header(mods ...func(*dataFile)) {
	for _, mod := range mods {
		mod(df)
	}
	df.setHeaderChecksum()
}

// setHeaderChecksum recomputes the header checksum; call after any header update,
// before the write that persists it.
func (df *dataFile) setHeaderChecksum() {
	binary.LittleEndian.PutUint64(df.hdr[56:64], xxhash.Sum64(df.hdr[:hdrSumCovered]))
}

func (df *dataFile) headerChecksumOK() bool {
	return binary.LittleEndian.Uint64(df.hdr[56:64]) == xxhash.Sum64(df.hdr[:hdrSumCovered])
}

// store is the raw, []byte-oriented file backend. Not safe for concurrent use;
// the DiskQueue serializes access with its own mutex.
type store struct {
	dir            string
	segmentSize    int64 // capacity of each file's data region (excludes header)
	maxSegments    int   // max number of data files retained at once; 0 == unbounded
	noSync         bool
	syncEvery      int  // fsync every N writes/commits; <=1 means every one
	maxMapped      int  // cap on simultaneously open segment files; 0 == unbounded
	recoverCorrupt bool // drop torn tails / skip corrupt segments instead of erroring

	files   []*dataFile // sorted by num ascending; last is the active write file
	nextNum uint64

	// Intrusive LRU list of currently open files, so touch/evict/remove are O(1)
	// pointer splices rather than O(n) slice shifts. mappedMRU is the
	// most-recently-used end (where touches and new opens go); mappedLRU is the
	// eviction end. mappedLen tracks the length against maxMapped.
	mappedMRU *dataFile
	mappedLRU *dataFile
	mappedLen int

	// Reused I/O buffers: writeBuf frames a record before a single WriteAt; readBuf
	// receives a record (or just its length prefix) on ReadAt. Reusing them keeps
	// append/read alloc-free once warm.
	writeBuf []byte
	readBuf  []byte

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
		maxMapped = 2 // need the active file plus the one being read open at once
	}
	s := &store{
		dir:            dir,
		segmentSize:    segmentSize,
		maxSegments:    maxSegments,
		noSync:         noSync,
		syncEvery:      syncEvery,
		maxMapped:      maxMapped,
		recoverCorrupt: recoverCorrupt,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// growBuf returns b resized to length n, allocating a new backing array only when
// the current capacity is too small (so a warm buffer never allocates).
func growBuf(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

// ensureMapped opens df's file if needed and marks it most-recently-used; the
// active file stays open because every append touches it.
func (s *store) ensureMapped(df *dataFile) error {
	if df.f != nil {
		s.touchMapped(df)
		return nil
	}
	f, err := os.OpenFile(s.filePath(df.num), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	df.f = f
	s.trackMapped(df)
	return nil
}

// trackMapped records df as open (most-recently-used) and evicts down to the cap.
func (s *store) trackMapped(df *dataFile) {
	s.mappedPushMRU(df)
	s.evictMapped(df)
}

// touchMapped moves an already-open df to the most-recently-used end.
func (s *store) touchMapped(df *dataFile) {
	if df == s.mappedMRU {
		return
	}
	s.mappedUnlink(df)
	s.mappedPushMRU(df)
}

// removeMapped detaches df from the LRU list (its file is being closed/removed).
func (s *store) removeMapped(df *dataFile) {
	s.mappedUnlink(df)
}

// mappedPushMRU links df in at the most-recently-used end. df must not already
// be in the list.
func (s *store) mappedPushMRU(df *dataFile) {
	df.lruPrev = nil
	df.lruNext = s.mappedMRU
	if s.mappedMRU != nil {
		s.mappedMRU.lruPrev = df
	} else {
		s.mappedLRU = df
	}
	s.mappedMRU = df
	s.mappedLen++
}

// mappedUnlink removes df from the LRU list and clears its links.
func (s *store) mappedUnlink(df *dataFile) {
	if df.lruPrev != nil {
		df.lruPrev.lruNext = df.lruNext
	} else {
		s.mappedMRU = df.lruNext
	}
	if df.lruNext != nil {
		df.lruNext.lruPrev = df.lruPrev
	} else {
		s.mappedLRU = df.lruPrev
	}
	df.lruPrev, df.lruNext = nil, nil
	s.mappedLen--
}

// evictMapped closes least-recently-used files until at most maxMapped remain
// open, never closing the active file or keep (the one just opened). A dirty
// victim is fsync'd before its handle is closed; under noSync the dirty flag is
// just cleared (kernel writeback covers the page-cache bytes).
func (s *store) evictMapped(keep *dataFile) {
	if s.maxMapped <= 0 {
		return
	}
	active := s.active()
	for s.mappedLen > s.maxMapped {
		// Walk from the least-recently-used end toward the most-recently-used,
		// skipping the active and just-opened files (which are never evicted).
		var victim *dataFile
		for df := s.mappedLRU; df != nil; df = df.lruPrev {
			if df != active && df != keep {
				victim = df
				break
			}
		}
		if victim == nil {
			return // only the active and just-opened files remain
		}
		if !s.noSync {
			_ = s.flushFile(victim) // fsync if dirty, then close
		} else {
			victim.dirty = false // noSync: kernel writeback covers the dirty pages
		}
		_ = victim.f.Close()
		victim.f = nil
		s.mappedUnlink(victim)
	}
}

// batched reports whether the sync policy defers fsync to a periodic flush
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

// flushBatch fsyncs each dirty file and resets the counter. A torn tail from a
// power loss between flushes is caught by the record checksum.
func (s *store) flushBatch() {
	for _, df := range s.files {
		_ = s.flushFile(df)
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

	// Recover from each file's header alone (no record scan, no open handle): read
	// the 64-byte header with pread, validate it, and cache the cursors/counts.
	s.nextNum = nums[len(nums)-1] + 1
	var base int64
	commitCurs := make([]int64, 0, len(nums))
	for idx, num := range nums {
		isLast := idx == len(nums)-1
		h, herr := s.readHeader(num)
		var verr error
		var th *dataFile
		if herr != nil {
			verr = herr
		} else {
			th = &dataFile{hdr: h}
			switch {
			case th.magic() != headerMagic || th.version() != formatVersion:
				verr = fmt.Errorf("%w: %s", ErrBadFormat, s.filePath(num))
			case !th.headerChecksumOK():
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
		w := th.writeCursor()
		if w < headerSize {
			w = headerSize
		}
		if w > headerSize+s.segmentSize {
			w = headerSize + s.segmentSize
		}
		df := &dataFile{num: num, hdr: h, base: base, size: w - headerSize}
		df.written = max64(th.writtenCount(), 0)
		df.committed = th.committedCount()
		if df.committed < 0 {
			df.committed = 0
		}
		if df.committed > df.written {
			df.committed = df.written
		}
		cc := th.commitCursor()
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
	// Open the active file so appends can write into it; the rest open on demand.
	return s.ensureMapped(s.active())
}

// readHeader preads a file's fixed-size header without keeping the handle.
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
	df := &dataFile{num: num, f: f, hdr: make([]byte, headerSize), base: base}
	df.header(
		(*dataFile).initHeader,
		setCommitCursor(headerSize),
		setWriteCursor(headerSize),
	)
	// Persist the header so a freshly cycled segment is a valid file on disk
	// (magic/checksum) even before its first record is written.
	if err := s.writeHeader(df); err != nil {
		_ = f.Close()
		return nil, err
	}
	if !s.noSync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, err
		}
		df.dirty = false
	}
	// else: writeHeader left it dirty so an explicit Sync flushes the fresh header.
	return df, nil
}

func (s *store) active() *dataFile {
	if len(s.files) == 0 {
		return nil
	}
	return s.files[len(s.files)-1]
}

// writeHeader writes df's resident header to its page 0 (page cache, not yet
// durable) and marks the file dirty.
func (s *store) writeHeader(df *dataFile) error {
	if _, err := df.f.WriteAt(df.hdr, 0); err != nil {
		return err
	}
	df.dirty = true
	return nil
}

// writeRecord frames payload (uvarint length, payload, checksum) into the reused
// writeBuf and writes it at data offset off with a single WriteAt.
func (s *store) writeRecord(df *dataFile, off int64, payload []byte) error {
	L := len(payload)
	total := uvarintLen(uint64(L)) + L + checksumSize
	s.writeBuf = growBuf(s.writeBuf, total)
	n := binary.PutUvarint(s.writeBuf, uint64(L))
	copy(s.writeBuf[n:], payload)
	binary.LittleEndian.PutUint64(s.writeBuf[n+L:], xxhash.Sum64(payload))
	if _, err := df.f.WriteAt(s.writeBuf[:total], headerSize+off); err != nil {
		return err
	}
	df.dirty = true
	return nil
}

// flushFile fsyncs df if it has unsynced writes, then marks it clean. No-op for a
// closed or already-clean file.
func (s *store) flushFile(df *dataFile) error {
	if df.f == nil || !df.dirty {
		return nil
	}
	if err := df.f.Sync(); err != nil {
		return err
	}
	df.dirty = false
	return nil
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
	// The active file stays open; this also marks it most-recently-used so the
	// LRU never evicts it.
	if err := s.ensureMapped(af); err != nil {
		return err
	}

	if err := s.writeRecord(af, af.size, payload); err != nil {
		return err
	}
	af.size += recLen
	af.written++
	s.writeOff += recLen
	s.nWritten++
	// Update the header (write cursor + count) in memory; it is published to disk
	// after the record bytes so recovery only sees fully-written records.
	af.header(
		setWriteCursor(headerSize+af.size),
		setWrittenCount(af.written),
	)
	switch {
	case s.noSync:
		// No fsync; the record and header sit in the page cache and an explicit
		// Sync/Close flushes them.
		return s.writeHeader(af)
	case s.batched():
		if err := s.writeHeader(af); err != nil {
			return err
		}
		s.recordOp()
		return nil
	default:
		// Per-op: fsync the record bytes, then write and fsync the header. Syncing
		// the data first guarantees a crash can only ever lose the header update (a
		// clean truncation), never leave a published record whose payload never
		// landed.
		if err := af.f.Sync(); err != nil {
			return err
		}
		if err := s.writeHeader(af); err != nil {
			return err
		}
		if err := af.f.Sync(); err != nil {
			return err
		}
		af.dirty = false
		return nil
	}
}

// cycle drops any now fully-committed files and starts a fresh active file. It
// fails with ErrFull if creating the new file would exceed maxSegments.
func (s *store) cycle() error {
	s.dropCommitted(nil) // the soon-to-be-old active file may go; a new one follows
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

// dropCommitted removes (and closes) every fully-committed file except keep.
// Called from cycle (writes) with keep == nil — it recreates the active file
// right after, so the old full one may go — and from commitTo (commits) with
// keep == the active file, which holds the write position and must survive even
// when fully drained. Both run under the DiskQueue lock, so no store op races it. A
// just-delivered record's file may be closed here; that's safe only because
// read copied the payload into the Reader's scratch under the lock.
func (s *store) dropCommitted(keep *dataFile) {
	survive := s.files[:0]
	for _, df := range s.files {
		if df != keep && df.base+df.size <= s.commitOff {
			// written == committed here, so this keeps Count exact.
			s.nWritten -= df.written
			s.nCommitted -= df.committed
			if df.f != nil {
				_ = df.f.Close()
				df.f = nil
				s.removeMapped(df)
			}
			_ = os.Remove(s.filePath(df.num))
			continue
		}
		survive = append(survive, df)
	}
	s.files = survive
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

// recordAt preads the record at global offset off (which must lie in df) into the
// reused readBuf, returning its payload (a slice of readBuf, valid until the next
// read), the stored payload checksum, the offset past the record, and whether it
// decoded. A pread failure is returned as an error.
func (s *store) recordAt(df *dataFile, off int64) ([]byte, uint64, int64, bool, error) {
	dataOff := off - df.base
	if dataOff < 0 || dataOff >= df.size {
		return nil, 0, 0, false, nil
	}
	avail := df.size - dataOff

	// Read the length prefix (at most MaxVarintLen64 bytes, never past the data).
	hn := int64(binary.MaxVarintLen64)
	if hn > avail {
		hn = avail
	}
	s.readBuf = growBuf(s.readBuf, int(hn))
	if _, err := df.f.ReadAt(s.readBuf[:hn], headerSize+dataOff); err != nil {
		return nil, 0, 0, false, err
	}
	v, n := binary.Uvarint(s.readBuf[:hn])
	if n <= 0 {
		return nil, 0, 0, false, nil
	}
	L := int(v)
	// A corrupt length must decode as "not ok", never read past the data region
	// (L wraps negative when v exceeds maxInt).
	if L < 0 || int64(n+L+checksumSize) > avail {
		return nil, 0, 0, false, nil
	}
	total := n + L + checksumSize
	s.readBuf = growBuf(s.readBuf, total)
	if _, err := df.f.ReadAt(s.readBuf[:total], headerSize+dataOff); err != nil {
		return nil, 0, 0, false, err
	}
	sum := binary.LittleEndian.Uint64(s.readBuf[n+L : total])
	return s.readBuf[n : n+L], sum, off + int64(total), true, nil
}

// recordLen preads only the length prefix of the record at off, returning the
// offset past the record. Used by commitTo, which needs the record boundary but
// not the payload.
func (s *store) recordLen(df *dataFile, off int64) (int64, bool, error) {
	dataOff := off - df.base
	if dataOff < 0 || dataOff >= df.size {
		return 0, false, nil
	}
	avail := df.size - dataOff
	hn := int64(binary.MaxVarintLen64)
	if hn > avail {
		hn = avail
	}
	s.readBuf = growBuf(s.readBuf, int(hn))
	if _, err := df.f.ReadAt(s.readBuf[:hn], headerSize+dataOff); err != nil {
		return 0, false, err
	}
	v, n := binary.Uvarint(s.readBuf[:hn])
	if n <= 0 {
		return 0, false, nil
	}
	L := int(v)
	if L < 0 || int64(n+L+checksumSize) > avail {
		return 0, false, nil
	}
	return off + int64(n+L+checksumSize), true, nil
}

// read locates and decodes the record at global offset off, opening its file on
// demand. ok is false only at the tail (off >= writeOff); a record that should be
// present but won't decode returns ErrCorrupt (distinct from empty). An I/O
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
	p, sum, next, ok, err := s.recordAt(df, off)
	if err != nil {
		return nil, 0, 0, false, err
	}
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
		if err := s.ensureMapped(df); err == nil {
			df.header(
				setCommitCursor(headerSize+df.size),
				setCommittedCount(df.committed),
			)
			_ = s.writeHeader(df)
			if !s.noSync {
				_ = s.flushFile(df) // recovery wants this durable now
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
	var cur *dataFile
	for s.commitOff < off {
		df := s.fileForOffset(s.commitOff)
		if df == nil {
			break
		}
		if err := s.ensureMapped(df); err != nil {
			break // can't open the file to advance the cursor; replay later
		}
		next, ok, err := s.recordLen(df, s.commitOff)
		if err != nil || !ok {
			break
		}
		if cur != nil && cur != df {
			_ = s.writeHeader(cur) // previous file's header is final; persist it
			if perOp {
				_ = s.flushFile(cur)
			}
		}
		s.commitOff = next
		s.nCommitted++
		df.committed++
		// header() rebuilds the checksum in memory; the bytes are written once per
		// file (on leaving it, and the last below).
		df.header(
			setCommitCursor(headerSize+(s.commitOff-df.base)),
			setCommittedCount(df.committed),
		)
		cur = df
	}
	if cur == nil {
		return // nothing committed
	}
	_ = s.writeHeader(cur)
	if perOp {
		_ = s.flushFile(cur)
	} else if s.batched() {
		s.recordOp()
	}
	// Reclaim any files this commit fully drained, so a consume-only or producer-
	// stopped workload frees disk without waiting for the next append. Keep the
	// active file (it holds the write position); the directory entry removal is
	// not fsync'd here — a lingering file after a crash is re-dropped, never
	// re-delivered (its records stay committed), so reclamation is best-effort.
	s.dropCommitted(s.active())
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
		if err := s.flushFile(df); err != nil {
			return err
		}
	}
	return nil
}

// syncDir fsyncs the directory so segment creations/removals are durable: an
// fsync of a file flushes its data and inode but never its directory entry, which
// a power loss would otherwise drop — stranding already-fsync'd records.
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
		if df.f == nil {
			continue // not currently open
		}
		if !s.noSync {
			if err := s.flushFile(df); err != nil && first == nil {
				first = err
			}
		}
		df.dirty = false
		if err := df.f.Close(); err != nil && first == nil {
			first = err
		}
		df.f = nil
	}
	s.files = nil
	s.mappedMRU, s.mappedLRU, s.mappedLen = nil, nil, 0
	return first
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
