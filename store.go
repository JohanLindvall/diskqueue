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

// knownVersion reports whether this build can read a segment written in version
// v. Each segment carries its own version in its header, so a future bump can
// keep reading the segments already on disk by widening this rather than
// invalidating them; a segment naming a version outside the set is dropped as
// foreign at open instead of failing it.
func knownVersion(v byte) bool { return v == formatVersion }

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
	dir         string
	dirFile     *os.File // held open for the whole session: directory fsync + advisory lock
	segmentSize int64    // capacity of each file's data region (excludes header)
	maxSegments int      // max number of data files retained at once; 0 == unbounded
	noSync      bool
	syncEvery   int // fsync every N writes/commits; <=1 means every one
	maxMapped   int // cap on simultaneously open segment files; 0 == unbounded

	// ioErr latches the first fsync failure (see failIO). Once set, every
	// operation that would otherwise claim durability returns it.
	ioErr error

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
	unreclaimed int64 // fully-committed segments that would not unlink (retried later)

	// Loss accounting. Corruption is never allowed to wedge the queue, so the
	// only way a consumer learns what a bad byte cost is through these: each
	// event surfaces as one ErrCorrupt, and these carry the magnitude.
	//
	// pendingCorrupt is the backlog of losses that happened with no read to
	// report them — segments dropped at open, mostly. takeHead pays it down one
	// per call so each lost segment reaches the consumer exactly once.
	pendingCorrupt  int
	corruptions     int64
	lostBytes       uint64
	lostRecords     uint64
	lostSegments    uint64
	foreignSegments uint64
	foreignBytes    uint64
	discardedBytes  uint64

	nAdded     uint64 // records accepted by append
	nDelivered uint64 // records handed out by takeHead
	nFull      uint64 // appends refused with ErrFull
}

func openStore(dir string, segmentSize int64, maxSegments int, noSync bool, syncEvery, maxMapped int) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if maxMapped > 0 && maxMapped < 2 {
		maxMapped = 2 // need the active file plus the one being read open at once
	}
	s := &store{
		dir:         dir,
		segmentSize: segmentSize,
		maxSegments: maxSegments,
		noSync:      noSync,
		syncEvery:   syncEvery,
		maxMapped:   maxMapped,
	}
	// Hold the directory open for the session: it is both the handle the segment
	// creations/removals are fsync'd through and the thing the advisory lock hangs
	// on, so no other DiskQueue can write into the same directory.
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	s.dirFile = d
	if err := tryLockDir(d); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("%w: %s", err, dir)
	}
	if err := s.load(); err != nil {
		_ = s.close() // no half-open store: release the handles and the lock
		return nil, err
	}
	return s, nil
}

// failIO latches an unrecoverable durability failure. Only fsync failures land
// here: the kernel reports a writeback error once and then drops the dirty pages,
// so a retry can report success with the data already gone. Rather than claim a
// durability it cannot deliver, the store remembers the first such failure and
// every later append/commit/sync repeats it; the caller's recourse is to close
// and reopen. Write and open failures are *not* latched — they leave the store
// consistent and are safe to retry.
func (s *store) failIO(err error) error {
	if err == nil {
		return nil
	}
	if s.ioErr == nil {
		s.ioErr = fmt.Errorf("%w: %w", ErrIO, err)
	}
	return s.ioErr
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
// victim is fsync'd before its handle is closed (a failure there latches ioErr,
// so it is not lost); under noSync nothing is fsync'd and the victim stays
// marked dirty, so a later explicit Sync reopens it and flushes it rather than
// mistaking a closed handle for a clean file.
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
			// Latches into ioErr on failure; the handle still goes, because
			// holding it open would not make the lost writeback reappear.
			_ = s.flushFile(victim)
		}
		_ = victim.f.Close() // read-back errors, if any, are already accounted for
		victim.f = nil
		s.mappedUnlink(victim)
	}
}

// batched reports whether the sync policy defers fsync to a periodic flush
// rather than syncing after every write/commit.
func (s *store) batched() bool { return !s.noSync && s.syncEvery > 1 }

// recordOp counts one durable operation (a write or a commit) and flushes every
// segment once syncEvery have accumulated. Used only on the batched path.
func (s *store) recordOp() error {
	s.unsynced++
	if s.unsynced >= s.syncEvery {
		return s.flushBatch()
	}
	return nil
}

// flushBatch fsyncs each dirty file and resets the counter. A torn tail from a
// power loss between flushes is caught by the record checksum. The counter is
// only cleared when every file really flushed, so a failure retries on the next
// operation instead of booking the batch as durable.
func (s *store) flushBatch() error {
	var errs error
	for _, df := range s.files {
		errs = errors.Join(errs, s.flushFile(df))
	}
	if errs != nil {
		return errs
	}
	s.unsynced = 0
	return nil
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
		return s.startFresh(1)
	}

	// Recover from each file's header alone (no record scan, no open handle): read
	// the 64-byte header with pread, validate it, and cache the cursors/counts.
	s.nextNum = nums[len(nums)-1] + 1
	var base int64
	commitCurs := make([]int64, 0, len(nums))
	for _, num := range nums {
		df, cc, err := s.loadFile(num, base)
		if err != nil {
			return err
		}
		if df == nil {
			continue // dropped: unreadable, foreign, or an aborted create
		}
		commitCurs = append(commitCurs, cc)
		base += df.size
		s.nWritten += df.written
		s.files = append(s.files, df)
	}

	// Every segment was dropped. Resume numbering *past* the highest seen, never
	// back at 1: a reused number would name a segment a stale cursor or a lingering
	// unlink still refers to.
	if len(s.files) == 0 {
		return s.startFresh(s.nextNum)
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

	// Reconcile the per-file committed counts with the cursor we just recovered.
	// The counts are per-segment while the cursor is global, and a crash can leave
	// a segment's header claiming records that the (rewound) cursor will replay —
	// counting those as committed here and again when they are re-consumed drives
	// Count() negative. The cursor wins: everything below it is committed,
	// everything above it is not, and the segment straddling it keeps its header's
	// figure.
	s.nCommitted = 0
	for _, df := range s.files {
		switch {
		case df.base+df.size <= s.commitOff:
			df.committed = df.written
		case df.base >= s.commitOff:
			df.committed = 0
		}
		s.nCommitted += df.committed
	}

	// Open the active file so appends can write into it; the rest open on demand.
	if err := s.ensureMapped(s.active()); err != nil {
		return err
	}
	// A segment written by an older build — or while the filesystem had no
	// fallocate — is sparse, and the active one is the one about to be appended to.
	// Reserving it again is a no-op if it is already allocated. Deliberately
	// best-effort: making this fatal would stop a queue on a full disk from being
	// opened to drain it, which is exactly when opening it matters most.
	_ = preallocate(s.active().f, headerSize+s.segmentSize)
	return nil
}

// startFresh creates segment num as the sole (active) file, for an empty
// directory or one whose every segment turned out to be a dropped torn tail.
func (s *store) startFresh(num uint64) error {
	df, err := s.createFile(num, s.writeOff)
	if err != nil {
		return err
	}
	s.nextNum = num + 1
	s.files = append(s.files, df)
	s.trackMapped(df)
	if !s.noSync {
		return s.syncDir()
	}
	return nil
}

// loadFile recovers one segment from its 64-byte header, returning the file and
// its stored commit cursor. A nil *dataFile with a nil error means the segment
// was dropped, and the counters say why.
//
// A segment whose header cannot be believed is dropped wherever it sits in the
// sequence — not only at the tail — and the loss is counted and later reported as
// one ErrCorrupt. The alternative, failing the open, leaves a queue that cannot
// be started at all and whose intact segments are unreachable: corruption has to
// degrade to reported loss, or the recovery story stops at "restore a backup".
//
// What is *not* evidence of damage never licenses a delete: an error that merely
// says the file could not be read (EACCES, EMFILE, EIO) fails the open instead,
// and an unknown format version is dropped silently as foreign rather than
// counted as data loss.
func (s *store) loadFile(num uint64, base int64) (*dataFile, int64, error) {
	path := s.filePath(num)
	fi, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()

	// A zero-length segment is a create interrupted between linking the file and
	// writing its header. It cannot hold a record, so removing it loses nothing
	// and must not raise a data-loss event.
	if size == 0 {
		return nil, 0, s.removeFile(num)
	}

	h, herr := s.readHeader(num)
	if herr != nil {
		if !errors.Is(herr, ErrCorrupt) {
			return nil, 0, herr // could not look; not evidence of damage
		}
		return nil, 0, s.dropSegment(num, size, false)
	}
	th := &dataFile{hdr: h}
	switch {
	case th.magic() != headerMagic:
		return nil, 0, s.dropSegment(num, size, false)
	case !th.headerChecksumOK():
		return nil, 0, s.dropSegment(num, size, false)
	case !knownVersion(th.version()):
		// A deliberate format change, or a rollback to a build that predates one.
		// Its records are unreadable here, but nothing is damaged, so it is
		// counted apart from corruption and stays silent.
		return nil, 0, s.dropSegment(num, size, true)
	}

	// Segments are preallocated to their full length, so a file of any other size
	// is either a store built with a different SegmentSize — reopening with the
	// wrong one would discard records, so reject it — or a file that lost bytes.
	// The header tells them apart: a write cursor past the end of the file means
	// the bytes it published are gone.
	truncated := false
	if size != headerSize+s.segmentSize {
		if th.writeCursor() <= size {
			return nil, 0, fmt.Errorf("%w: store created with segment size %d, opened with %d",
				ErrSegmentSizeMismatch, size-headerSize, s.segmentSize)
		}
		// Keep the records that are still there and account for the cut tail; the
		// per-record checksum drops whatever the truncation ran into.
		truncated = true
		s.discardedBytes += uint64(th.writeCursor() - size)
		s.corruptions++
		s.pendingCorrupt++
	}

	w := th.writeCursor()
	if w < headerSize {
		w = headerSize
	}
	if w > headerSize+s.segmentSize {
		w = headerSize + s.segmentSize
	}
	if truncated && w > size {
		w = size // never address bytes past the real end of the file
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
	return df, cc, nil
}

// dropSegment unlinks a segment that cannot be read and books the loss: as
// foreign (a format this build does not know — no damage, so no data-loss
// signal) or as corruption, which owes the consumer one ErrCorrupt so the lost
// records are reported rather than silently missing from the stream.
//
// The payload figure excludes the header: it is what the segment could have been
// holding, which is the number an operator sizing the damage wants.
func (s *store) dropSegment(num uint64, size int64, foreign bool) error {
	if err := s.removeFile(num); err != nil {
		// A store that cannot clear an unreadable segment would fail this way on
		// every open, so say so rather than pretending it is gone.
		return err
	}
	payload := uint64(max64(size-headerSize, 0))
	if foreign {
		s.foreignSegments++
		s.foreignBytes += payload
		return nil
	}
	s.lostSegments++
	s.lostBytes += payload
	s.corruptions++
	s.pendingCorrupt++
	return nil
}

// removeFile unlinks a segment and makes the removal durable, so a torn tail
// dropped on open cannot come back after a power loss.
func (s *store) removeFile(num uint64) error {
	if err := os.Remove(s.filePath(num)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !s.noSync {
		return s.syncDir()
	}
	return nil
}

// readHeader preads a file's fixed-size header without keeping the handle.
//
// Only a short read is reported as ErrCorrupt — the file really is missing header
// bytes. Everything else (EACCES, EMFILE, EIO, …) is returned as itself: it says
// nothing about the contents, and calling it corruption would hand a recovering
// open a licence to delete a perfectly good segment.
func (s *store) readHeader(num uint64) ([]byte, error) {
	f, err := os.Open(s.filePath(num))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // read-only handle: nothing to lose on close
	h := make([]byte, headerSize)
	if _, err := io.ReadFull(f, h); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: reading header of %s: %w", ErrCorrupt, s.filePath(num), err)
		}
		return nil, err
	}
	return h, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// createFile creates and preallocates segment num. Every failure path unlinks the
// partial file again, so a failed create leaves nothing behind for the next open
// to trip over, and returns cleanly: nothing was published, so the store is
// exactly where it was.
func (s *store) createFile(num uint64, base int64) (*dataFile, error) {
	f, err := os.OpenFile(s.filePath(num), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*dataFile, error) {
		_ = f.Close()
		_ = os.Remove(s.filePath(num))
		return nil, err
	}
	// Reserve the blocks now rather than discovering a full filesystem in the
	// middle of an append: here the segment is still empty and unreferenced.
	if err := preallocate(f, headerSize+s.segmentSize); err != nil {
		return fail(err)
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
		return fail(err)
	}
	if !s.noSync {
		if err := f.Sync(); err != nil {
			// Not latched: the file is about to be unlinked, so nothing durable
			// depends on this fsync having happened.
			return fail(err)
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
// durable) and marks the file dirty, reopening the file if it was evicted.
func (s *store) writeHeader(df *dataFile) error {
	if df.f == nil {
		if err := s.ensureMapped(df); err != nil {
			return err
		}
	}
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

// flushFile fsyncs df if it has unsynced writes, then marks it clean. No-op for
// an already-clean file; a dirty file whose handle was evicted is reopened,
// because dirty means "has unsynced bytes", not "is open".
//
// A failed fsync latches ioErr: those bytes may be gone for good and a second
// fsync would happily report success. A failure to reopen does not latch — the
// data is still dirty and the next flush retries it.
func (s *store) flushFile(df *dataFile) error {
	if !df.dirty {
		return nil
	}
	if df.f == nil {
		if err := s.ensureMapped(df); err != nil {
			return err
		}
	}
	if err := df.f.Sync(); err != nil {
		return s.failIO(err)
	}
	df.dirty = false
	return nil
}

// append writes payload as a new record at the tail, cycling to a new file when
// the active one is full.
//
// The record's bytes go down first and the in-memory cursors advance only once
// nothing can still make the record invisible, so a failure before that point
// leaves the store exactly as it was: the caller's error means "not appended".
func (s *store) append(payload []byte) error {
	if s.ioErr != nil {
		return s.ioErr
	}
	L := len(payload)
	recLen := int64(uvarintLen(uint64(L)) + L + checksumSize)
	if recLen > s.segmentSize {
		return ErrRecordTooLarge
	}

	af := s.active()
	if af == nil || af.size+recLen > s.segmentSize {
		if err := s.cycle(); err != nil {
			if errors.Is(err, ErrFull) {
				s.nFull++
			}
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
		return err // nothing advanced; the bytes are unreferenced and overwritable
	}
	perOp := !s.noSync && !s.batched()
	if perOp {
		// Per-op: fsync the record bytes before the header publishes them. Syncing
		// the data first guarantees a crash can only ever lose the header update (a
		// clean truncation), never leave a published record whose payload never
		// landed.
		if err := af.f.Sync(); err != nil {
			return s.failIO(err)
		}
	}

	af.size += recLen
	af.written++
	s.writeOff += recLen
	s.nWritten++
	s.nAdded++
	// Update the header (write cursor + count) in memory and publish it.
	af.header(
		setWriteCursor(headerSize+af.size),
		setWrittenCount(af.written),
	)
	if err := s.writeHeader(af); err != nil {
		// The header never reached even the page cache, so the record is invisible
		// to a reopen; roll the in-memory view back to match.
		af.size -= recLen
		af.written--
		s.writeOff -= recLen
		s.nWritten--
		s.nAdded--
		af.header(
			setWriteCursor(headerSize+af.size),
			setWrittenCount(af.written),
		)
		return err
	}
	switch {
	case s.noSync:
		// No fsync; the record and header sit in the page cache and an explicit
		// Sync/Close flushes them.
		return nil
	case s.batched():
		return s.recordOp()
	default:
		if err := af.f.Sync(); err != nil {
			// The header is in the page cache, so the record is real to anything
			// short of a power loss: it stays, and the store is poisoned.
			return s.failIO(err)
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
//
// A file that will not unlink stays in the live set — its records are committed,
// so it is never re-delivered, but leaving it counted keeps maxSegments a truthful
// statement about what is on disk, and the next drop retries the removal.
func (s *store) dropCommitted(keep *dataFile) {
	survive := s.files[:0]
	for _, df := range s.files {
		if df != keep && df.base+df.size <= s.commitOff {
			if df.f != nil {
				_ = df.f.Close() // read-only from here on; nothing left to lose
				df.f = nil
				s.removeMapped(df)
			}
			if err := os.Remove(s.filePath(df.num)); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.unreclaimed++
				survive = append(survive, df)
				continue
			}
			// written == committed here, so this keeps Count exact.
			s.nWritten -= df.written
			s.nCommitted -= df.committed
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

// shortReadIsCorrupt reclassifies a truncated read as corruption. Records live
// inside a preallocated segment, so hitting the end of the file means the bytes
// the header published are no longer there — which is exactly what ErrCorrupt
// says, and what the recovery path knows how to quarantine. A real device error is
// left alone so it is never mistaken for recoverable corruption.
func shortReadIsCorrupt(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return err
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
		return nil, 0, 0, false, shortReadIsCorrupt(err)
	}
	v, n := binary.Uvarint(s.readBuf[:hn])
	if n <= 0 {
		return nil, 0, 0, false, nil
	}
	if !fitsInRecord(v, n, avail) {
		return nil, 0, 0, false, nil
	}
	L := int(v)
	total := n + L + checksumSize
	s.readBuf = growBuf(s.readBuf, total)
	if _, err := df.f.ReadAt(s.readBuf[:total], headerSize+dataOff); err != nil {
		return nil, 0, 0, false, shortReadIsCorrupt(err)
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
		return 0, false, shortReadIsCorrupt(err)
	}
	v, n := binary.Uvarint(s.readBuf[:hn])
	if n <= 0 {
		return 0, false, nil
	}
	if !fitsInRecord(v, n, avail) {
		return 0, false, nil
	}
	return off + int64(n) + int64(v) + checksumSize, true, nil
}

// fitsInRecord reports whether a decoded length prefix v (n bytes of varint)
// describes a record that fits in the avail bytes left in the segment.
//
// The comparison is done in uint64 and *before* v is narrowed to int, which is
// load-bearing: a corrupt length near 2^63 narrows to a large positive int, and
// computing n+L+checksumSize in int then wraps negative — sailing past a signed
// bounds check and panicking in growBuf's reslice. Since avail never exceeds
// segmentSize, the first comparison makes the second one overflow-free.
func fitsInRecord(v uint64, n int, avail int64) bool {
	return v <= uint64(avail) && uint64(n)+v+checksumSize <= uint64(avail)
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
		if errors.Is(err, os.ErrNotExist) {
			// The segment the header says holds this record is gone: those bytes are
			// not coming back, which is what ErrCorrupt means and what the recovery
			// path knows how to abandon. EMFILE/EACCES stay as themselves — they are
			// transient, and a retry is the right answer there, not a lossy skip.
			return nil, 0, 0, false, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
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
// advances.
//
// Damage never stops the queue and never comes out as data. Each event drops
// what is unreadable, moves the cursor past it and returns one ErrCorrupt — so
// the caller sees every loss and the *next* call makes progress. How much is
// dropped depends on how much can still be trusted:
//
//   - the record's length framed it inside the segment but its checksum fails:
//     trust the framing exactly that far and drop one record;
//   - the length itself is unusable (undecodable, overrunning the segment, or the
//     file is gone): the frame boundaries from here on are lost with it, so the
//     rest of the segment goes.
//
// A genuine I/O error is not damage — nothing is dropped and the cursor stays
// put, because the bytes may well still be there on the next attempt.
func (s *store) takeHead() ([]byte, int64, bool, error) {
	// Losses with no read of their own to report them — segments dropped at open
	// — are paid out one per call, so each reaches the consumer exactly once.
	if s.pendingCorrupt > 0 {
		s.pendingCorrupt--
		return nil, 0, false, fmt.Errorf("%w: unreadable segment dropped", ErrCorrupt)
	}
	payload, sum, next, ok, err := s.read(s.headOff)
	if err != nil {
		if !errors.Is(err, ErrCorrupt) {
			return nil, 0, false, err // transient: retry, don't destroy
		}
		if serr := s.skipCorruptSegment(s.headOff); serr != nil {
			return nil, 0, false, errors.Join(err, serr)
		}
		return nil, 0, false, err
	}
	if !ok {
		return nil, 0, false, nil // empty
	}
	if xxhash.Sum64(payload) != sum {
		s.lostBytes += uint64(next - s.headOff)
		s.lostRecords++
		s.corruptions++
		s.headOff = next
		return nil, 0, false, fmt.Errorf("%w: record checksum", ErrCorrupt)
	}
	s.headOff = next
	s.nDelivered++
	return payload, next, true, nil
}

// rewindHead puts the read cursor back to off, un-reading the record takeHead
// just returned. Used when the value could not be handed over after all, so a
// failure does not quietly swallow a record that was never delivered. off must be
// a cursor takeHead was called at, so it can never precede the commit cursor.
func (s *store) rewindHead(off int64) {
	if off < s.headOff {
		s.headOff = off
	}
}

// skipCorruptSegment abandons the rest of the segment holding off: it advances
// the read cursor past the segment, and — when the commit cursor is already
// within that segment (the auto-committing read path) — force-commits the
// abandoned tail so it is reclaimed and never replayed.
//
// This is the expensive half of the blast-radius rule, reached only when the
// framing itself is gone: with the record boundaries unknown, there is nothing
// left to resynchronize on, and waiting for them to become readable would wedge
// the queue forever. Everything from the read cursor to the end of the segment is
// counted as lost.
//
// The in-memory force-commit is applied only once the header carrying it is
// durable. Otherwise a failure here would leave the store believing a segment was
// quarantined while the next open replays and re-quarantines it forever.
func (s *store) skipCorruptSegment(off int64) error {
	df := s.fileForOffset(off)
	if df == nil {
		// The cursor addresses no live segment. Unreachable now that commitTo drags
		// the read cursor along, but if it ever happens, abandon to the tail *and*
		// square the counters, so Count() and Empty() agree instead of leaving a
		// blocking consumer waiting on a backlog that is not there.
		s.corruptions++
		s.lostSegments++
		if lost := s.writeOff - s.headOff; lost > 0 {
			s.lostBytes += uint64(lost)
		}
		s.headOff = s.writeOff
		if s.commitOff < s.writeOff {
			s.commitOff = s.writeOff
		}
		s.nCommitted = s.nWritten
		return nil
	}
	end := df.base + df.size
	if s.commitOff >= df.base {
		err := s.ensureMapped(df)
		switch {
		case err == nil:
			prevCursor, prevCount := df.commitCursor(), df.committedCount()
			df.header(
				setCommitCursor(headerSize+df.size),
				setCommittedCount(df.written),
			)
			if err := s.writeHeader(df); err != nil {
				df.header(setCommitCursor(prevCursor), setCommittedCount(prevCount))
				return err
			}
			if !s.noSync {
				if err := s.flushFile(df); err != nil { // recovery wants this durable now
					return err
				}
			}
		case errors.Is(err, os.ErrNotExist):
			// The segment is gone from the directory: there is no header left to
			// record the quarantine in, and nothing left to replay from it either,
			// so the in-memory advance below is all there is to do. dropCommitted
			// then retires the entry.
		default:
			return err
		}
		if abandoned := df.written - df.committed; abandoned > 0 {
			s.nCommitted += abandoned
			df.committed = df.written
		}
		if s.commitOff < end {
			s.commitOff = end
		}
	}
	// Everything between the read cursor and the end of the segment is destroyed.
	// For a segment whose file has vanished, its recorded size is the only figure
	// left to count, which is why LostBytes is documented as a lower bound.
	if lost := end - s.headOff; lost > 0 {
		s.lostBytes += uint64(lost)
	}
	s.lostSegments++
	if s.headOff < end {
		s.headOff = end
	}
	s.corruptions++
	return nil
}

// commitTo advances the commit cursor to off, counting the records crossed and
// persisting the cursor and count into each file's header.
//
// A failure stops the walk and is returned, but whatever was committed before it
// stands: the cursor only ever moves forward over records that were really there,
// and a commit that never reached disk simply replays after a reopen
// (at-least-once), which is the contract anyway.
func (s *store) commitTo(off int64) error {
	if s.ioErr != nil {
		return s.ioErr
	}
	if off <= s.commitOff {
		return nil
	}
	if off > s.writeOff {
		off = s.writeOff
	}
	// Per-op policy flushes each file's header once, not once per record: commits
	// cross files in order, so flush a file when the commit leaves it, and the
	// last at the end. A crash before the flush replays the batch (at-least-once).
	perOp := !s.noSync && !s.batched()
	var cur *dataFile // file with header changes not yet written out
	var stop error
	advanced := false
	for s.commitOff < off {
		df := s.fileForOffset(s.commitOff)
		if df == nil {
			stop = ErrCorrupt // the cursor points at no file: nothing can advance it
			break
		}
		if cur != nil && cur != df {
			// Leaving a file: write its final header now, while its handle is
			// certainly still open — opening the next segment can evict this one.
			if err := s.persistCommit(cur, perOp); err != nil {
				stop = err
				break
			}
			cur = nil
		}
		if err := s.ensureMapped(df); err != nil {
			stop = err // can't open the file to advance the cursor; replay later
			break
		}
		next, ok, err := s.recordLen(df, s.commitOff)
		// The cursor may only ever move forward, and never past the tail: a decoded
		// length that says otherwise is corruption, not a destination.
		if ok && (next <= s.commitOff || next > s.writeOff) {
			ok = false
		}
		if err != nil || !ok {
			if err == nil {
				err = ErrCorrupt // no decodable record where the cursor stands
			}
			// Abandon the segment rather than freezing the commit cursor on it
			// forever — the same policy, and the same accounting, the read path
			// applies. A cursor stuck on an unframable record would also stop all
			// reclamation, so the disk fills behind it.
			if errors.Is(err, ErrCorrupt) {
				if serr := s.skipCorruptSegment(s.commitOff); serr != nil {
					stop = serr
					break
				}
				cur, advanced = nil, true // it persisted the header itself
				continue
			}
			stop = err
			break
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
		advanced = true
	}
	if cur != nil {
		if err := s.persistCommit(cur, perOp); err != nil && stop == nil {
			stop = err
		}
	}
	if !advanced {
		return stop // nothing committed
	}
	if s.batched() {
		if err := s.recordOp(); err != nil && stop == nil {
			stop = err
		}
	}
	// Commit may have overtaken the read cursor (Commit takes any offset up to the
	// write cursor). Drag the read cursor along, or dropCommitted below would
	// reclaim the very file headOff points into and every later read would land in
	// no file at all.
	if s.headOff < s.commitOff {
		s.headOff = s.commitOff
	}
	// Reclaim any files this commit fully drained, so a consume-only or producer-
	// stopped workload frees disk without waiting for the next append. Keep the
	// active file (it holds the write position); the directory entry removal is
	// not fsync'd here — a lingering file after a crash is re-dropped, never
	// re-delivered (its records stay committed), so reclamation is best-effort.
	s.dropCommitted(s.active())
	return stop
}

// persistCommit writes df's header out and, on the per-op policy, makes it
// durable.
func (s *store) persistCommit(df *dataFile, perOp bool) error {
	if err := s.writeHeader(df); err != nil {
		return err
	}
	if perOp {
		return s.flushFile(df)
	}
	return nil
}

func (s *store) empty() bool            { return s.headOff >= s.writeOff && s.pendingCorrupt == 0 }
func (s *store) size() int64            { return s.writeOff - s.commitOff }
func (s *store) count() int64           { return s.nWritten - s.nCommitted }
func (s *store) writeOffset() int64     { return s.writeOff }
func (s *store) headOffset() int64      { return s.headOff }
func (s *store) corruptionCount() int64 { return s.corruptions }
func (s *store) failure() error         { return s.ioErr }

// stats snapshots the counters. The caller holds the DiskQueue lock.
func (s *store) stats() Stats {
	return Stats{
		BacklogBytes: s.size(),
		Backlog:      s.count(),
		Segments:     len(s.files),
		MaxSegments:  s.maxSegments,
		// Segments are preallocated to their full length, so what they occupy is
		// the count times the geometry — not the bytes of records in them.
		DiskBytes:       int64(len(s.files)) * (headerSize + s.segmentSize),
		Added:           s.nAdded,
		Delivered:       s.nDelivered,
		Committed:       uint64(s.nCommitted),
		Full:            s.nFull,
		LostBytes:       s.lostBytes,
		LostRecords:     s.lostRecords,
		LostSegments:    s.lostSegments,
		ForeignSegments: s.foreignSegments,
		ForeignBytes:    s.foreignBytes,
		DiscardedBytes:  s.discardedBytes,
		Corruptions:     uint64(s.corruptions),
	}
}

// sync makes every unsynced segment durable. Once a flush has failed the store is
// poisoned and sync keeps saying so: re-running fsync would report success over
// pages the kernel already dropped.
func (s *store) sync() error {
	if s.ioErr != nil {
		return s.ioErr
	}
	var errs error
	for _, df := range s.files {
		errs = errors.Join(errs, s.flushFile(df)) // keep going; flush what can be flushed
	}
	if errs != nil {
		return errs
	}
	s.unsynced = 0 // a full flush makes any batched-but-unsynced ops durable
	return nil
}

// syncDir fsyncs the directory so segment creations/removals are durable: an
// fsync of a file flushes its data and inode but never its directory entry, which
// a power loss would otherwise drop — stranding already-fsync'd records. The
// handle is the one held open for the session (it also carries the lock).
//
// A filesystem with no directory-fsync primitive is not a failure: this runs on
// the open path, and refusing to open a queue over such a mount would be a worse
// answer than ordering creates the way that filesystem sees fit.
func (s *store) syncDir() error {
	if s.dirFile == nil {
		return nil
	}
	err := fsyncDir(s.dirFile)
	if err == nil || dirSyncUnsupported(err) {
		return nil
	}
	return s.failIO(err)
}

// close flushes and releases everything, including the directory handle and with
// it the advisory lock. It always closes every handle, even when a flush fails,
// and reports the first error (a latched durability failure included).
func (s *store) close() error {
	first := s.ioErr
	for _, df := range s.files {
		// Close flushes even under noSync: noSync means "no fsync per operation",
		// not "never", and the documented contract is that Close always flushes.
		if s.ioErr == nil {
			if err := s.flushFile(df); err != nil && first == nil {
				first = err
			}
		}
		df.dirty = false
		if df.f == nil {
			continue // not currently open
		}
		if err := df.f.Close(); err != nil && first == nil {
			first = err
		}
		df.f = nil
	}
	s.files = nil
	s.mappedMRU, s.mappedLRU, s.mappedLen = nil, nil, 0
	if s.dirFile != nil {
		if err := s.dirFile.Close(); err != nil && first == nil {
			first = err
		}
		s.dirFile = nil
	}
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
