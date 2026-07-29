package diskqueue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
)

// On-disk format: numbered data files (data.00000001, …), each a 64-byte header
// (magic, cursors/counts, version, header checksum — see the dataFile accessors)
// followed by records (uvarint(len) || payload || xxhash64(payload) as 8 little-
// endian bytes).
//
// Everything recovery needs lives in the header, so it scans records only for a
// segment whose own header proves it was truncated (see surviveCount). Records
// never span files. A global byte offset addresses the stream: file F holds
// offsets [F.base, F.base+F.size). Files are dropped once fully committed — on
// the write that cycles to a new segment, and at the end of a commit. Each record
// and each header carries an xxhash64, verified on read/open to catch corruption.
//
// I/O is plain pread/pwrite/fsync (no mmap): records are written with WriteAt and
// read back with ReadAt into reused buffers, and durability is fsync. Each file's
// 64-byte header is kept resident in memory (dataFile.hdr) and written to its page
// 0 with WriteAt; recovery reads it back with a bare pread.

// store is the raw, []byte-oriented file backend. Not safe for concurrent use;
// the Queue serializes access with its own mutex.
type store struct {
	dir          string
	dirFile      *os.File // held open for the whole session: directory fsync + advisory lock
	segmentSize  int64    // capacity of each file's data region (excludes header)
	maxSegments  int      // max number of data files retained at once; 0 == unbounded
	noSync       bool
	syncEvery    int // fsync every N writes/commits; <=1 means every one
	maxOpenFiles int // cap on simultaneously open segment files; 0 == unbounded

	// ioErr latches the first fsync failure (see failIO). Once set, every
	// operation that would otherwise claim durability returns it.
	ioErr error

	files   []*dataFile // sorted by num ascending; last is the active write file
	nextNum uint64

	// Intrusive LRU list of currently open files, so touch/evict/remove are O(1)
	// pointer splices rather than O(n) slice shifts. lruMRU is the
	// most-recently-used end (where touches and new opens go); lruLRU is the
	// eviction end. nOpen tracks the length against maxOpenFiles.
	lruMRU *dataFile
	lruLRU *dataFile
	nOpen  int

	// lastFrameAt/lastFrameEnd cache the boundary of the record the most recent
	// read crossed. Every consume path is read-then-commit under one lock, so the
	// commit walk starts exactly where that read started and would otherwise pread
	// the length prefix of a record it just read in full. Only a boundary a read
	// established is ever cached, so trusting it costs nothing in integrity: it is
	// the same number recordLen would return, without the syscall.
	lastFrameAt  int64
	lastFrameEnd int64

	// Reused I/O buffers: writeBuf frames a record before a single WriteAt; readBuf
	// holds a block of one segment's data region. Reusing them keeps append/read
	// alloc-free once warm; see record.go for what the block cache guarantees.
	writeBuf  []byte
	readBuf   []byte
	blockFile *dataFile // segment readBuf currently holds bytes from, or nil
	blockOff  int64     // data-region offset of readBuf[0]
	blockLen  int       // valid bytes in readBuf

	writeOff  int64 // global offset of the next record to write (tail)
	headOff   int64 // global offset of the next record to read (in memory only)
	commitOff int64 // global offset of the next record to commit (persisted)

	nWritten   int64 // total records appended
	nCommitted int64 // total records committed

	unsynced    int    // writes/commits accumulated since the last batched flush
	unreclaimed uint64 // failed attempts to unlink a fully-committed segment

	// Loss accounting. Corruption is never allowed to wedge the queue, so the
	// only way a consumer learns what a bad byte cost is through these: each
	// event surfaces as one ErrCorrupt, and these carry the magnitude.
	//
	// pendingCorrupt is the backlog of losses that happened with no read to
	// report them — segments dropped at open, mostly. takeHead pays it down one
	// per call so each lost segment reaches the consumer exactly once.
	pendingCorrupt  int
	corruptions     uint64
	lostBytes       uint64
	lostRecords     uint64
	lostSegments    uint64
	foreignSegments uint64
	foreignBytes    uint64
	discardedBytes  uint64

	nAdded     uint64 // records accepted by append
	nDelivered uint64 // records handed out by takeHead
	nFull      uint64 // appends refused with ErrFull

	// nCommittedTotal is the lifetime count of records retired by a commit.
	// It is deliberately NOT nCommitted, which is a gauge: nCommitted is paired
	// with nWritten to compute Count() and is decremented when a fully-committed
	// segment is reclaimed, so it falls as the queue does its job and resets on
	// reopen. Reporting that as a counter made rate() go negative.
	nCommittedTotal uint64
}

func openStore(dir string, segmentSize int64, maxSegments int, noSync bool, syncEvery, maxOpenFiles int) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if maxOpenFiles != 0 && maxOpenFiles < 3 {
		// Three cursors can each sit in a different segment: the write cursor, the
		// read cursor, and the commit cursor that Reserve/Commit leaves behind it.
		// A cap of 2 evicts one of them on every operation, so the file the next op
		// needs is always the one just closed — reopening a handle per record.
		//
		// A negative value means the floor, not "unbounded": a caller computing a
		// descriptor budget must never get an uncapped queue out of a bad number.
		maxOpenFiles = 3
	}
	s := &store{
		dir:          dir,
		segmentSize:  segmentSize,
		maxSegments:  maxSegments,
		noSync:       noSync,
		syncEvery:    syncEvery,
		maxOpenFiles: maxOpenFiles,
	}
	// Hold the directory open for the session: it is both the handle the segment
	// creations/removals are fsync'd through and the thing the advisory lock hangs
	// on, so no other Queue can write into the same directory.
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

// ensureOpen opens df's file if needed and marks it most-recently-used; the
// active file stays open because every append touches it.
func (s *store) ensureOpen(df *dataFile) error {
	if df.f != nil {
		s.touchOpen(df)
		return nil
	}
	f, err := os.OpenFile(df.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	df.f = f
	s.trackOpen(df)
	return nil
}

// trackOpen records df as open (most-recently-used) and evicts down to the cap.
func (s *store) trackOpen(df *dataFile) {
	s.lruPushMRU(df)
	s.evictOpen(df)
}

// touchOpen moves an already-open df to the most-recently-used end.
func (s *store) touchOpen(df *dataFile) {
	if df == s.lruMRU {
		return
	}
	s.lruUnlink(df)
	s.lruPushMRU(df)
}

// untrackOpen detaches df from the LRU list (its file is being closed/removed).
func (s *store) untrackOpen(df *dataFile) {
	s.lruUnlink(df)
}

// lruPushMRU links df in at the most-recently-used end. df must not already
// be in the list.
func (s *store) lruPushMRU(df *dataFile) {
	df.lruPrev = nil
	df.lruNext = s.lruMRU
	if s.lruMRU != nil {
		s.lruMRU.lruPrev = df
	} else {
		s.lruLRU = df
	}
	s.lruMRU = df
	s.nOpen++
}

// lruUnlink removes df from the LRU list and clears its links.
func (s *store) lruUnlink(df *dataFile) {
	if df.lruPrev != nil {
		df.lruPrev.lruNext = df.lruNext
	} else {
		s.lruMRU = df.lruNext
	}
	if df.lruNext != nil {
		df.lruNext.lruPrev = df.lruPrev
	} else {
		s.lruLRU = df.lruPrev
	}
	df.lruPrev, df.lruNext = nil, nil
	s.nOpen--
}

// evictOpen closes least-recently-used files until at most maxOpenFiles remain
// open, never closing the active file or keep (the one just opened). A dirty
// victim is fsync'd before its handle is closed (a failure there latches ioErr,
// so it is not lost); under noSync nothing is fsync'd and the victim stays
// marked dirty, so a later explicit Sync reopens it and flushes it rather than
// mistaking a closed handle for a clean file.
func (s *store) evictOpen(keep *dataFile) {
	if s.maxOpenFiles <= 0 {
		return
	}
	active := s.active()
	for s.nOpen > s.maxOpenFiles {
		// Walk from the least-recently-used end toward the most-recently-used,
		// skipping the active and just-opened files (which are never evicted).
		var victim *dataFile
		for df := s.lruLRU; df != nil; df = df.lruPrev {
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
		s.lruUnlink(victim)
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

// createFile creates and preallocates segment num. Every failure path unlinks the
// partial file again, so a failed create leaves nothing behind for the next open
// to trip over, and returns cleanly: nothing was published, so the store is
// exactly where it was.
func (s *store) createFile(num uint64, base int64) (*dataFile, error) {
	path := s.filePath(num)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*dataFile, error) {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	// Reserve the blocks now rather than discovering a full filesystem in the
	// middle of an append: here the segment is still empty and unreferenced.
	if err := preallocate(f, headerSize+s.segmentSize); err != nil {
		return fail(err)
	}
	df := &dataFile{num: num, path: path, f: f, hdr: make([]byte, headerSize), base: base}
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
		// A full fsync, not datasync: this is the one segment write that changes
		// metadata — the file was just created and preallocated — so the inode has
		// to reach disk too. Do not "finish the job" by converting this one.
		//
		// Not latched: the file is about to be unlinked, so nothing durable depends
		// on this fsync having happened.
		if err := f.Sync(); err != nil {
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
		if err := s.ensureOpen(df); err != nil {
			return err
		}
	}
	if _, err := df.f.WriteAt(df.hdr, 0); err != nil {
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
		if err := s.ensureOpen(df); err != nil {
			return err
		}
	}
	if err := datasync(df.f); err != nil {
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
	if err := s.ensureOpen(af); err != nil {
		return err
	}

	if err := faultPoint("append.writeRecord"); err != nil {
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
		if err := faultPoint("append.syncData"); err != nil {
			return s.failIO(err)
		}
		if err := datasync(af.f); err != nil {
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
	if err := faultPoint("append.writeHeader"); err != nil {
		s.rollbackAppend(af, recLen)
		return err
	}
	if err := s.writeHeader(af); err != nil {
		// The header never reached even the page cache, so the record is invisible
		// to a reopen; roll the in-memory view back to match.
		s.rollbackAppend(af, recLen)
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
		if err := faultPoint("append.syncHeader"); err != nil {
			return s.failIO(err)
		}
		if err := datasync(af.f); err != nil {
			// The header is in the page cache, so the record is real to anything
			// short of a power loss: it stays, and the store is poisoned.
			return s.failIO(err)
		}
		af.dirty = false
		return nil
	}
}

// rollbackAppend undoes the in-memory advance for a record whose header never
// reached even the page cache, so the record is invisible to a reopen and the
// store's view matches it. Nothing on disk is touched: the record bytes sit in a
// preallocated region past the write cursor, where the next append overwrites
// them and no reader can address them.
func (s *store) rollbackAppend(af *dataFile, recLen int64) {
	af.size -= recLen
	af.written--
	s.writeOff -= recLen
	s.nWritten--
	s.nAdded--
	af.header(
		setWriteCursor(headerSize+af.size),
		setWrittenCount(af.written),
	)
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
	s.trackOpen(df)
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
// when fully drained. Both run under the Queue lock, so no store op races it. A
// just-delivered record's file may be closed and unlinked here, in the very call
// that delivered it — the consumer's value is unaffected, because the payload it
// holds is a copy in the Reader's own buffer and was never this file's memory.
//
// A file that will not unlink stays in the live set — its records are committed,
// so it is never re-delivered, but leaving it counted keeps maxSegments a truthful
// statement about what is on disk, and the next drop retries the removal.
func (s *store) dropCommitted(keep *dataFile) {
	// Files ascend by base and never overlap, so anything fully committed is a
	// prefix of the slice: if the first file is not reclaimable, none are. This
	// runs on every commit, so the common no-op case should not walk the backlog.
	if len(s.files) == 0 || s.files[0] == keep || s.files[0].base+s.files[0].size > s.commitOff {
		return
	}
	// A reclaim invalidates both caches: the frame boundary and the block may name
	// a segment that is about to stop existing.
	s.lastFrameAt, s.lastFrameEnd = 0, 0
	s.dropBlock()
	survive := s.files[:0]
	for _, df := range s.files {
		if df != keep && df.base+df.size <= s.commitOff {
			if df.f != nil {
				_ = df.f.Close() // read-only from here on; nothing left to lose
				df.f = nil
				s.untrackOpen(df)
			}
			if err := os.Remove(df.path); err != nil && !errors.Is(err, os.ErrNotExist) {
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
	if err := s.ensureOpen(df); err != nil {
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
		head := s.headOff
		if serr := s.skipCorruptSegment(head); serr != nil {
			if s.headOff == head {
				// The quarantine could not be recorded and nothing moved. Reporting
				// ErrCorrupt here would tell the caller the queue had stepped past
				// the damage, and the documented recovery loop ("count it and go
				// round again") would re-read the same bytes forever. It is a
				// retriable I/O failure, so hand back the failure itself.
				return nil, 0, false, serr
			}
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
	// Hand the boundary to the commit that is about to follow: every consume path
	// reads and then commits this same record while still holding the lock.
	s.lastFrameAt, s.lastFrameEnd = s.headOff, next
	s.headOff = next
	s.nDelivered++
	return payload, next, true, nil
}

// rewindHead puts the read cursor back to off, un-reading the record takeHead
// just returned. Used when the value could not be handed over after all, so a
// failure does not quietly swallow a record that was never delivered. off must be
// a cursor takeHead was called at, so it can never precede the commit cursor.
//
// The delivery count is rolled back with it: takeHead counted the record on its
// way out, and it never reached the consumer.
func (s *store) rewindHead(off int64) {
	if off < s.headOff {
		s.headOff = off
		if s.nDelivered > 0 {
			s.nDelivered--
		}
	}
}

// rewindToCommit puts the read cursor back to the commit cursor, so every record
// that was delivered but never acknowledged is offered again. It returns the
// bytes made readable.
//
// Unlike rewindHead this does NOT un-count the deliveries: those records really
// were handed to a consumer, and the redelivery will be counted again. That is
// what Stats.Delivered means by "redeliveries included".
func (s *store) rewindToCommit() int64 {
	n := s.headOff - s.commitOff
	s.headOff = s.commitOff
	return n
}

// progress is the pair a consume operation has to move for an iteration to be
// getting anywhere: the read cursor, and the backlog of corruption reports still
// owed (paid down one per takeHead call, without the cursor moving).
//
// A caller that treats "damage was dropped, go round again" as a reason to retry
// must check this first. Damage the store could not actually step past reports
// the same error from the same offset forever, and a loop that keeps going on the
// error alone spins — under the queue lock, in the iterators' case.
type progress struct {
	head    int64
	pending int
}

func (s *store) progress() progress { return progress{head: s.headOff, pending: s.pendingCorrupt} }

// drained reports whether the read cursor has reached end with no corruption
// report still owed. The backlog is part of the question: a segment dropped at
// open destroys records that no read will ever fail on, so a consumer that
// watched the cursor alone would never collect those reports.
func (s *store) drained(end int64) bool { return s.headOff >= end && s.pendingCorrupt == 0 }

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
		// Square the per-file counts too, not just the global one: dropCommitted
		// subtracts df.committed from nCommitted as it reclaims, so leaving the two
		// out of step here would drive Count() negative on the next drop.
		for _, df := range s.files {
			if abandoned := df.written - df.committed; abandoned > 0 {
				s.nCommittedTotal += uint64(abandoned)
			}
			df.committed = df.written
		}
		s.nCommitted = s.nWritten
		return nil
	}
	end := df.base + df.size
	if s.commitOff >= df.base {
		err := s.ensureOpen(df)
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
			s.nCommittedTotal += uint64(abandoned)
			df.committed = df.written
		}
		if s.commitOff < end {
			s.commitOff = end
		}
	}
	// Everything between the read cursor and the end of the segment is destroyed.
	// For a segment whose file has vanished, its recorded size is the only figure
	// left to count, which is why LostBytes is documented as a lower bound.
	// A segment counts as lost only if something in it actually was. Reached from
	// the commit path, the read cursor is often already past the end — every record
	// here was delivered — and booking a lost segment for zero lost bytes tells an
	// operator data went missing when none did.
	if lost := end - s.headOff; lost > 0 {
		s.lostBytes += uint64(lost)
		s.lostSegments++
	}
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
		if err := s.ensureOpen(df); err != nil {
			stop = err // can't open the file to advance the cursor; replay later
			break
		}
		next, ok, err := s.frameEnd(df, s.commitOff)
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
				// The commit path has no read to carry the report, so owe it
				// through the backlog: the invariant is that every counted event
				// surfaces as exactly one ErrCorrupt, and a Corruptions() that
				// climbs with nothing in any log is what breaks an operator's trust
				// in the number.
				s.pendingCorrupt++
				cur, advanced = nil, true // it persisted the header itself
				continue
			}
			stop = err
			break
		}
		if next > off {
			// A non-boundary target: the caller asked to acknowledge up to off and
			// this record ends past it. Stop short rather than retiring it — the
			// bias everywhere else is to redeliver, never to drop something nobody
			// said was done.
			break
		}
		s.commitOff = next
		s.nCommitted++
		s.nCommittedTotal++
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

func (s *store) empty() bool { return s.drained(s.writeOff) }
func (s *store) size() int64 { return s.writeOff - s.commitOff }

// count is a gauge, so it must never be negative. nWritten and nCommitted are
// exact by construction, but they are reconciled from per-segment headers at
// open: a forged or self-inconsistent header has to produce a wrong number, not
// an impossible one.
func (s *store) count() int64 {
	if c := s.nWritten - s.nCommitted; c > 0 {
		return c
	}
	return 0
}
func (s *store) writeOffset() int64      { return s.writeOff }
func (s *store) headOffset() int64       { return s.headOff }
func (s *store) corruptionCount() uint64 { return s.corruptions }
func (s *store) failure() error          { return s.ioErr }

// stats snapshots the counters. The caller holds the Queue lock.
func (s *store) stats() Stats {
	return Stats{
		BacklogBytes:  s.size(),
		Backlog:       s.count(),
		InFlightBytes: max(s.headOff-s.commitOff, 0),
		Segments:      len(s.files),
		MaxSegments:   s.maxSegments,
		// Segments are preallocated to their full length, so what they occupy is
		// the count times the geometry — not the bytes of records in them.
		DiskBytes:       int64(len(s.files)) * (headerSize + s.segmentSize),
		Added:           s.nAdded,
		Delivered:       s.nDelivered,
		Committed:       s.nCommittedTotal,
		Full:            s.nFull,
		LostBytes:       s.lostBytes,
		LostRecords:     s.lostRecords,
		LostSegments:    s.lostSegments,
		ForeignSegments: s.foreignSegments,
		ForeignBytes:    s.foreignBytes,
		DiscardedBytes:  s.discardedBytes,
		Unreclaimed:     s.unreclaimed,
		Corruptions:     s.corruptions,
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
	// Keep the slice. Every handle is closed and nil'd above, so nothing here is
	// live — but Stats stays readable after Close, and a shutdown scrape that
	// reported "0 segments, 0 bytes on disk" for a directory still holding a
	// backlog would be the one number an operator most needs to be true.
	s.lruMRU, s.lruLRU, s.nOpen = nil, nil, 0
	if s.dirFile != nil {
		if err := s.dirFile.Close(); err != nil && first == nil {
			first = err
		}
		s.dirFile = nil
	}
	return first
}
