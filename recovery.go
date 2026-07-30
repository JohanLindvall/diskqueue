package diskqueue

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
)

// The open path: everything that runs before the store serves its first
// operation, and nothing that runs after.
//
// Recovery reads no records. Each segment's 64-byte header carries the write
// cursor, the commit cursor and the counts, so reopening costs one pread per
// segment rather than a scan of the backlog.
//
// The other half of this file is the policy for a segment that cannot be
// believed. Corruption degrades to reported loss: a damaged segment is dropped
// wherever it sits and the open succeeds, with the loss counted and owed to the
// reader as one ErrCorrupt. What is emphatically not corruption is a segment that
// merely could not be read — EACCES, EMFILE, EIO fail the open and delete
// nothing, because "I could not look" is not evidence of damage.

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
	slices.Sort(nums)

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

	// A truncated segment has had its size clamped to what survived, but its header
	// still publishes the old write cursor. Republish the clamped one now, or the
	// next open re-detects the same short file and books the same loss again — and
	// again on the open after that, forever.
	//
	// EVERY truncated segment, not just the active one: a middle segment is never
	// re-extended and never appended to, so nothing else would ever correct it. It
	// also has to happen before the reservation below, which would otherwise leave
	// the active file full-size with a header pointing past its surviving records,
	// so the zero fill reads back as a phantom backlog.
	for _, df := range s.files {
		if !df.truncated {
			continue
		}
		if err := s.ensureOpen(df); err != nil {
			return err
		}
		df.header(
			setWriteCursor(headerSize+df.size),
			setWrittenCount(df.written),
		)
		if err := s.writeHeader(df); err != nil {
			return err
		}
		if !s.noSync {
			if err := s.flushFile(df); err != nil {
				return err
			}
		}
		// Restore the geometry too, and only after the clamped cursor is durable.
		// Segments are preallocated to a known length, so a short file is read as a
		// different SegmentSize unless its own header proves it lost bytes — and the
		// header no longer says that, because we just fixed it. Left short, the next
		// open would reject the whole store with ErrSegmentSizeMismatch. Re-extended,
		// it is an ordinary partly-filled segment whose tail is zero fill, which is
		// what every other segment looks like.
		if err := preallocate(df.f, headerSize+df.capacity); err != nil {
			return err
		}
		df.truncated = false
	}

	// Open the active file so appends can write into it; the rest open on demand.
	af := s.active()
	if err := s.ensureOpen(af); err != nil {
		return err
	}
	// A segment written by an older build — or while the filesystem had no
	// fallocate — is sparse, and the active one is the one about to be appended to.
	// Reserving it again is a no-op if it is already allocated. Deliberately
	// best-effort: making this fatal would stop a queue on a full disk from being
	// opened to drain it, which is exactly when opening it matters most.
	_ = preallocate(af.f, headerSize+af.capacity)
	return nil
}

// startFresh creates segment num as the sole (active) file, for an empty
// directory or one whose every segment turned out to be a dropped torn tail.
func (s *store) startFresh(num uint64) error {
	df, err := s.createFile(num, s.writeOff, s.segmentSize)
	if err != nil {
		return err
	}
	s.nextNum = num + 1
	s.files = append(s.files, df)
	s.trackOpen(df)
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

	// The header, not the file length, is the evidence about geometry. Every
	// segment states the SegmentSize its store was created under — the witness for
	// the mismatch check, and the reason a store whose every surviving segment is
	// oversized still refuses the wrong configuration — and its own capacity,
	// which is what a file of any length is measured against. Deriving either
	// from os.Stat, as this used to, could not tell an oversized segment from a
	// store built at a different SegmentSize, and could not see that an oversized
	// segment had been truncated at all.
	if cfg := th.hdrSegSize(); cfg != s.segmentSize {
		return nil, 0, fmt.Errorf("%w: store created with segment size %d, opened with %d",
			ErrSegmentSizeMismatch, cfg, s.segmentSize)
	}
	capacity := max(th.hdrCapacity(), 0)

	// A file shorter than its stated capacity lost bytes; whether it lost DATA is
	// the write cursor's call. Bytes past the cursor are preallocated zero fill,
	// so a cut that stayed within them costs nothing — the repair in load
	// re-extends the file and no loss event is raised. A cut that reached
	// published bytes is corruption: count the tail, owe the reader one report.
	// (A file LONGER than its capacity is ignored rather than diagnosed: nothing
	// past capacity is ever addressed, so the surplus is inert.)
	truncated := size < headerSize+capacity
	lostTail := truncated && th.writeCursor() > size
	if lostTail {
		s.discardedBytes += uint64(th.writeCursor() - size)
		s.corruptions++
		s.pendingCorrupt++
	}

	w := th.writeCursor()
	if w < headerSize {
		w = headerSize
	}
	if w > headerSize+capacity {
		w = headerSize + capacity
	}
	if w > size {
		w = size // never address bytes past the real end of the file
	}
	df := &dataFile{num: num, path: path, hdr: h, base: base, size: w - headerSize,
		capacity: capacity, truncated: truncated}
	df.written = max(th.writtenCount(), 0)
	if lostTail {
		// The header counts records the file no longer holds, so Count() would
		// promise a backlog no drain can deliver and the queue would never read
		// empty again. Believe the bytes that survived instead of the header.
		//
		// An arithmetic bound (size / smallest possible frame) is not enough: it is
		// only tight when the records are minimal, and for anything larger it sits
		// above the header's count and never fires at all. Count the frames.
		if n, end, err := s.surviveCount(df); err == nil {
			if gone := df.written - n; gone > 0 {
				s.lostRecords += uint64(gone)
			}
			df.written = n
			// Cut the published extent back to the last WHOLE frame, not to where
			// the file happens to end. The bytes between the two are a partial
			// frame, and leaving them inside the extent is what once destroyed
			// records accepted after a clean recovery: the repair republished the
			// write cursor beyond them, the next append landed there, and the read
			// walk then tripped over the stale partial frame and mis-framed
			// everything behind it — fresh, fsynced, acknowledged records included.
			// Clamped, the repair republishes the cursor at the clean boundary and
			// the next append overwrites the garbage.
			if cut := df.base + df.size - end; cut > 0 {
				s.discardedBytes += uint64(cut)
				df.size = end - df.base
				// The walk itself just read this segment through the block cache,
				// which may now hold bytes past the clamped extent. The cache's
				// soundness rests on published records being immutable — and the
				// repair breaks exactly that premise here, republishing the write
				// cursor at the clean boundary so a later append legitimately
				// overwrites the partial bytes. Served stale, that cached prefix
				// mis-frames the fresh record and destroys it; drop the block.
				s.dropBlock()
			}
		}
	}
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

// surviveCount counts the whole record frames a truncated segment still holds.
//
// This is the one place recovery reads records, and the exception is licensed by
// the segment's own header having proved it lost bytes: that header's count
// describes a file which no longer exists. The walk is bounded by the clamped
// size and only ever runs for a segment already known to be damaged, so the
// "recovery reads no records" cost model still holds for every healthy open.
//
// It steps with recordLen rather than its own decoder, so the frame layout stays
// spelled in exactly one place — a change there that this missed would silently
// produce a wrong count rather than an error.
// It returns the count and the offset just past the last whole frame — the
// clean boundary. The caller clamps the published extent to it, because bytes
// past it are a partial frame that would poison every later append.
func (s *store) surviveCount(df *dataFile) (int64, int64, error) {
	if err := s.ensureOpen(df); err != nil {
		return 0, 0, err
	}
	var n int64
	off := df.base
	for off < df.base+df.size {
		next, ok, err := s.recordLen(df, off)
		if err != nil || !ok {
			break // no whole frame starts here; the rest is gone
		}
		off = next
		n++
	}
	return n, off, nil
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
	payload := uint64(max(size-headerSize, 0))
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
		if isShortRead(err) {
			return nil, fmt.Errorf("%w: reading header of %s: %w", ErrCorrupt, s.filePath(num), err)
		}
		return nil, err
	}
	return h, nil
}
