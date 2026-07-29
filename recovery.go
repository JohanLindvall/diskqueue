package diskqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
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
	af := s.active()
	if err := s.ensureOpen(af); err != nil {
		return err
	}
	// A truncated active segment has had its size clamped to what survived, but its
	// header still publishes the old write cursor. Republish the clamped one BEFORE
	// the reservation below re-extends the file, or the next open finds a full-size
	// file whose header points past the surviving records and reads the zero fill
	// back as a phantom backlog — re-reporting the same loss on every open.
	if af.truncated {
		af.header(
			setWriteCursor(headerSize+af.size),
			setWrittenCount(af.written),
		)
		if err := s.writeHeader(af); err != nil {
			return err
		}
		if !s.noSync {
			if err := s.flushFile(af); err != nil {
				return err
			}
		}
		af.truncated = false
	}
	// A segment written by an older build — or while the filesystem had no
	// fallocate — is sparse, and the active one is the one about to be appended to.
	// Reserving it again is a no-op if it is already allocated. Deliberately
	// best-effort: making this fatal would stop a queue on a full disk from being
	// opened to drain it, which is exactly when opening it matters most.
	_ = preallocate(af.f, headerSize+s.segmentSize)
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
	df := &dataFile{num: num, hdr: h, base: base, size: w - headerSize, truncated: truncated}
	df.written = max64(th.writtenCount(), 0)
	if truncated {
		// The header counts records the file no longer holds, so Count() would
		// promise a backlog no drain can deliver and the queue would never read
		// empty again. Believe the bytes that survived instead of the header.
		//
		// An arithmetic bound (size / smallest possible frame) is not enough: it is
		// only tight when the records are minimal, and for anything larger it sits
		// above the header's count and never fires at all. Count the frames.
		if n, err := s.surviveCount(num, df.size); err == nil {
			if gone := df.written - n; gone > 0 {
				s.lostRecords += uint64(gone)
			}
			df.written = n
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

// surviveCount counts the whole record frames in the first size bytes of a
// segment's data region.
//
// This is the one place recovery reads records, and the exception is licensed by
// the segment's own header having proved it lost bytes: that header's count
// describes a file which no longer exists. The walk is bounded by the segment
// size and only ever runs for a segment already known to be damaged, so the
// "recovery reads no records" cost model still holds for every healthy open.
//
// It never fails the open. A segment it cannot read falls back to the header's
// count, which is no worse than not having tried.
func (s *store) surviveCount(num uint64, size int64) (int64, error) {
	f, err := os.Open(s.filePath(num))
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }() // read-only handle: nothing to lose on close
	var buf [binary.MaxVarintLen64]byte
	var n, off int64
	for off < size {
		avail := size - off
		hn := min(int64(len(buf)), avail)
		if _, err := f.ReadAt(buf[:hn], headerSize+off); err != nil {
			return n, nil // the file ends here; everything past it is gone
		}
		v, used := binary.Uvarint(buf[:hn])
		if used <= 0 || !fitsInRecord(v, used, avail) {
			return n, nil // no whole frame starts here
		}
		off += int64(used) + int64(v) + checksumSize
		n++
	}
	return n, nil
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
