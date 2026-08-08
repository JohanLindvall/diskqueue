package diskqueue

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
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
		// Everything below addresses a segment by NUMBER, and filePath turns a
		// number back into the padded name this store writes. An entry that is not
		// in that form ("data.7", "data.007", "data.2024") therefore names a file
		// recovery would look for under a different name — and if the padded twin
		// does not exist, every later open fails with an ENOENT naming a file that
		// never existed. Load such a number only when the padded file is really
		// there (the duplicate case below); otherwise the stray is inert, exactly
		// like a "data.notanumber".
		if e.Name() != filepath.Base(s.filePath(num)) {
			if _, serr := os.Lstat(s.filePath(num)); serr != nil {
				continue
			}
		}
		nums = append(nums, num)
	}
	slices.Sort(nums)
	// Distinct names can parse to one number ("data.1" beside "data.00000001"),
	// and filePath resolves both to the padded form: loading the number twice
	// would load the same segment twice, double-counting its records and its
	// bytes. First occurrence wins; the unpadded stray is inert and stays.
	nums = slices.Compact(nums)

	if len(nums) == 0 {
		return s.startFresh(1)
	}

	// Recover from each file's header alone (no record scan, no open handle): read
	// the 64-byte header with pread, validate it, and cache the cursors/counts.
	s.nextNum = nums[len(nums)-1] + 1
	var base int64
	commitCurs := make([]int64, 0, len(nums))
	for i, num := range nums {
		df, cc, err := s.loadFile(num, base, i == len(nums)-1)
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
		s.diskBytes += headerSize + df.capacity
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

	// Write back everything the two passes above concluded, for every segment whose
	// header on disk does not already say it. Recovery is not finished until its
	// conclusions are durable: a correction that lives only in memory is believed
	// again by the next open, and the whole point of the passes was that the header
	// could not be believed.
	//
	// Two shapes need it. A truncated segment has had its size clamped to what
	// survived while its header still publishes the old write cursor — republish the
	// clamped one, or the next open re-detects the same short file and books the same
	// loss again, forever. And a segment whose committed count the reconciliation
	// above overrode still carries the stale figure; once the segments ahead of it are
	// drained and reclaimed it becomes the leading one, and its untouched header is
	// then believed outright — silently retiring records this session reported as
	// backlog.
	//
	// EVERY such segment, not just the active one: a middle segment is never
	// re-extended and never appended to, so nothing else would ever correct it. It
	// also has to happen before the reservation below, which would otherwise leave
	// the active file full-size with a header pointing past its surviving records,
	// so the zero fill reads back as a phantom backlog.
	//
	// On a healthy open the guard never fires — every field already agrees — so
	// reopening still costs one 64-byte pread per segment and no writes.
	for _, df := range s.files {
		// The commit cursor this segment must publish, reconciled against the
		// recovered GLOBAL cursor exactly as the count pass above reconciles the
		// counts: everything below it is committed, everything above it is not, and
		// the segment straddling it keeps the position the cursor came from.
		cc := int64(headerSize)
		switch {
		case df.base+df.size <= s.commitOff:
			cc = headerSize + df.size
		case df.base < s.commitOff:
			cc = headerSize + (s.commitOff - df.base)
		}
		if !df.truncated &&
			df.writeCursor() == headerSize+df.size && df.writtenCount() == df.written &&
			df.commitCursor() == cc && df.committedCount() == df.committed {
			continue
		}
		// Fatal only for a TRUNCATED segment, whose republish must land or the same
		// loss is re-booked on every open. For the reconciliation-only population
		// this is best-effort, like the re-extension below: a segment that can be
		// read but not written (a read-only file, a full disk) must not take its
		// healthy siblings down with it, and leaving that header uncorrected is
		// exactly the state every release before this one shipped.
		fatal := df.truncated
		if err := s.ensureOpen(df); err != nil {
			if !fatal {
				continue
			}
			return err
		}
		df.header(
			setWriteCursor(headerSize+df.size),
			setWrittenCount(df.written),
			setCommitCursor(cc),
			setCommittedCount(df.committed),
		)
		if err := s.writeHeader(df); err != nil {
			if !fatal {
				continue
			}
			return err
		}
		if !s.noSync {
			if err := s.flushFile(df); err != nil {
				if !fatal {
					continue
				}
				return err
			}
		}
		// Restore the geometry too, and only after the clamped cursor is durable.
		// Re-extended, the segment is an ordinary partly-filled file whose tail is
		// zero fill, which is what every other segment looks like.
		//
		// Best-effort, like the active-file reservation below, and for the same
		// reason: the repaired cursor above is already durable, so a later open
		// re-detects the short file with its header agreeing (writeCursor at the
		// real end — no loss to re-book) and simply retries this extension. Failing
		// the open here would lock a full disk out of the very queue that must be
		// drained to free it — truncation plus ENOSPC is precisely the double
		// failure a disk-full incident produces.
		if df.truncated {
			_ = preallocate(df.f, headerSize+df.capacity)
			df.truncated = false
		}
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
	s.diskBytes += headerSize + df.capacity
	s.trackOpen(df)
	if !s.noSync {
		return s.syncDir()
	}
	return nil
}

// loadFile recovers one segment from its 64-byte header, returning the file and
// its stored commit cursor. A nil *dataFile with a nil error means the segment
// was dropped, and the counters say why. tail says whether num is the
// highest-numbered segment in the directory, which is the only position an
// unfinished create can occupy (see abortedCreate).
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
func (s *store) loadFile(num uint64, base int64, tail bool) (*dataFile, int64, error) {
	path := s.filePath(num)
	fi, err := os.Stat(path)
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()

	// A zero-length segment is a create interrupted between linking the file and
	// writing its header. It cannot hold a record, so removing it loses nothing
	// and must not raise a data-loss event.
	//
	// But only at the TAIL, for the same reason abortedCreate insists on it:
	// segments are created one at a time at the end of the sequence and the number
	// advances only once the create has succeeded, so a zero-length file with a
	// live sibling ABOVE it was never an unfinished create — it is a segment that
	// lost every byte it had. Unlinking that silently is the one thing recovery must
	// never do, so it takes the ordinary corruption path and is counted and owed.
	if size == 0 {
		if tail {
			return nil, 0, s.removeFile(num)
		}
		return nil, 0, s.dropSegment(num, size, false)
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
		// The same aborted create one reservation later: the blocks are reserved and
		// nothing has been written into them, so the segment is a full-length file of
		// zeros rather than a zero-length one. Counted as damage it reports a whole
		// segment's worth of loss — 8 MiB at the default geometry — for a file that
		// never held a byte, which is what an operator sees after a power cut.
		if tail && s.abortedCreate(num, size, h) {
			return nil, 0, s.removeFile(num)
		}
		return nil, 0, s.dropSegment(num, size, false)
	case !th.headerChecksumOK():
		// Magic and version intact with a failing checksum is the signature of a
		// torn in-place header rewrite: neither field ever changes between one
		// header image and the next, so both survive any interleaving of old and
		// new bytes. Every commit rewrites the header in place, which made this the
		// one way a routine, successful operation plus a power cut could destroy a
		// whole segment of already-durable records. Only the header is gone — the
		// records beneath it carry their own framing and checksums — so rebuild it
		// from a verified walk instead of dropping the segment.
		if knownVersion(th.version()) {
			df, cc, ok, serr := s.salvageTornHeader(num, path, base, size)
			if serr != nil {
				// The walk could not be completed. Falling through would DROP the
				// segment — dropSegment unlinks it — on the strength of a read that
				// failed, which is the one thing "failing to look licenses nothing"
				// forbids. Fail the open instead; the records are still on disk.
				return nil, 0, serr
			}
			if ok {
				return df, cc, nil
			}
		}
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
		n, end, werr := s.surviveCount(df)
		if werr != nil {
			// The walk could not be completed, so nothing is known about how many
			// frames survived or where the last whole one ends. Skipping the
			// correction is NOT the safe default: df.size would keep the raw
			// truncation point, leaving a partial frame inside the published extent
			// for the repair below to republish and the next append to land behind.
			// Fail the open — the segment is untouched and a retry once the error
			// clears recovers everything.
			return nil, 0, werr
		}
		{
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
		if err != nil {
			// A read that FAILED says nothing about the frames: only corruption
			// licenses the caller to clamp the segment, and clamping on a transient
			// EIO would destroy records that are still on disk. Report it so the
			// caller fails the OPEN — "I could not look" is not evidence of damage,
			// and the caller must not treat a missing answer as a clean one.
			if !errors.Is(err, ErrCorrupt) {
				return 0, 0, err
			}
			break
		}
		if !ok {
			break // no whole frame starts here; the rest is gone
		}
		off = next
		n++
	}
	return n, off, nil
}

// salvageTornHeader rebuilds a segment whose header checksum fails while its
// magic and version bytes are intact — the residue of a header rewrite torn by
// a power cut, since those two fields are identical in the old and new images
// and survive any interleaving of the two. The header is the only casualty:
// the records beneath it are individually framed and checksummed, so a bounded
// walk that verifies every checksum recovers exactly the records that were
// really written, and the header is reconstructed from what the walk proved.
//
// The walk stops at the first frame that does not both decode and verify.
// Everything past that point is zero fill, a partial append the lost header
// never published, or damage; none of it is tellable apart without the header,
// so the extent is clamped to the last verified frame and nothing behind the
// clamp is ever addressed. For a segment that also lost payload bytes mid-run
// this under-recovers — records past the damage are cut with it — but that
// takes two independent failures in one segment, and the bias is the right
// way: nothing unverified is ever delivered.
//
// What cannot be reconstructed is the commit position: the torn header was the
// only witness. It resets to the segment's start, so everything from this
// segment forward replays — at-least-once already permits that, and a replay
// is the opposite of the loss this path used to book. The geometry witness is
// gone with it, so the capacity is rebuilt from the file's own length (never
// smaller, so the repair in load can only ever extend the file), and a store
// reopened under a different SegmentSize slips this one segment past the
// mismatch check — delivering its records under the wrong geometry beats
// unlinking them.
//
// One corruption event is booked and owed to the reader: a torn header did
// happen, and Corruptions climbing is how an operator learns of it. No bytes
// are booked with it — the bytes past the clamp are usually preallocated zero
// fill, and counting a near-whole segment of phantom loss for every torn
// header is exactly what abortedCreate exists to avoid. LostBytes is
// documented as a lower bound, and the walk cannot tell fill from loss.
//
// A walk that cannot run at all answers ok=false and the torn-header verdict
// stands: failing to look licenses nothing.
// A non-nil error means the walk could not be completed and NOTHING was proven
// about the bytes. It is returned rather than folded into ok=false because the
// caller's answer to ok=false is to unlink the segment.
func (s *store) salvageTornHeader(num uint64, path string, base, size int64) (*dataFile, int64, bool, error) {
	df := &dataFile{
		num:  num,
		path: path,
		hdr:  make([]byte, headerSize),
		base: base,
		size: size - headerSize, // bound the walk by what is physically there
	}
	n, end, err := s.countVerified(df)
	if err != nil {
		if df.f != nil {
			_ = df.f.Close() // nothing written through it; nothing to lose
			df.f = nil
			s.untrackOpen(df)
		}
		return nil, 0, false, err
	}
	df.size = end - base
	df.written = n
	// The walk read through the block cache, which may now hold bytes past the
	// clamped extent — bytes a later append will legitimately overwrite. Served
	// stale they would mis-frame the fresh record; drop the block.
	s.dropBlock()

	// Capacity from the file's own length, floored at the configured geometry so
	// the extension in load never truncates the file: an ordinary full-length
	// segment lands exactly on segmentSize, an oversized one on its own framed
	// length (and is re-flagged so reopen keeps exempting it), and a file that
	// lost tail bytes is re-extended to the standard geometry.
	capacity := max(size-headerSize, s.segmentSize)
	df.capacity = capacity
	mods := []func(*dataFile){
		(*dataFile).initHeader,
		setCommitCursor(headerSize), // the old position is unknowable; replay
		setWriteCursor(headerSize + df.size),
		setWrittenCount(df.written),
		setCommittedCount(0),
		setHdrCapacity(capacity),
		setHdrSegSize(s.segmentSize),
	}
	if capacity > s.segmentSize {
		mods = append(mods, setOversized())
	}
	df.header(mods...)
	// Mark it for the repair pass in load, which republishes the rebuilt header
	// durably and re-extends the file — the same aftercare a truncated segment
	// gets, for the same reason: the next open must find a header it can believe.
	df.truncated = true

	s.corruptions++
	s.pendingCorrupt++
	return df, headerSize, true, nil
}

// countVerified counts the whole, checksum-verified record frames at the start
// of df's data region, returning the count and the offset just past the last
// one. It is surviveCount with the trust turned all the way down: with no
// believable header, a frame that merely decodes is not evidence — in the
// region past the true (lost) write cursor, decoding is exactly what zero fill
// does — so only a payload matching its stored checksum extends the walk.
func (s *store) countVerified(df *dataFile) (int64, int64, error) {
	if err := s.ensureOpen(df); err != nil {
		return 0, 0, err
	}
	var n int64
	off := df.base
	for off < df.base+df.size {
		payload, sum, next, ok, err := s.recordAt(df, off)
		if err != nil {
			// As in surviveCount: a failed read is not a verdict on the bytes — and
			// this walk's caller responds to a bare "no" by UNLINKING the segment,
			// so the error must travel out distinctly. salvageTornHeader returns it
			// rather than answering ok=false, and the open fails with the records
			// still on disk, to be read again once whatever failed has cleared.
			if !errors.Is(err, ErrCorrupt) {
				return 0, 0, err
			}
			break
		}
		if !ok || xxhash.Sum64(payload) != sum {
			break // no verifiable frame starts here; the rest is unaddressable
		}
		off = next
		n++
	}
	return n, off, nil
}

// abortedCreate reports whether segment num is a create interrupted between the
// block reservation and the first header write: a preallocated file of nothing
// but zeros. Reclaiming one is lossless, so it takes the silent path the
// zero-length case already does instead of being booked as a lost segment.
//
// Three things keep that silence from swallowing real damage, and all three are
// needed, because the accounting may over-report a loss but must never
// under-report one:
//
//   - Nothing acknowledged can hide in an all-zero region. A record's bytes and
//     the header that publishes them are both fsync'd before the append returns,
//     so a segment whose header never landed holds nothing an Add ever confirmed
//     — and under a deferred sync policy, what it holds is the exposure
//     Stats().UnsyncedBytes reports, which is not corruption. Nothing is
//     recoverable from it either: it frames as empty records, and xxhash64 of an
//     empty payload is not zero, so every checksum word there disagrees with the
//     payload it covers.
//   - Only the tail can be an unfinished create: segments are created one at a
//     time at the end of the sequence, and the number advances only once the
//     create has succeeded. A zeroed segment with a higher-numbered sibling is
//     therefore something else, and stays corruption.
//   - A scan that cannot finish answers false, leaving the segment with the
//     verdict its unbelievable header already earned. Failing to look licenses
//     nothing here — the unlink was licensed already, and what is at stake is
//     only whether it is counted as loss.
//
// Writing the header before reserving the blocks would not settle the same
// ambiguity: the reservation is metadata and can reach the disk while the
// header's bytes are still in the page cache, leaving exactly this file again.
func (s *store) abortedCreate(num uint64, size int64, h []byte) bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	f, err := os.Open(s.filePath(num))
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }() // read-only handle: nothing to lose on close
	// The walk stops at the first nonzero byte, so a segment that holds records
	// costs a block or two; only a genuine reservation is read end to end, and only
	// on an open that found a header of zeros.
	buf := make([]byte, 64<<10)
	for off := int64(headerSize); off < size; {
		n, rerr := f.ReadAt(buf[:min(int64(len(buf)), size-off)], off)
		for _, b := range buf[:n] {
			if b != 0 {
				return false
			}
		}
		if rerr != nil {
			// The file ended before the length os.Stat reported: everything that is
			// there has been read, and all of it was zero.
			return isShortRead(rerr)
		}
		off += int64(n)
	}
	return true
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
