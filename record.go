package diskqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cespare/xxhash/v2"
)

// The record frame, in both directions: writeRecord lays one down, recordAt and
// recordLen pick one up, and the guards in between decide whether the bytes on
// disk can be trusted to frame anything at all.
//
// Two rules hold this file together. All length arithmetic happens in
// uint64/int64 (fitsInRecord) because a corrupt prefix decodes to an arbitrary
// uint64, and narrowing it first wraps negative past a signed bounds check. And a
// short read is corruption, not an I/O error: records live inside a preallocated
// segment, so hitting EOF means the bytes the header published are gone.

// growBuf returns b resized to length n, allocating a new backing array only when
// the current capacity is too small (so a warm buffer never allocates).
func growBuf(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

// trimOver returns nil once b's capacity has grown past limit — the release
// half of the package-wide buffer policy. Every reused buffer (the store's
// frame and block buffers, the Queue's marshal scratch, each Reader's copy)
// applies it at its own site with limit = segmentSize: only an oversized
// record can grow past that, keeping the buffer would pin the largest record
// ever seen for the owner's lifetime, and the next oversized use pays one
// allocation. For ordinary records the comparison is the whole cost, so the
// zero-alloc steady state stands.
func trimOver(b []byte, limit int64) []byte {
	if int64(cap(b)) > limit {
		return nil
	}
	return b
}

// growKeeping is growBuf that preserves the first keep bytes across a
// reallocation, so a buffer can be extended without re-reading what it holds.
func growKeeping(b []byte, n, keep int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	grown := make([]byte, n)
	copy(grown, b[:keep])
	return grown
}

// writeRecord frames payload (uvarint length, payload, checksum) into the reused
// writeBuf and writes it at data offset off with a single WriteAt.
func (s *store) writeRecord(df *dataFile, off int64, payload []byte) error {
	L := len(payload)
	total := int(framedLen(L)) // the one definition of what a record costs
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

// shortReadIsCorrupt reclassifies a truncated read as corruption. Records live
// inside a preallocated segment, so hitting the end of the file means the bytes
// the header published are no longer there — which is exactly what ErrCorrupt
// says, and what the recovery path knows how to quarantine. A real device error is
// left alone so it is never mistaken for recoverable corruption.
func shortReadIsCorrupt(err error) error {
	if isShortRead(err) {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return err
}

// isShortRead reports whether err means "the file ended before the read did",
// as opposed to a device or permission failure.
func isShortRead(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// The read block. readBuf holds a run of one segment's data region — not just
// the record a read asked for — and blockFile/blockOff/blockLen say which run.
//
// Records are immutable once published: append only ever writes past the write
// cursor, and the header that publishes those bytes is written afterwards. So a
// block, which never extends past the published size it was read at, stays valid
// for as long as its segment does. That is what makes serving later records out
// of it sound, and it is why the only invalidation needed is "this segment's
// published extent could have shrunk, or its file could be gone" — dropCommitted
// and the quarantine.

// blockHas reports whether the cached block holds n bytes at df's dataOff.
func (s *store) blockHas(df *dataFile, dataOff int64, n int) bool {
	return s.blockFile == df && dataOff >= s.blockOff &&
		dataOff+int64(n) <= s.blockOff+int64(s.blockLen)
}

// blockAt returns the cached bytes at dataOff. The caller must have checked
// blockHas first.
func (s *store) blockAt(dataOff int64, n int) []byte {
	i := int(dataOff - s.blockOff)
	return s.readBuf[i : i+n]
}

// dropBlock forgets the cached block. Called wherever a segment's published extent
// can shrink or its file can go away: dropCommitted, which unlinks a reclaimed
// segment, and the two recovery clamps that cut a segment back to its last whole
// frame. The quarantine does NOT need it — skipCorruptSegment moves cursors past a
// segment without changing a byte in it, so a block read from it stays exactly as
// valid as it was.
//
// It is also where a buffer grown by an oversized record is released: readBuf
// sized for the largest record ever read would otherwise stay resident for the
// store's lifetime. Safe here because every consumer copies out of readBuf under
// the same lock hold that read it, so by the time a reclamation or quarantine
// drops the block, nothing aliases the bytes.
func (s *store) dropBlock() {
	s.blockFile, s.blockOff, s.blockLen = nil, 0, 0
	s.readBuf = trimOver(s.readBuf, s.segmentSize)
}

// fillBlock makes the cached block cover at least need bytes of df starting at
// dataOff, reading a whole readAhead run when it can.
//
// The preferred run may overshoot the end of a segment whose file is shorter
// than its header claims. That is not necessarily fatal: a record wholly inside
// the surviving bytes still reads, and only what the cut ran into is corruption.
// So a short read retries at exactly what this record needs, and only a failure
// there is reported.
func (s *store) fillBlock(df *dataFile, dataOff, avail int64, need int) error {
	want := min(max(int64(readAhead), int64(need)), avail)
	if err := s.readBlock(df, dataOff, want); err != nil {
		if !isShortRead(err) {
			s.dropBlock()
			return err
		}
		// Retry at exactly what this record needs — but only if that is actually a
		// smaller read. For a frame at least readAhead wide the two are the same
		// pread, and re-issuing one that has already reported EOF just costs a
		// syscall to learn the same thing.
		if narrowed := min(int64(need), avail); narrowed < want {
			if err := s.readBlock(df, dataOff, narrowed); err != nil {
				s.dropBlock()
				return shortReadIsCorrupt(err)
			}
			return nil
		}
		s.dropBlock()
		return shortReadIsCorrupt(err)
	}
	return nil
}

// readBlock fills readBuf with n bytes of df's data region at dataOff.
//
// A block that already starts at dataOff is EXTENDED in place: only the tail is
// read, and the bytes already held are kept across the grow. That is the
// oversized-record path — a frame larger than readAhead — where re-reading from
// the start would fetch a whole block the kernel just handed us.
func (s *store) readBlock(df *dataFile, dataOff, n int64) error {
	have := 0
	if s.blockFile == df && s.blockOff == dataOff {
		if s.blockLen >= int(n) {
			return nil // already covered
		}
		have = s.blockLen
	}
	if have > 0 {
		s.readBuf = growKeeping(s.readBuf, int(n), have)
	} else {
		s.dropBlock() // readBuf is about to be overwritten, and may be reallocated
		s.readBuf = growBuf(s.readBuf, int(n))
	}
	if _, err := df.f.ReadAt(s.readBuf[have:n], headerSize+dataOff+int64(have)); err != nil {
		return err // the caller decides; the block still describes what is valid
	}
	s.blockFile, s.blockOff, s.blockLen = df, dataOff, int(n)
	return nil
}

// frameHeader returns the decoded length prefix of the record at dataOff: the
// prefix width, the payload length, and the total frame size. ok is false when
// the bytes there cannot be trusted to frame a record inside the segment.
func (s *store) frameHeader(df *dataFile, dataOff, avail int64) (n, total int, ok bool, err error) {
	probe := int(min(int64(binary.MaxVarintLen64), avail))
	if !s.blockHas(df, dataOff, probe) {
		// fillBlock either covers `probe` bytes at dataOff or reports an error —
		// its narrowest successful read is exactly min(need, avail), and probe is
		// already <= avail — so there is nothing left to clamp against blockLen.
		if err := s.fillBlock(df, dataOff, avail, probe); err != nil {
			return 0, 0, false, err
		}
	}
	v, n := binary.Uvarint(s.blockAt(dataOff, probe))
	if n <= 0 || !fitsInRecord(v, n, avail) {
		return 0, 0, false, nil
	}
	return n, n + int(v) + checksumSize, true, nil
}

// recordAt reads the record at global offset off (which must lie in df),
// returning its payload (a slice of readBuf, valid until the next read), the
// stored payload checksum, the offset past the record, and whether it decoded.
//
// The common case costs no syscall at all: records are consumed in order, and a
// readAhead block holds many of them, so only the read that crosses out of the
// block goes to the kernel.
func (s *store) recordAt(df *dataFile, off int64) ([]byte, uint64, int64, bool, error) {
	dataOff := off - df.base
	if dataOff < 0 || dataOff >= df.size {
		return nil, 0, 0, false, nil
	}
	avail := df.size - dataOff

	n, total, ok, err := s.frameHeader(df, dataOff, avail)
	if err != nil || !ok {
		return nil, 0, 0, false, err
	}
	if !s.blockHas(df, dataOff, total) {
		// The frame runs past the block. Re-read from its start, asking for the
		// whole thing: a record larger than readAhead becomes its own block.
		if err := s.fillBlock(df, dataOff, avail, total); err != nil {
			return nil, 0, 0, false, err
		}
		if !s.blockHas(df, dataOff, total) {
			return nil, 0, 0, false, shortReadIsCorrupt(io.ErrUnexpectedEOF)
		}
	}
	frame := s.blockAt(dataOff, total)
	L := total - n - checksumSize
	sum := binary.LittleEndian.Uint64(frame[n+L:])
	return frame[n : n+L], sum, off + int64(total), true, nil
}

// frameEnd returns the offset just past the record at off, using the boundary
// the last read established when it is the same one. Consume ops read and then
// commit the same record under one lock, so this is the common case, and it
// turns the commit half of a Take into pure bookkeeping — no syscall at all.
//
// The cache is only ever written by a read that successfully framed a record, so
// a hit returns exactly what recordLen would have re-derived from the bytes.
func (s *store) frameEnd(df *dataFile, off int64) (int64, bool, error) {
	if s.lastFrameEnd != 0 && s.lastFrameAt == off {
		return s.lastFrameEnd, true, nil
	}
	return s.recordLen(df, off)
}

// recordLen returns the offset past the record at off without fetching its
// payload. Used by commitTo, which needs the boundary but not the bytes — and
// which walks record by record, so it reads through the same block cache rather
// than issuing a pread per record while holding the queue lock.
func (s *store) recordLen(df *dataFile, off int64) (int64, bool, error) {
	dataOff := off - df.base
	if dataOff < 0 || dataOff >= df.size {
		return 0, false, nil
	}
	avail := df.size - dataOff
	_, total, ok, err := s.frameHeader(df, dataOff, avail)
	if err != nil || !ok {
		return 0, false, err
	}
	return off + int64(total), true, nil
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
	// maxInt matters on 32-bit builds: avail can be up to segmentSize, so a
	// segment above 2 GiB would let a length through that int cannot hold, and the
	// narrowing in the caller would wrap negative again. Bounding by both keeps
	// the guard true on every word size.
	const maxInt = uint64(^uint(0) >> 1)
	// Bound the TOTAL frame, not just v. On a 32-bit build avail itself can exceed
	// maxInt, so a v that satisfies `v <= maxInt` can still make n+int(v)+checksumSize
	// wrap negative in the caller — which sails past the signed bounds check and
	// panics reslicing readBuf. The uint64 sum cannot overflow: the first comparison
	// already bounds v by avail, and avail is bounded by real file bytes.
	return v <= uint64(avail) &&
		uint64(n)+v+checksumSize <= min(uint64(avail), maxInt)
}

// framedLen is the on-disk cost of a record with a payload of L bytes: the uvarint
// length prefix, the payload, and the checksum trailer. Every admission check,
// cycle decision and staged-span tally has to agree with what writeRecord actually
// lays down, so the arithmetic is spelled once.
func framedLen(L int) int64 { return int64(uvarintLen(uint64(L)) + L + checksumSize) }

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
