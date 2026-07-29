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

	// Read a block up front rather than probing for the length and then re-reading
	// the same bytes: a whole record under readAhead arrives in one pread, which is
	// the common case by a wide margin, and the length prefix is decoded out of
	// bytes already in hand. Only a record too big for the block costs the second
	// syscall — and then the first read was not wasted either, since the kernel has
	// the page.
	hn := int64(readAhead)
	if hn > avail {
		hn = avail
	}
	s.readBuf = growBuf(s.readBuf, int(hn))
	if _, err := df.f.ReadAt(s.readBuf[:hn], headerSize+dataOff); err != nil {
		// Short of the block is fine as long as the record itself is whole: the
		// segment's data region simply ends there. Re-read exactly the prefix and
		// let the framing checks below decide.
		if !isShortRead(err) {
			return nil, 0, 0, false, err
		}
		hn = min(int64(binary.MaxVarintLen64), avail)
		if _, err := df.f.ReadAt(s.readBuf[:hn], headerSize+dataOff); err != nil {
			return nil, 0, 0, false, shortReadIsCorrupt(err)
		}
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
	if int64(total) > hn { // the record ran past the block: fetch the whole frame
		s.readBuf = growBuf(s.readBuf, total)
		if _, err := df.f.ReadAt(s.readBuf[:total], headerSize+dataOff); err != nil {
			return nil, 0, 0, false, shortReadIsCorrupt(err)
		}
	}
	sum := binary.LittleEndian.Uint64(s.readBuf[n+L : total])
	return s.readBuf[n : n+L], sum, off + int64(total), true, nil
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
	// maxInt matters on 32-bit builds: avail can be up to segmentSize, so a
	// segment above 2 GiB would let a length through that int cannot hold, and the
	// narrowing in the caller would wrap negative again. Bounding by both keeps
	// the guard true on every word size.
	const maxInt = uint64(^uint(0) >> 1)
	return v <= uint64(avail) && v <= maxInt &&
		uint64(n)+v+checksumSize <= uint64(avail)
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}
