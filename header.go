package diskqueue

import (
	"encoding/binary"
	"os"

	"github.com/cespare/xxhash/v2"
)

// The on-disk format: the segment header and the framing constants. Change
// anything here and you have changed what is written to disk — so this file is
// deliberately pure, with no I/O and no knowledge of the store.
//
// A directory holds numbered segment files (data.00000001, …), each preallocated
// to headerSize+segmentSize. Every segment opens with a 64-byte header, followed
// by records framed as uvarint(len) || payload || xxhash64(payload).

const (
	headerSize   = 64 // [magic][cursors+counts][version][reserved][header checksum]
	checksumSize = 8  // xxhash64 trailer per record
	// readAhead is how much of a segment a read fetches in its first pread.
	// Consumption is sequential, so a bigger run means proportionally fewer
	// syscalls when draining a backlog — 64 KiB costs one pread where page-sized
	// runs cost sixteen — and it still lands a whole record of ordinary size in
	// one fetch instead of a length probe plus a re-read. The buffer never grows
	// past the segment's remaining published bytes (fillBlock caps the run by
	// avail), so a small-segment store never pays the full width, and dropBlock
	// releases anything an oversized record grew beyond segmentSize.
	readAhead     = 64 << 10
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
	num uint64
	// path is this segment's file name, built once. ensureOpen and dropCommitted
	// would otherwise rebuild it with fmt.Sprintf on every eviction cycle, which
	// profiling put at 45% of the allocations under a capped open-file count.
	path string
	f    *os.File // open handle, or nil when not currently open
	hdr  []byte   // resident copy of the 64-byte header (page 0)
	base int64    // global offset of this file's first data byte
	size int64    // bytes of records written into the data region (excludes header)
	// capacity is how many record bytes this segment's data region can hold. It is
	// the store's segmentSize for an ordinary segment, and exactly one record's
	// framed length for an oversized one — which is what keeps "records never span
	// files" true for a record bigger than a segment.
	capacity  int64
	written   int64 // number of records written (mirrors the header)
	committed int64 // number of records committed (mirrors the header)

	// Intrusive LRU links, valid only while open (f != nil). The store threads
	// open files from mru (most-recently-used) toward lru via prev.
	lruPrev *dataFile // toward the most-recently-used end
	lruNext *dataFile // toward the least-recently-used end

	// dirty is set when the file has page-cache writes (record bytes and/or header)
	// not yet fsync'd, so the batched/evict/sync/close paths fsync only files that
	// need it and skip a file that was merely read since its last flush.
	dirty bool

	// truncated marks a segment whose file was found shorter than the write cursor
	// its header publishes: the bytes past the cut are gone. size is clamped to
	// what survived, so the published cursor must be rewritten before anything
	// re-extends the file — otherwise the next open reads the zero fill back as
	// records.
	truncated bool
}

// Header layout (little-endian): [0:8] magic, [8:16] commit cursor, [16:24] write
// cursor, [24:32] written count, [32:40] committed count, [40] version, [41]
// flags, [42:50] this segment's data-region capacity, [50:56] the SegmentSize the
// store was created with (6-byte little-endian, so any size below 256 TiB
// round-trips exactly — the store layer accepts arbitrary sizes, not just
// page-rounded ones), [56:64] xxhash64 of [0:56]. The checksum is rewritten on every header update so
// torn/rotten headers are caught on open — and it covers [0:56], so the flags and
// geometry fields are protected like everything else.
//
// The two geometry fields exist because file length stopped being trustworthy
// evidence once segments could be individually sized: capacity is what bounds
// every read and tells a short file it lost bytes, and the created-with
// SegmentSize is the witness for ErrSegmentSizeMismatch — without it, a store
// whose every surviving segment was oversized carried no record of the geometry
// it was built under.
func (df *dataFile) magic() uint64 { return binary.LittleEndian.Uint64(df.hdr[0:8]) }

func (df *dataFile) version() byte { return df.hdr[40] }

// flagOversized marks a segment sized to hold ONE record too large for the
// configured SegmentSize, rather than to the standard geometry.
//
// It has to be on disk rather than inferred, because without it "this file is
// bigger than headerSize+segmentSize" is ambiguous: it equally describes a store
// created with a LARGER SegmentSize and reopened with a smaller one, which must
// still be rejected with ErrSegmentSizeMismatch instead of being silently
// accepted and half-read.
const flagOversized = 1 << 0

func (df *dataFile) oversized() bool { return df.hdr[41]&flagOversized != 0 }

func setOversized() func(*dataFile) {
	return func(df *dataFile) { df.hdr[41] |= flagOversized }
}

func (df *dataFile) commitCursor() int64 { return int64(binary.LittleEndian.Uint64(df.hdr[8:16])) }

func (df *dataFile) writeCursor() int64 { return int64(binary.LittleEndian.Uint64(df.hdr[16:24])) }

func (df *dataFile) writtenCount() int64 { return int64(binary.LittleEndian.Uint64(df.hdr[24:32])) }

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

// hdrCapacity is the data-region capacity this segment was created with: the
// configured SegmentSize for an ordinary segment, one record's framed length for
// an oversized one. It bounds every offset derived from this header.
func (df *dataFile) hdrCapacity() int64 { return int64(binary.LittleEndian.Uint64(df.hdr[42:50])) }

func setHdrCapacity(v int64) func(*dataFile) {
	return func(df *dataFile) { binary.LittleEndian.PutUint64(df.hdr[42:50], uint64(v)) }
}

// hdrSegSize is the SegmentSize the creating store was configured with — the
// same for every segment of a store, oversized ones included, which is what
// makes it usable as the geometry witness on reopen. Six raw little-endian
// bytes: the store layer does not require page-rounded sizes, so the field must
// round-trip any value, and 2^48 is far past what fallocate can reserve anyway.
func (df *dataFile) hdrSegSize() int64 {
	b := df.hdr[50:56]
	return int64(uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
		uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40)
}

func setHdrSegSize(v int64) func(*dataFile) {
	return func(df *dataFile) {
		u := uint64(v)
		b := df.hdr[50:56]
		b[0], b[1], b[2], b[3], b[4], b[5] =
			byte(u), byte(u>>8), byte(u>>16), byte(u>>24), byte(u>>32), byte(u>>40)
	}
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
