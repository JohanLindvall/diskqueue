package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// On-disk format: numbered data files (data.00000001, …), each a 32-byte header
// of four little-endian uint64s followed by records (uvarint(len) || payload):
//
//	[0:8]   commit cursor   — file-local offset of the next uncommitted record
//	[8:16]  write cursor    — file-local offset of the data end
//	[16:24] written count   — records written into this file
//	[24:32] committed count — records committed in this file
//
// Everything recovery needs lives in the header, so it never scans records.
// Records never span files. A global byte offset addresses the stream: file F
// holds offsets [F.base, F.base+F.size). Files are dropped once fully committed,
// but only while writing — reads and commits never delete files.

const (
	headerSize = 32
	filePrefix = "data."
)

type dataFile struct {
	num       uint64
	f         *os.File
	data      []byte // mmap of the whole file: header followed by the data region
	base      int64  // global offset of this file's first data byte
	size      int64  // bytes of records written into the data region (excludes header)
	written   int64  // number of records written (mirrors the header)
	committed int64  // number of records committed (mirrors the header)
}

func (df *dataFile) commitCursor() int64     { return int64(binary.LittleEndian.Uint64(df.data[0:8])) }
func (df *dataFile) setCommitCursor(v int64) { binary.LittleEndian.PutUint64(df.data[0:8], uint64(v)) }
func (df *dataFile) writeCursor() int64      { return int64(binary.LittleEndian.Uint64(df.data[8:16])) }
func (df *dataFile) setWriteCursor(v int64)  { binary.LittleEndian.PutUint64(df.data[8:16], uint64(v)) }

func (df *dataFile) writtenCount() int64 { return int64(binary.LittleEndian.Uint64(df.data[16:24])) }
func (df *dataFile) setWrittenCount(v int64) {
	binary.LittleEndian.PutUint64(df.data[16:24], uint64(v))
}
func (df *dataFile) committedCount() int64 { return int64(binary.LittleEndian.Uint64(df.data[24:32])) }
func (df *dataFile) setCommittedCount(v int64) {
	binary.LittleEndian.PutUint64(df.data[24:32], uint64(v))
}

// store is the raw, []byte-oriented file backend. Not safe for concurrent use;
// the WAL serializes access with its own mutex.
type store struct {
	dir         string
	segmentSize int64 // capacity of each file's data region (excludes header)
	maxSegments int   // max number of data files retained at once; 0 == unbounded
	noSync      bool
	pageSize    int64

	files   []*dataFile // sorted by num ascending; last is the active write file
	nextNum uint64

	writeOff  int64 // global offset of the next record to write (tail)
	headOff   int64 // global offset of the next record to read (in memory only)
	commitOff int64 // global offset of the next record to commit (persisted)

	nWritten   int64 // total records appended
	nCommitted int64 // total records committed
}

func openStore(dir string, segmentSize int64, maxSegments int, noSync bool) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &store{
		dir:         dir,
		segmentSize: segmentSize,
		maxSegments: maxSegments,
		noSync:      noSync,
		pageSize:    int64(os.Getpagesize()),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
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
		s.nextNum = 2
		return nil
	}

	var base int64
	for _, num := range nums {
		df, err := s.openFile(num)
		if err != nil {
			return err
		}
		df.base = base
		// Read the data end and counts from the header; clamp defensively.
		w := df.writeCursor()
		if w < headerSize {
			w = headerSize
		}
		if w > headerSize+s.segmentSize {
			w = headerSize + s.segmentSize
		}
		df.size = w - headerSize
		df.written = max64(df.writtenCount(), 0)
		df.committed = df.committedCount()
		if df.committed < 0 {
			df.committed = 0
		}
		if df.committed > df.written {
			df.committed = df.written
		}
		base += df.size
		s.nWritten += df.written
		s.nCommitted += df.committed
		s.files = append(s.files, df)
	}
	s.nextNum = nums[len(nums)-1] + 1
	s.writeOff = base

	// Commit cursor: the first file whose commit cursor is short of its end.
	s.commitOff = s.writeOff
	for _, df := range s.files {
		h := df.commitCursor()
		if h < headerSize {
			h = headerSize
		}
		if h > headerSize+df.size {
			h = headerSize + df.size
		}
		if h < headerSize+df.size {
			s.commitOff = df.base + (h - headerSize)
			break
		}
	}
	s.headOff = s.commitOff
	return nil
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
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	df := &dataFile{num: num, f: f, data: data, base: base}
	df.setCommitCursor(headerSize)
	df.setWriteCursor(headerSize)
	return df, nil
}

func (s *store) openFile(num uint64) (*dataFile, error) {
	f, err := os.OpenFile(s.filePath(num), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Size to full capacity so the active file has room to grow.
	if err := f.Truncate(headerSize + s.segmentSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	data, err := mmapFile(f, int(headerSize+s.segmentSize))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &dataFile{num: num, f: f, data: data}, nil
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
	recLen := int64(uvarintLen(uint64(L)) + L)
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

	p := headerSize + int(af.size)
	m := binary.PutUvarint(af.data[p:], uint64(L))
	copy(af.data[p+m:], payload)
	old := af.size
	af.size += recLen
	af.written++
	s.writeOff += recLen
	s.nWritten++
	// Publish the data end and count after the record bytes, so recovery only
	// sees fully-written records.
	af.setWriteCursor(headerSize + af.size)
	af.setWrittenCount(af.written)
	if !s.noSync {
		s.msync(af, headerSize+int(old), headerSize+int(af.size)) // record bytes
		s.msync(af, 8, 24)                                        // write cursor + written count
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
	return nil
}

// dropCommitted removes (and unmaps) every fully-committed file. Only called
// from cycle, so no read is in flight and a recently-read record is never in a
// committed file — unmapping here can't invalidate a live slice. It is the only
// place files are deleted.
func (s *store) dropCommitted() {
	keep := s.files[:0]
	for _, df := range s.files {
		if df.base+df.size <= s.commitOff {
			// written == committed here, so this keeps Count exact.
			s.nWritten -= df.written
			s.nCommitted -= df.committed
			_ = os.Remove(s.filePath(df.num))
			_ = df.f.Close()
			_ = unix.Munmap(df.data)
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
// payload (a slice into the mmap), the offset past it, and whether it decoded.
func (df *dataFile) recordAt(off int64) ([]byte, int64, bool) {
	p := headerSize + int(off-df.base)
	v, n := binary.Uvarint(df.data[p:])
	if n <= 0 {
		return nil, 0, false
	}
	L := int(v)
	start := p + n
	return df.data[start : start+L], off + int64(n+L), true
}

// read locates and decodes the record at global offset off.
func (s *store) read(off int64) ([]byte, int64, bool) {
	if off >= s.writeOff {
		return nil, 0, false
	}
	df := s.fileForOffset(off)
	if df == nil {
		return nil, 0, false
	}
	return df.recordAt(off)
}

// takeHead reads the record at the head cursor and advances it.
func (s *store) takeHead() ([]byte, int64, bool) {
	payload, next, ok := s.read(s.headOff)
	if !ok {
		return nil, 0, false
	}
	s.headOff = next
	return payload, next, true
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
	for s.commitOff < off {
		df := s.fileForOffset(s.commitOff)
		if df == nil {
			break
		}
		_, next, ok := df.recordAt(s.commitOff)
		if !ok {
			break
		}
		s.commitOff = next
		s.nCommitted++
		df.committed++
		df.setCommitCursor(headerSize + (s.commitOff - df.base))
		df.setCommittedCount(df.committed)
		if !s.noSync {
			s.msync(df, 0, 8)   // commit cursor
			s.msync(df, 24, 32) // committed count
		}
	}
}

func (s *store) empty() bool        { return s.headOff >= s.writeOff }
func (s *store) size() int64        { return s.writeOff - s.commitOff }
func (s *store) count() int64       { return s.nWritten - s.nCommitted }
func (s *store) writeOffset() int64 { return s.writeOff }
func (s *store) headOffset() int64  { return s.headOff }

func (s *store) sync() error {
	for _, df := range s.files {
		if err := unix.Msync(df.data, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) close() error {
	var first error
	for _, df := range s.files {
		if !s.noSync {
			if err := unix.Msync(df.data, unix.MS_SYNC); err != nil && first == nil {
				first = err
			}
		}
		if err := unix.Munmap(df.data); err != nil && first == nil {
			first = err
		}
		if err := df.f.Close(); err != nil && first == nil {
			first = err
		}
	}
	s.files = nil
	return first
}

// msync flushes [from,to) of df, aligning the start down to a page boundary.
func (s *store) msync(df *dataFile, from, to int) {
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
