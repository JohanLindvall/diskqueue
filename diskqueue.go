// Package diskqueue implements a generic, durable, FIFO disk-backed queue on top
// of nsqio/go-diskqueue (github.com/nsqio/go-diskqueue), which provides the
// []byte-only file backend (numbered data files, a persisted read/write cursor,
// and an ioLoop goroutine that exposes records over a channel).
//
// Items are appended with Add and consumed through a Reader (DiskQueue.NewReader):
// Take reads the next record (blocking), TryTake reads without blocking, and
// Drain/Follow iterate. A read advances the persisted read cursor as the record
// is delivered, and fully-read data files are reclaimed by the backend.
//
// Value lifetime: each consumed record is delivered as a freshly allocated slice
// owned by the caller, so the value handed to UnmarshalFunc stays valid
// indefinitely.
//
// Concurrency: a DiskQueue is safe for concurrent use. Readers share one
// underlying read channel and cooperate — each record is delivered to exactly one
// reader. A single Reader holds no mutable state, so it is also safe to share, but
// one per consuming goroutine remains the clear idiom.
//
// Crash semantics: at-most-once. A record's read cursor advances as it is
// delivered and is persisted by the backend (governed by SyncEvery/SyncInterval
// and always on Close), so a delivered-but-unprocessed record is not replayed
// after a crash. This is a deliberate change from the previous mmap store, which
// offered at-least-once replay via a separate commit cursor; that two-phase
// Reserve/Commit acknowledgement is no longer available.
package diskqueue

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	godq "github.com/nsqio/go-diskqueue"
)

// MarshalFunc serializes v by appending to dst and returning the extended slice
// (like the builtin append).
type MarshalFunc[T any] func(dst []byte, v T) ([]byte, error)

// UnmarshalFunc decodes a value from data. The slice is owned by the caller and
// stays valid indefinitely.
type UnmarshalFunc[T any] func(data []byte) (T, error)

// Errors returned by the package.
var (
	// ErrClosed is returned once the DiskQueue has been closed.
	ErrClosed = errors.New("diskqueue: closed")
	// ErrFull is returned by Add when a new record would exceed MaxSegments live
	// segment files.
	ErrFull = errors.New("diskqueue: full")
	// ErrRecordTooLarge is returned by Add when a record cannot fit one segment.
	ErrRecordTooLarge = errors.New("diskqueue: record too large")
)

// queueName is the fixed prefix go-diskqueue uses for the data and metadata files
// it creates under the queue directory (e.g. "diskqueue.diskqueue.000001.dat").
const queueName = "diskqueue"

// Options tunes the behaviour of a DiskQueue. The zero value is valid and selects
// sensible defaults.
type Options struct {
	// NoSync minimizes fsync after writes. This trades durability against a power
	// loss for higher throughput; data still survives a process crash via the page
	// cache, and Close still flushes. Default false.
	NoSync bool

	// SyncEvery batches durability: fsync once every N writes/reads instead of after
	// each one. 0 or 1 syncs every operation (the default). A larger N raises
	// throughput but widens the power-loss window. Ignored when NoSync is set.
	SyncEvery int

	// SegmentSize sets each segment file's capacity. Default 8 MiB, floored at
	// 4 KiB. A record too big for one segment is rejected with ErrRecordTooLarge.
	SegmentSize int64

	// MaxSegments caps how many segment files are kept at once; once reached, Add
	// returns ErrFull until reading reclaims a segment. The footprint is about
	// MaxSegments × SegmentSize bytes. 0 selects the default of 32; a negative
	// value means unbounded. The bound is best-effort (checked against the files on
	// disk), so the live count may briefly overshoot by a segment.
	MaxSegments int

	// SyncInterval, if > 0, sets how often the backend flushes to stable storage on
	// a timer — a wall-clock backstop for SyncEvery batching. Defaults to 2s.
	SyncInterval time.Duration
}

// DiskQueue is a generic persistent FIFO queue of T.
type DiskQueue[T any] struct {
	marshal   MarshalFunc[T]
	unmarshal UnmarshalFunc[T]

	dir        string
	segCap     int64
	maxMsgSize int32
	maxSeg     int // 0 == unbounded

	mu     sync.Mutex
	dq     godq.Interface
	closed bool
	done   chan struct{} // closed by Close to unblock readers

	// scratch is reused by Add to serialize values.
	scratch []byte

	// segCount caches the live segment-file count and bytesSinceGlob tracks bytes
	// appended since it was last refreshed, so MaxSegments backpressure does not
	// stat the directory on every Add (see segmentsFull).
	segCount       int
	bytesSinceGlob int64

	// drainMu serializes Drain iterations so a bounded drain never deadlocks two
	// readers competing for the last record on the shared read channel.
	drainMu sync.Mutex

	// corruptions counts backend recovery events (a torn file renamed .bad and
	// skipped); incremented from the backend log callback.
	corruptions int64
}

// New opens (creating if necessary) a DiskQueue under the directory path.
func New[T any](path string, marshal MarshalFunc[T], unmarshal UnmarshalFunc[T], opts ...Options) (*DiskQueue[T], error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	segCap := segmentCapacity(opt.SegmentSize)
	maxMsg := segCap - 4 // go-diskqueue prefixes each record with a 4-byte length
	if maxMsg > math.MaxInt32 {
		maxMsg = math.MaxInt32
	}

	w := &DiskQueue[T]{
		marshal:    marshal,
		unmarshal:  unmarshal,
		dir:        path,
		segCap:     segCap,
		maxMsgSize: int32(maxMsg),
		maxSeg:     resolveMaxSegments(opt.MaxSegments),
		done:       make(chan struct{}),
	}

	n, err := w.countSegments()
	if err != nil {
		return nil, err
	}
	w.segCount = n

	w.dq = godq.New(queueName, path, segCap, 0, int32(maxMsg),
		syncEvery(opt), syncInterval(opt), w.logf)
	return w, nil
}

// logf is the backend log callback; it counts corruption-recovery events.
func (w *DiskQueue[T]) logf(lvl godq.LogLevel, f string, args ...interface{}) {
	if strings.Contains(f, "saving bad file") {
		atomic.AddInt64(&w.corruptions, 1)
	}
}

// syncEvery maps Options onto go-diskqueue's per-write sync count.
func syncEvery(opt Options) int64 {
	switch {
	case opt.NoSync:
		return math.MaxInt64
	case opt.SyncEvery <= 1:
		return 1
	default:
		return int64(opt.SyncEvery)
	}
}

// syncInterval maps Options onto go-diskqueue's timer-driven sync period
// (which must be positive).
func syncInterval(opt Options) time.Duration {
	switch {
	case opt.SyncInterval > 0:
		return opt.SyncInterval
	case opt.NoSync:
		return 24 * time.Hour
	default:
		return 2 * time.Second
	}
}

// defaultMaxSegments bounds the live file count when Options.MaxSegments is left
// at its zero value.
const defaultMaxSegments = 32

// resolveMaxSegments maps Options.MaxSegments to the internal convention, where 0
// means unbounded: the zero value selects defaultMaxSegments and a negative value
// requests unbounded.
func resolveMaxSegments(v int) int {
	switch {
	case v == 0:
		return defaultMaxSegments
	case v < 0:
		return 0
	default:
		return v
	}
}

func segmentCapacity(size int64) int64 {
	c := size
	if c <= 0 {
		c = 8 << 20 // 8 MiB default
	}
	if c < 4096 {
		c = 4096
	}
	return c
}

// countSegments returns the number of live numbered segment files under the
// queue directory (excluding the metadata file and any quarantined .bad files).
func (w *DiskQueue[T]) countSegments() (int, error) {
	matches, err := filepath.Glob(filepath.Join(w.dir, queueName+".diskqueue.*.dat"))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range matches {
		if strings.HasSuffix(m, ".meta.dat") {
			continue
		}
		n++
	}
	return n, nil
}

// segmentsFull reports whether the live segment count is at the MaxSegments cap,
// refreshing the cached count when the cap is in reach or a segment's worth of
// bytes has been written since the last refresh. The caller must hold w.mu.
func (w *DiskQueue[T]) segmentsFull(recLen int) bool {
	if w.maxSeg <= 0 {
		return false
	}
	w.bytesSinceGlob += int64(recLen) + 4
	if w.segCount >= w.maxSeg || w.bytesSinceGlob >= w.segCap {
		if n, err := w.countSegments(); err == nil {
			w.segCount = n
			w.bytesSinceGlob = 0
		}
	}
	return w.segCount >= w.maxSeg
}

// Add appends data to the back of the log.
func (w *DiskQueue[T]) Add(data T) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	b, err := w.marshal(w.scratch[:0], data)
	if err != nil {
		return err
	}
	w.scratch = b // retain grown capacity for reuse
	if int64(len(b)) > int64(w.maxMsgSize) {
		return ErrRecordTooLarge
	}
	if w.segmentsFull(len(b)) {
		return ErrFull
	}
	return w.dq.Put(b)
}

// Empty reports whether there are no items available to read.
func (w *DiskQueue[T]) Empty() bool {
	return w.depth() == 0
}

// Count returns the number of items added but not yet read.
func (w *DiskQueue[T]) Count() int {
	return int(w.depth())
}

// depth returns the backend depth, or 0 once closed.
func (w *DiskQueue[T]) depth() int64 {
	w.mu.Lock()
	closed, dq := w.closed, w.dq
	w.mu.Unlock()
	if closed {
		return 0
	}
	return dq.Depth()
}

// Size returns the bytes the queue currently occupies on disk (its segment
// files). This is the physical footprint, not the count of unread bytes: it
// includes already-read records whose file has not yet been reclaimed.
func (w *DiskQueue[T]) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	matches, err := filepath.Glob(filepath.Join(w.dir, queueName+".diskqueue.*.dat"))
	if err != nil {
		return 0
	}
	var total int64
	for _, m := range matches {
		if strings.HasSuffix(m, ".meta.dat") {
			continue
		}
		if fi, err := os.Stat(m); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// Corruptions returns how many corruption-recovery events the backend has logged
// since open (a torn file renamed .bad and skipped). A non-zero value means data
// was dropped.
func (w *DiskQueue[T]) Corruptions() int64 {
	return atomic.LoadInt64(&w.corruptions)
}

// Close flushes and closes the DiskQueue. Further use returns ErrClosed.
func (w *DiskQueue[T]) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.closed = true
	close(w.done)
	dq := w.dq
	w.mu.Unlock()
	return dq.Close()
}
