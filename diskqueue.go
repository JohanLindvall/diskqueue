// Package diskqueue implements a generic, durable, FIFO disk-backed queue (a
// persistent work queue that doubles as a write-ahead log) backed by its own file
// store (see store.go) using plain pread/pwrite/fsync; its only dependency is
// cespare/xxhash/v2 (per-record checksums).
//
// Items are appended with Add and consumed through a Reader (DiskQueue.NewReader): Take
// reads + commits in one step, or Reserve reads and later Commits its offset.
// Committing advances a persisted read cursor; data files are reclaimed once
// fully committed.
//
// It is built for high throughput and minimal allocation: Add serializes into a
// reused buffer, and a Reader copies each record into its own reused buffer — so
// both are alloc-free once warm.
//
// Value lifetime: the slice passed to UnmarshalFunc (and anything in T aliasing
// it) is owned by the Reader and valid only until that Reader's next read; copy
// it if you need it longer.
//
// Concurrency: a DiskQueue is safe for concurrent use; a single Reader is not — use one
// per consuming goroutine. Readers share one read/commit cursor and cooperate
// (each item delivered once). Take/TryTake and Drain/Follow commit under the lock
// as they read, so they are safe for concurrent cooperating readers. Reserve/
// Commit is the only deferred path: its commits must be issued in offset order
// (single consumer) or one reader reclaims another's in-flight record. The
// blocking methods honour their context.
//
// Crash semantics: at-least-once. On open the read cursor resets to the persisted
// commit cursor, so uncommitted items replay.
package diskqueue

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"
)

// MarshalFunc serializes v by appending to dst and returning the extended slice
// (like the builtin append). Appending rather than allocating keeps Add alloc-free.
type MarshalFunc[T any] func(dst []byte, v T) ([]byte, error)

// UnmarshalFunc decodes a value from data, a Reader-owned buffer valid only until
// that Reader's next read; copy out of it if you need it longer.
type UnmarshalFunc[T any] func(data []byte) (T, error)

// Errors returned by the package.
var (
	// ErrClosed is returned once the DiskQueue has been closed.
	ErrClosed = errors.New("diskqueue: closed")
	// ErrFull is returned by Add when a new segment would exceed maxSegments.
	ErrFull = errors.New("diskqueue: full")
	// ErrInvalidOffset is returned by Commit for an offset beyond the last record.
	ErrInvalidOffset = errors.New("diskqueue: invalid offset")
	// ErrRecordTooLarge is returned by Add when a record cannot fit one segment.
	ErrRecordTooLarge = errors.New("diskqueue: record too large")
	// ErrCorrupt is returned when a stored xxhash64 does not match its data,
	// indicating on-disk corruption — either a record's payload (the read cursor
	// does not advance, so the bad record is reported on every subsequent read) or
	// a file header (open fails).
	ErrCorrupt = errors.New("diskqueue: checksum mismatch")
	// ErrBadFormat is returned by New when a file in the directory is not a DiskQueue
	// segment of a supported version (wrong magic or version).
	ErrBadFormat = errors.New("diskqueue: unrecognized file format")
	// ErrSegmentSizeMismatch is returned by New when reopening a store with a
	// different SegmentSize than it was created with (which would discard data).
	ErrSegmentSizeMismatch = errors.New("diskqueue: segment size mismatch")
)

// Options tunes the behaviour of a DiskQueue. The zero value is valid and selects
// sensible defaults.
type Options struct {
	// NoSync disables the fsync after every write and commit. This trades
	// durability against a power loss for substantially higher throughput; data
	// still survives a process crash via the page cache. Default false.
	NoSync bool

	// SyncEvery batches durability: fsync once every N writes/commits instead of
	// after each one, amortizing the fsync cost. 0 or 1 syncs every operation (the
	// default). A larger N raises throughput but widens the power-loss window — up
	// to the last N unsynced operations can be lost on power loss (they still
	// survive a process crash via the page cache, and a torn tail is caught by the
	// per-record checksum). Call Sync to flush on demand; Close always flushes.
	// Ignored when NoSync is set.
	SyncEvery int

	// SegmentSize sets each segment file's capacity. Default 8 MiB, floored at
	// 4 KiB and rounded up to a page. A record too big for one segment is
	// rejected with ErrRecordTooLarge. Fixed at creation: reopening with a
	// different (post-rounding) value is rejected with ErrSegmentSizeMismatch.
	SegmentSize int64

	// MaxSegments caps how many segment files are kept at once; once reached, Add
	// returns ErrFull until a segment is committed and reclaimed. The footprint is
	// about MaxSegments × SegmentSize bytes. 0 selects the default of 32; a
	// negative value means unbounded.
	MaxSegments int

	// MaxMapped caps how many segment files are kept open at once. Segments are
	// opened on demand and the least-recently-used handles are closed beyond the
	// cap, bounding open descriptors for deep backlogs; the active segment is
	// always open. 0 means unbounded (keep every touched segment open). Values are
	// raised to a minimum of 2 (the active segment plus one reader).
	MaxMapped int

	// SyncInterval, if > 0, runs a background goroutine that flushes to stable
	// storage on that period — a wall-clock backstop for SyncEvery batching, so an
	// idle queue's last writes become durable within the interval instead of
	// waiting for SyncEvery more operations. Ignored when NoSync is set.
	SyncInterval time.Duration

	// RecoverCorrupt enables best-effort recovery instead of failing on
	// corruption. On open, a torn trailing segment (a crash mid-cycle) is dropped
	// rather than returning ErrCorrupt/ErrBadFormat. On read, a corrupt record
	// quarantines the remainder of its segment and continues with the next valid
	// record instead of returning ErrCorrupt. Recovery is lossy — the dropped data
	// is gone — so each event is counted (see DiskQueue.Corruptions). Default false
	// (strict: corruption is surfaced as an error).
	RecoverCorrupt bool
}

// DiskQueue is a generic persistent FIFO queue of T.
type DiskQueue[T any] struct {
	marshal   MarshalFunc[T]
	unmarshal UnmarshalFunc[T]

	mu     sync.Mutex
	st     *store
	closed bool

	// scratch is reused by Add to serialize values without allocating.
	scratch []byte

	// notify is lazily created by a blocked consumer and closed by Add to wake
	// waiters; nil when nobody waits, keeping Add alloc-free.
	notify chan struct{}

	// syncStop/syncDone coordinate the optional background syncer (SyncInterval);
	// both nil when it is not running.
	syncStop chan struct{}
	syncDone chan struct{}
}

// New opens (creating if necessary) a DiskQueue under the directory path. The segment
// count, durability, and recovery behaviour are tuned via Options (see
// Options.MaxSegments for the file-count cap, which defaults to 32).
func New[T any](path string, marshal MarshalFunc[T], unmarshal UnmarshalFunc[T], opts ...Options) (*DiskQueue[T], error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	st, err := openStore(path, segmentCapacity(opt.SegmentSize), resolveMaxSegments(opt.MaxSegments), opt.NoSync, opt.SyncEvery, opt.MaxMapped, opt.RecoverCorrupt)
	if err != nil {
		return nil, err
	}
	w := &DiskQueue[T]{marshal: marshal, unmarshal: unmarshal, st: st}
	if opt.SyncInterval > 0 && !opt.NoSync {
		w.syncStop = make(chan struct{})
		w.syncDone = make(chan struct{})
		go w.syncLoop(opt.SyncInterval)
	}
	return w, nil
}

// defaultMaxSegments bounds the live file count when Options.MaxSegments is left
// at its zero value: ~32 × SegmentSize of footprint by default.
const defaultMaxSegments = 32

// resolveMaxSegments maps Options.MaxSegments to the store's convention, where 0
// means unbounded: the zero value selects defaultMaxSegments, a negative value
// requests unbounded, and a positive value is used as-is.
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
	if ps := int64(os.Getpagesize()); c%ps != 0 {
		c = (c/ps + 1) * ps
	}
	return c
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
	before := w.st.writeOffset()
	err = w.st.append(b)
	// The record can land even when a per-op fsync then fails (append advances the
	// write offset before flushing). Wake waiters whenever it did, so a durability
	// error doesn't also strand a blocked consumer on a record that is in the log.
	if w.st.writeOffset() != before {
		w.signal()
	}
	return err
}

// Empty reports whether there are no items available to read.
func (w *DiskQueue[T]) Empty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.empty()
}

// Count returns the number of items added but not yet committed.
func (w *DiskQueue[T]) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int(w.st.count())
}

// Size returns the bytes of uncommitted records, roughly the data on disk.
func (w *DiskQueue[T]) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.size()
}

// Corruptions returns how many corruption events have been recovered from since
// open (torn trailing segments dropped on open plus segments quarantined on
// read). Always 0 unless RecoverCorrupt is set. A non-zero value means data was
// dropped.
func (w *DiskQueue[T]) Corruptions() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.corruptionCount()
}

// Sync flushes buffered writes to stable storage.
func (w *DiskQueue[T]) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.st.sync()
}

// Close flushes and closes the DiskQueue. Further use returns ErrClosed.
func (w *DiskQueue[T]) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.closed = true
	if w.notify != nil {
		close(w.notify)
		w.notify = nil
	}
	w.mu.Unlock()

	// Stop the background syncer before closing the store. The lock is released
	// so the syncer (which takes it each tick) can observe closed and exit; it
	// won't touch the store once closed is set.
	if w.syncStop != nil {
		close(w.syncStop)
		<-w.syncDone
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.close()
}

// syncLoop flushes the store on a fixed interval until Close stops it; a
// wall-clock backstop for SyncEvery batching.
func (w *DiskQueue[T]) syncLoop(d time.Duration) {
	defer close(w.syncDone)
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-w.syncStop:
			return
		case <-t.C:
			w.mu.Lock()
			if !w.closed {
				_ = w.st.sync()
			}
			w.mu.Unlock()
		}
	}
}

// waitLocked releases the lock, blocks until Add signals or ctx is done, then
// reacquires it. The caller must hold w.mu.
func (w *DiskQueue[T]) waitLocked(ctx context.Context) error {
	if w.notify == nil {
		w.notify = make(chan struct{})
	}
	ch := w.notify
	w.mu.Unlock()
	select {
	case <-ch:
		w.mu.Lock()
		return nil
	case <-ctx.Done():
		w.mu.Lock()
		return ctx.Err()
	}
}

// signal wakes any goroutines blocked in waitLocked. The caller must hold w.mu.
func (w *DiskQueue[T]) signal() {
	if w.notify != nil {
		close(w.notify)
		w.notify = nil
	}
}
