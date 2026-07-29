// Package diskqueue implements a generic, durable, FIFO disk-backed queue (a
// persistent work queue that doubles as a write-ahead log) backed by its own file
// store (see store.go) using plain pread/pwrite/fsync; its only dependency is
// cespare/xxhash/v2 (per-record checksums).
//
// Items are appended with Add and consumed through a Reader (Queue.NewReader): Take
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
// Concurrency: a Queue is safe for concurrent use; a single Reader is not — use one
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
	"sync"
	"time"
)

// MarshalFunc serializes v by appending to dst and returning the extended slice
// (like the builtin append). Appending rather than allocating keeps Add alloc-free.
//
// It is called with the Queue's internal lock held, so it must not call back
// into the Queue or any of its Readers — the mutex is not reentrant and doing
// so deadlocks.
type MarshalFunc[T any] func(dst []byte, v T) ([]byte, error)

// UnmarshalFunc decodes a value from data, a Reader-owned buffer valid only until
// that Reader's next read; copy out of it if you need it longer.
//
// Like MarshalFunc it runs under the Queue's lock and must not call back into
// the queue. Returning an error leaves the record at the head of the queue rather
// than consuming it, so the same record is offered again; use Reader.Skip to step
// over one the codec will never accept.
type UnmarshalFunc[T any] func(data []byte) (T, error)

// Errors returned by the package.
var (
	// ErrClosed is returned once the Queue has been closed.
	ErrClosed = errors.New("diskqueue: closed")
	// ErrFull is returned by Add when a new segment would exceed maxSegments.
	ErrFull = errors.New("diskqueue: full")
	// ErrInvalidOffset is returned by Commit for an offset beyond the last record.
	ErrInvalidOffset = errors.New("diskqueue: invalid offset")
	// ErrRecordTooLarge is returned by Add when a record cannot fit one segment.
	ErrRecordTooLarge = errors.New("diskqueue: record too large")
	// ErrCorrupt is returned by a read whose data failed its integrity check: a
	// record whose stored xxhash64 does not match, a length that overruns its
	// segment, or a segment dropped at open for a damaged header.
	//
	// The damaged data is dropped and the queue advances past it — the record
	// alone when its framing is still trustworthy, otherwise the rest of that
	// segment — so corruption degrades to reported loss rather than to plausible
	// looking garbage or a queue that never moves again. The error says one event
	// happened; Stats().LostBytes and LostRecords carry the magnitude it cannot.
	ErrCorrupt = errors.New("diskqueue: corrupt")
	// ErrSegmentSizeMismatch is returned by New when reopening a store with a
	// different SegmentSize than it was created with (which would discard data).
	ErrSegmentSizeMismatch = errors.New("diskqueue: segment size mismatch")
	// ErrLocked is returned by New when another Queue — in this process or
	// another — already holds the directory's advisory lock.
	ErrLocked = errors.New("diskqueue: directory already in use")
	// ErrIO wraps a durability failure that the queue cannot recover from in
	// place: an fsync that failed. The kernel reports such an error once and then
	// drops the dirty pages, so a later fsync may well succeed with the data
	// already gone — rather than report a durability it does not have, the queue
	// latches the failure and every subsequent Add, commit and Sync returns it
	// (wrapping the original errno). Close it and reopen to continue.
	ErrIO = errors.New("diskqueue: durability failure")
)

// Options tunes the behaviour of a Queue. The zero value is valid and selects
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

	// MaxOpenFiles caps how many segment files are kept open at once. Segments are
	// opened on demand and the least-recently-used handles are closed beyond the
	// cap, bounding open descriptors for deep backlogs; the active segment is
	// always open. 0 means unbounded (keep every touched segment open).
	//
	// Values are raised to a floor of 3, because the write, read and commit
	// cursors can each be in a different segment; a smaller cap evicts the handle
	// the next operation needs. Note that the open-file count is already bounded
	// by MaxSegments, so this is only worth setting when MaxSegments is unbounded.
	MaxOpenFiles int

	// SyncInterval, if > 0, runs a background goroutine that flushes to stable
	// storage on that period — a wall-clock backstop for SyncEvery batching, so an
	// idle queue's last writes become durable within the interval instead of
	// waiting for SyncEvery more operations. Ignored when NoSync is set.
	SyncInterval time.Duration
}

// Stats is a snapshot of a Queue's gauges and lifetime counters, for
// monitoring. It is a plain struct on purpose: no registry model is imposed on
// callers, and no callback of theirs runs under the queue's lock.
//
// The loss counters are what make corruption observable. ErrCorrupt says an
// event happened; LostBytes and LostRecords say how much it cost.
type Stats struct {
	// Gauges.
	BacklogBytes int64 // uncommitted bytes: the same number as Size
	Backlog      int64 // uncommitted records: the same number as Count
	Segments     int   // live segment files
	MaxSegments  int   // the configured cap; 0 when unbounded
	DiskBytes    int64 // what the segment files occupy, including preallocated slack

	// Counters since New.
	Added     uint64 // records accepted by Add
	Delivered uint64 // records handed to a reader, redeliveries included
	Committed uint64 // records retired by a commit
	Full      uint64 // Adds refused with ErrFull
	// Unreclaimed counts failed attempts to unlink a fully-committed segment.
	// A segment that will not unlink stays in the live set and is retried on the
	// next drop, so this climbing means disk is not being freed.
	Unreclaimed uint64

	// Loss, all since New. LostBytes is a lower bound: for a segment that
	// vanished from the directory, only its recorded size is left to count.
	LostBytes       uint64 // destroyed by corruption
	LostRecords     uint64 // individually dropped damaged records
	LostSegments    uint64 // segments abandoned or dropped whole
	ForeignSegments uint64 // dropped for a format version this build cannot read
	ForeignBytes    uint64
	DiscardedBytes  uint64 // trailing bytes a segment lost to truncation
	// Corruptions counts corruption events since New: segments dropped at open,
	// records dropped for a bad checksum, and segments abandoned for unusable
	// framing. Each one was, or will be, surfaced as exactly one ErrCorrupt from a
	// read — this is the number an operator alerts on, and the Lost* fields above
	// say how much each event cost.
	Corruptions uint64
}

// Queue is a generic persistent FIFO queue of T.
type Queue[T any] struct {
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

// New opens (creating if necessary) a Queue under the directory path. The segment
// count, durability, and recovery behaviour are tuned via Options (see
// Options.MaxSegments for the file-count cap, which defaults to 32).
func New[T any](path string, marshal MarshalFunc[T], unmarshal UnmarshalFunc[T], opts ...Options) (*Queue[T], error) {
	if marshal == nil || unmarshal == nil {
		// Caught here rather than as a nil call on the first Add: construction is
		// the last point where the caller can still do something about it.
		return nil, errors.New("diskqueue: marshal and unmarshal must be non-nil")
	}
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	st, err := openStore(path, segmentCapacity(opt.SegmentSize), resolveMaxSegments(opt.MaxSegments), opt.NoSync, opt.SyncEvery, opt.MaxOpenFiles)
	if err != nil {
		return nil, err
	}
	w := &Queue[T]{marshal: marshal, unmarshal: unmarshal, st: st}
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

// segmentAlign is what SegmentSize rounds up to. It is a fixed constant, not the
// host's page size: nothing is mapped any more, and rounding by the running
// host's page size would bake that host into the on-disk segment length — a store
// created on a 4 KiB-page machine would then fail to reopen on a 64 KiB-page one
// with ErrSegmentSizeMismatch.
const segmentAlign = 4096

func segmentCapacity(size int64) int64 {
	c := size
	if c <= 0 {
		c = 8 << 20 // 8 MiB default
	}
	if c < segmentAlign {
		c = segmentAlign
	}
	if c%segmentAlign != 0 {
		c = (c/segmentAlign + 1) * segmentAlign
	}
	return c
}

// Add appends data to the back of the log.
//
// A write that cannot be placed at all (ErrFull, ErrRecordTooLarge, a failed
// pwrite) leaves the queue untouched: the error means the item is not in it. The
// exception is a durability failure — if the error wraps ErrIO the record's bytes
// and the header publishing them did reach the page cache, so the item is in the
// log and readable, and only its power-loss durability is in doubt; the queue is
// then poisoned and every later operation repeats the error.
func (w *Queue[T]) Add(data T) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	b, err := w.marshal(w.scratch[:0], data)
	if b != nil {
		w.scratch = b // retain grown capacity for reuse, even from a failed marshal
	}
	if err != nil {
		return err
	}
	before := w.st.writeOffset()
	err = w.st.append(b)
	// Wake waiters whenever the record landed, so a durability error doesn't also
	// strand a blocked consumer on a record that is in the log.
	if w.st.writeOffset() != before {
		w.signal()
	}
	return err
}

// Empty reports whether there are no items available to read.
func (w *Queue[T]) Empty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.empty()
}

// Count returns the number of items added but not yet committed.
func (w *Queue[T]) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int(w.st.count())
}

// Size returns the bytes of uncommitted records.
//
// This is payload accounting, not disk usage: segments are preallocated, so what
// the queue occupies is Stats().DiskBytes, which is a multiple of the segment
// geometry and never smaller than this.
//
// It remains readable after Close and reports the final observed state.
func (w *Queue[T]) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.size()
}

// Stats returns a snapshot of the queue's gauges and lifetime counters. It
// remains readable after Close and reports the final observed state.
func (w *Queue[T]) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.stats()
}

// Err returns the latched durability failure, if any: nil while the queue is
// healthy, and an error wrapping ErrIO once an fsync has failed. A poisoned queue
// keeps serving reads but refuses every write, commit and sync, because the
// kernel reports a writeback error once and then discards the pages — a second
// fsync would report success over data that is already gone. Close it and reopen
// to continue; whatever was durable is still there, and uncommitted records
// replay.
func (w *Queue[T]) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.failure()
}

// Rewind returns every delivered-but-uncommitted record to the queue, so the
// next read starts from the commit cursor again. It reports the bytes made
// readable, and wakes any blocked reader.
//
// Reserve/Commit is an acknowledgement protocol, and this is its nack. Without
// it, a consumer that reserved records and then could not process them — a
// downstream that stayed down, a worker that gave up — left the read cursor
// ahead of the commit cursor with no way back: Empty reported true, Follow
// blocked, and the records were unreachable until the process restarted, even
// though they were still on disk and still uncommitted.
//
// It moves the *shared* cursor, which is why it is here and not on Reader. With
// cooperating readers it replays records other readers may still be working on,
// and those will be delivered a second time; that is within the at-least-once
// contract, but it means Rewind belongs to whoever owns the consumer group, not
// to one worker. Records already committed are unaffected — this cannot un-commit
// anything.
func (w *Queue[T]) Rewind() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	n := w.st.rewindToCommit()
	if n > 0 {
		w.signal()
	}
	return n, nil
}

// Sync flushes buffered writes to stable storage.
func (w *Queue[T]) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.st.sync()
}

// Close flushes and closes the Queue. Further use returns ErrClosed.
func (w *Queue[T]) Close() error {
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
func (w *Queue[T]) syncLoop(d time.Duration) {
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
				// Nowhere to return it to, but a failed fsync latches inside the
				// store, so the next Add/Sync/Close — or Err — reports it.
				_ = w.st.sync()
			}
			w.mu.Unlock()
		}
	}
}

// waitLocked releases the lock, blocks until Add signals or ctx is done, then
// reacquires it. The caller must hold w.mu.
func (w *Queue[T]) waitLocked(ctx context.Context) error {
	if w.notify == nil {
		w.notify = make(chan struct{})
	}
	ch := w.notify
	// Touch the caller's context while the lock is still held. A nil ctx is a
	// caller bug either way, but panicking after the unlock makes it an
	// unrecoverable one: the caller's deferred Unlock then runs on a mutex nobody
	// holds, which is a fatal runtime throw rather than a panic recover() can catch.
	done := ctx.Done()
	w.mu.Unlock()
	select {
	case <-ch:
		w.mu.Lock()
		return nil
	case <-done:
		w.mu.Lock()
		return ctx.Err()
	}
}

// signal wakes any goroutines blocked in waitLocked. The caller must hold w.mu.
func (w *Queue[T]) signal() {
	if w.notify != nil {
		close(w.notify)
		w.notify = nil
	}
}
