// Package wal implements a generic, durable, FIFO write-ahead log (a persistent
// queue) backed by its own mmap file store (see store.go); the only dependency
// is golang.org/x/sys.
//
// Items are appended with Add and consumed through a Reader (WAL.NewReader): Take
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
// Concurrency: a WAL is safe for concurrent use; a single Reader is not — use one
// per consuming goroutine. Readers share one read/commit cursor and cooperate
// (each item delivered once). The blocking methods honour their context.
//
// Crash semantics: at-least-once. On open the read cursor resets to the persisted
// commit cursor, so uncommitted items replay.
package wal

import (
	"context"
	"errors"
	"os"
	"sync"
)

// MarshalFunc serializes v by appending to dst and returning the extended slice
// (like the builtin append). Appending rather than allocating keeps Add alloc-free.
type MarshalFunc[T any] func(dst []byte, v T) ([]byte, error)

// UnmarshalFunc decodes a value from data, a Reader-owned buffer valid only until
// that Reader's next read; copy out of it if you need it longer.
type UnmarshalFunc[T any] func(data []byte) (T, error)

// Errors returned by the package.
var (
	// ErrClosed is returned once the WAL has been closed.
	ErrClosed = errors.New("wal: closed")
	// ErrFull is returned by Add when a new segment would exceed maxSegments.
	ErrFull = errors.New("wal: full")
	// ErrInvalidOffset is returned by Commit for an offset beyond the last record.
	ErrInvalidOffset = errors.New("wal: invalid offset")
	// ErrRecordTooLarge is returned by Add when a record cannot fit one segment.
	ErrRecordTooLarge = errors.New("wal: record too large")
)

// Options tunes the behaviour of a WAL. The zero value is valid and selects
// sensible defaults.
type Options struct {
	// NoSync disables msync after every write and commit. This trades durability
	// against a power loss for substantially higher throughput; data still
	// survives a process crash via the page cache. Default false.
	NoSync bool

	// SegmentSize sets each segment file's capacity. Default 8 MiB, floored at
	// 4 KiB and rounded up to a page. A record too big for one segment is
	// rejected with ErrRecordTooLarge.
	SegmentSize int64
}

// WAL is a generic persistent FIFO queue of T.
type WAL[T any] struct {
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
}

// New opens (creating if necessary) a WAL under the directory path.
//
// maxSegments caps how many segment files are kept at once; once reached, Add
// returns ErrFull until a segment is committed and reclaimed. The footprint is
// about maxSegments × SegmentSize bytes. A value <= 0 means unbounded.
func New[T any](path string, maxSegments int, marshal MarshalFunc[T], unmarshal UnmarshalFunc[T], opts ...Options) (*WAL[T], error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	st, err := openStore(path, segmentCapacity(opt.SegmentSize), maxSegments, opt.NoSync)
	if err != nil {
		return nil, err
	}
	return &WAL[T]{marshal: marshal, unmarshal: unmarshal, st: st}, nil
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
func (w *WAL[T]) Add(data T) error {
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
	if err := w.st.append(b); err != nil {
		return err
	}
	w.signal()
	return nil
}

// Empty reports whether there are no items available to read.
func (w *WAL[T]) Empty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.empty()
}

// Count returns the number of items added but not yet committed.
func (w *WAL[T]) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int(w.st.count())
}

// Size returns the bytes of uncommitted records, roughly the data on disk.
func (w *WAL[T]) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.size()
}

// Sync flushes buffered writes to stable storage.
func (w *WAL[T]) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.st.sync()
}

// Close flushes and closes the WAL. Further use returns ErrClosed.
func (w *WAL[T]) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	if w.notify != nil {
		close(w.notify)
		w.notify = nil
	}
	return w.st.close()
}

// waitLocked releases the lock, blocks until Add signals or ctx is done, then
// reacquires it. The caller must hold w.mu.
func (w *WAL[T]) waitLocked(ctx context.Context) error {
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
func (w *WAL[T]) signal() {
	if w.notify != nil {
		close(w.notify)
		w.notify = nil
	}
}
