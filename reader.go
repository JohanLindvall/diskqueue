package diskqueue

import (
	"context"
	"iter"
)

// NewReader returns a Reader that consumes from this DiskQueue; all read operations
// are methods on it.
//
// A Reader copies each record into a private reused buffer before unmarshalling,
// so the value never aliases an unmappable file and stays valid until the
// Reader's next read (alloc-free once warm). A Reader is not safe for concurrent
// use: use one per consuming goroutine. Readers share the read/commit cursor and
// cooperate (each item delivered once); see the package doc on which ops are safe
// across concurrent readers.
func (w *DiskQueue[T]) NewReader() *Reader[T] {
	return &Reader[T]{w: w}
}

// Reader is a consuming view over a DiskQueue; create it with DiskQueue.NewReader.
type Reader[T any] struct {
	w       *DiskQueue[T]
	scratch []byte // record copy, reused across reads
}

// TryReserve returns the front item and its offset without committing; ok is
// false when empty. Pass the offset to Commit (or call Take) to consume it.
func (r *Reader[T]) TryReserve() (T, bool, int64, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return zero, false, 0, ErrClosed
	}
	v, off, ok, err := r.read()
	if err != nil || !ok {
		return zero, false, 0, err
	}
	return v, true, off, nil
}

// TryTake returns and commits the front item; ok is false when empty.
func (r *Reader[T]) TryTake() (T, bool, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return zero, false, ErrClosed
	}
	v, off, ok, err := r.read()
	if err != nil || !ok {
		return zero, false, err
	}
	r.w.st.commitTo(off)
	return v, true, nil
}

// Reserve blocks until an item is available (or ctx is done), returning it and
// its offset without committing.
func (r *Reader[T]) Reserve(ctx context.Context) (T, bool, int64, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	for {
		if r.w.closed {
			return zero, false, 0, ErrClosed
		}
		v, off, ok, err := r.read()
		if err != nil {
			return zero, false, 0, err
		}
		if ok {
			return v, true, off, nil
		}
		if err := r.w.waitLocked(ctx); err != nil {
			return zero, false, 0, err
		}
	}
}

// Take blocks until an item is available (or ctx is done) and returns + commits it.
func (r *Reader[T]) Take(ctx context.Context) (T, bool, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	for {
		if r.w.closed {
			return zero, false, ErrClosed
		}
		v, off, ok, err := r.read()
		if err != nil {
			return zero, false, err
		}
		if ok {
			r.w.st.commitTo(off)
			return v, true, nil
		}
		if err := r.w.waitLocked(ctx); err != nil {
			return zero, false, err
		}
	}
}

// Commit marks the record at offset, and every record before it, as consumed.
// Committing an already-committed offset is a no-op.
func (r *Reader[T]) Commit(offset int64) error {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return ErrClosed
	}
	if offset > r.w.st.writeOffset() {
		return ErrInvalidOffset
	}
	r.w.st.commitTo(offset)
	return nil
}

// Drain iterates the items present when iteration begins, oldest first,
// committing each as it is read (like Take), so a loop that stops early does not
// replay the item it stopped on. Use Reserve/Commit to ack after processing.
// Safe for concurrent cooperating readers.
func (r *Reader[T]) Drain(ctx context.Context) iter.Seq[T] {
	return r.stream(ctx, false)
}

// Follow is like Drain but unbounded: after the existing items it waits for and
// yields new ones until ctx is cancelled or the DiskQueue is closed. Each item is
// committed as it is read (at-most-once; see Drain). The lock is released across
// yields, so other methods may be called from within the loop.
func (r *Reader[T]) Follow(ctx context.Context) iter.Seq[T] {
	return r.stream(ctx, true)
}

func (r *Reader[T]) stream(ctx context.Context, follow bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		w := r.w
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		end := w.st.writeOffset() // snapshot the upper bound for the non-follow case
		w.mu.Unlock()

		for {
			if ctx.Err() != nil {
				return
			}

			w.mu.Lock()
			if w.closed {
				w.mu.Unlock()
				return
			}
			if !follow && w.st.headOffset() >= end {
				w.mu.Unlock()
				return
			}
			if w.st.empty() {
				if !follow {
					w.mu.Unlock()
					return
				}
				if err := w.waitLocked(ctx); err != nil {
					w.mu.Unlock()
					return
				}
				w.mu.Unlock()
				continue
			}
			v, off, ok, err := r.read()
			if err != nil || !ok {
				w.mu.Unlock()
				return
			}
			// Commit before yielding, under the read's lock (like Take): atomic and
			// in cursor order, so concurrent iterations cooperate. Cost: at-most-once.
			w.st.commitTo(off)
			w.mu.Unlock()

			if !yield(v) {
				return
			}
		}
	}
}

// read takes the head record, copies it into scratch (so it never aliases the
// mmap), and unmarshals it. ok is false when empty; a checksum mismatch returns
// ErrCorrupt. The caller must hold r.w.mu.
func (r *Reader[T]) read() (T, int64, bool, error) {
	var zero T
	payload, off, ok, err := r.w.st.takeHead()
	if err != nil || !ok {
		return zero, 0, false, err
	}
	r.scratch = append(r.scratch[:0], payload...) // copy out of the mmap
	v, err := r.w.unmarshal(r.scratch)
	if err != nil {
		return zero, 0, false, err
	}
	return v, off, true, nil
}
