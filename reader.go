package diskqueue

import (
	"context"
	"errors"
	"iter"
)

// NewReader returns a Reader that consumes from this DiskQueue; all read operations
// are methods on it.
//
// A Reader copies each record into a private reused buffer before unmarshalling,
// so the value never aliases the store's reused read buffer and stays valid until
// the Reader's next read (alloc-free once warm). A Reader is not safe for concurrent
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
	err     error  // why the last Drain/Follow stopped; reported by Err
}

// Err reports what went wrong during the most recent Drain or Follow: nil when
// the iteration simply ran out of items, the context was cancelled, or the
// DiskQueue was closed. An iter.Seq cannot carry an error, so check this after
// the loop — otherwise a failure is indistinguishable from an empty queue.
//
// A read, decode or commit failure ends the iteration and is reported here.
// ErrCorrupt does not: the damage is already dropped and the queue has advanced,
// so iteration continues and the first event is kept here for the loop to find
// afterwards. Stats().LostBytes and LostRecords say how much was lost.
func (r *Reader[T]) Err() error { return r.err }

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
//
// A non-nil error with ok true means the item was read but its commit could not
// be persisted: the item is yours, and it will be delivered again after a reopen
// (the commit, not the read, is what is missing).
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
	return v, true, r.w.st.commitTo(off)
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

// Take blocks until an item is available (or ctx is done) and returns + commits
// it. As with TryTake, a non-nil error alongside ok true means the item was read
// but its commit did not reach disk, so it replays after a reopen.
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
			return v, true, r.w.st.commitTo(off)
		}
		if err := r.w.waitLocked(ctx); err != nil {
			return zero, false, err
		}
	}
}

// Commit marks the record at offset, and every record before it, as consumed.
// Committing an already-committed offset is a no-op.
//
// The offset must be one a read handed out: committing past the shared read
// cursor returns ErrInvalidOffset rather than reclaiming records nobody has seen
// (which would delete them, and the segment a reader is positioned in with them).
func (r *Reader[T]) Commit(offset int64) error {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return ErrClosed
	}
	if offset > r.w.st.writeOffset() || offset > r.w.st.headOffset() {
		return ErrInvalidOffset
	}
	return r.w.st.commitTo(offset)
}

// Skip consumes the record at the head of the queue without decoding it, and
// commits it; ok is false when the queue is empty.
//
// It is the deliberate way past a record UnmarshalFunc rejects. Because a decode
// error leaves the record in place — so a codec bug can never silently eat data —
// a consumer that has decided a record is unprocessable has to say so explicitly.
func (r *Reader[T]) Skip() (bool, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return false, ErrClosed
	}
	_, off, ok, err := r.w.st.takeHead()
	if err != nil || !ok {
		return false, err
	}
	return true, r.w.st.commitTo(off)
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
		r.err = nil
		w.mu.Lock()
		closed := w.closed
		end := w.st.writeOffset() // snapshot the upper bound for the non-follow case
		w.mu.Unlock()
		if closed {
			return
		}

		for {
			if ctx.Err() != nil {
				return
			}
			v, ok, err := r.next(ctx, follow, end)
			if err != nil {
				r.err = err // an iter.Seq cannot carry it; Err reports it
				return
			}
			if !ok {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}

// next produces the iterators' following item, committing it as it reads. ok is
// false when the iteration should stop for an ordinary reason (drained, closed,
// context done). It takes and releases the lock itself, with a defer, so a panic
// out of UnmarshalFunc cannot leave the queue's mutex held.
func (r *Reader[T]) next(ctx context.Context, follow bool, end int64) (T, bool, error) {
	var zero T
	w := r.w
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.closed {
			return zero, false, nil
		}
		if !follow && w.st.headOffset() >= end {
			return zero, false, nil
		}
		if w.st.empty() {
			if !follow {
				return zero, false, nil
			}
			if err := w.waitLocked(ctx); err != nil {
				return zero, false, nil // context done: an ordinary end of iteration
			}
			continue
		}
		v, off, ok, err := r.read()
		if errors.Is(err, ErrCorrupt) {
			// The damage has already been dropped and the queue has moved past it,
			// so keep iterating: one bad record must not silently truncate a drain
			// of everything behind it. The first event is kept for Err, and
			// Stats().LostBytes counts them all.
			if r.err == nil {
				r.err = err
			}
			continue
		}
		if err != nil || !ok {
			return zero, false, err
		}
		// Commit before yielding, under the read's lock (like Take): atomic and
		// in cursor order, so concurrent iterations cooperate. Cost: at-most-once.
		// A commit that fails stops the iteration without yielding, so the item is
		// replayed after a reopen rather than handed out as if it were consumed.
		if err := w.st.commitTo(off); err != nil {
			return zero, false, err
		}
		return v, true, nil
	}
}

// read takes the head record, copies it into scratch (so it never aliases the
// store's reused read buffer), and unmarshals it. ok is false when empty; a
// checksum mismatch returns ErrCorrupt. The caller must hold r.w.mu.
func (r *Reader[T]) read() (T, int64, bool, error) {
	var zero T
	start := r.w.st.headOffset()
	payload, off, ok, err := r.w.st.takeHead()
	if err != nil || !ok {
		return zero, 0, false, err
	}
	r.scratch = append(r.scratch[:0], payload...) // copy out of the store's read buffer
	v, err := r.w.unmarshal(r.scratch)
	if err != nil {
		// The record was never handed over, so put the head back on it rather than
		// consuming it into thin air — the same stance as a corrupt record.
		r.w.st.rewindHead(start)
		return zero, 0, false, err
	}
	return v, off, true, nil
}
