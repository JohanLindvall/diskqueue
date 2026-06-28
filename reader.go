package diskqueue

import (
	"context"
	"iter"
	"runtime"
)

// NewReader returns a Reader that consumes from this DiskQueue; all read
// operations are methods on it.
//
// Readers share the underlying read channel and cooperate (each item delivered
// once). A Reader holds no mutable state, so it is safe to share, but one per
// consuming goroutine remains the clear idiom.
func (w *DiskQueue[T]) NewReader() *Reader[T] {
	return &Reader[T]{w: w}
}

// Reader is a consuming view over a DiskQueue; create it with DiskQueue.NewReader.
type Reader[T any] struct {
	w *DiskQueue[T]
}

// TryTake returns the front item without blocking; ok is false when the queue is
// empty. The item's read cursor is advanced as it is delivered (at-most-once).
//
// The backend surfaces records to the read channel from a background goroutine,
// so a record may be on disk a moment before it reaches the channel. TryTake
// therefore spins while the backend still reports a non-zero depth, returning
// ok=false only once the queue is genuinely drained (or another consumer raced
// ahead for the last record).
func (r *Reader[T]) TryTake() (T, bool, error) {
	var zero T
	w := r.w
	for {
		select {
		case <-w.done:
			return zero, false, ErrClosed
		case b := <-w.dq.ReadChan():
			v, err := w.unmarshal(b)
			if err != nil {
				return zero, false, err
			}
			return v, true, nil
		default:
		}
		if w.dq.Depth() == 0 {
			return zero, false, nil
		}
		runtime.Gosched()
	}
}

// Take blocks until an item is available (or ctx is done) and returns it. The
// item's read cursor is advanced as it is delivered (at-most-once).
func (r *Reader[T]) Take(ctx context.Context) (T, bool, error) {
	var zero T
	w := r.w
	select {
	case <-w.done:
		return zero, false, ErrClosed
	case <-ctx.Done():
		return zero, false, ctx.Err()
	case b := <-w.dq.ReadChan():
		v, err := w.unmarshal(b)
		if err != nil {
			return zero, false, err
		}
		return v, true, nil
	}
}

// Drain iterates the items present when iteration begins, oldest first, advancing
// the read cursor as each is delivered, so a loop that stops early does not
// replay the item it stopped on. Drain iterations are serialized so concurrent
// drainers split the stream without loss, duplication, or deadlock.
func (r *Reader[T]) Drain(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		w := r.w
		w.drainMu.Lock()
		defer w.drainMu.Unlock()

		// Bound the drain to the records present now, and stop early if the queue
		// empties out (e.g. another consumer raced ahead) so a blocking receive
		// below can never wait for a record that will not arrive.
		n := w.dq.Depth()
		for consumed := int64(0); consumed < n; consumed++ {
			if ctx.Err() != nil || w.dq.Depth() == 0 {
				return
			}
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case b := <-w.dq.ReadChan():
				v, err := w.unmarshal(b)
				if err != nil {
					return
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}

// Follow is like Drain but unbounded: after the existing items it waits for and
// yields new ones until ctx is cancelled or the DiskQueue is closed. Each item's
// read cursor is advanced as it is delivered (at-most-once).
func (r *Reader[T]) Follow(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		w := r.w
		for {
			select {
			case <-w.done:
				return
			case <-ctx.Done():
				return
			case b := <-w.dq.ReadChan():
				v, err := w.unmarshal(b)
				if err != nil {
					return
				}
				if !yield(v) {
					return
				}
			}
		}
	}
}
