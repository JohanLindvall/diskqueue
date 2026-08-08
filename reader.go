package diskqueue

import (
	"context"
	"errors"
	"fmt"
	"iter"
)

// NewReader returns a Reader that consumes from this Queue; all read operations
// are methods on it.
//
// A Reader copies each record into a private reused buffer before unmarshalling,
// so the value never aliases the store's reused read buffer and stays valid until
// the Reader's next read (alloc-free once warm). A Reader is not safe for concurrent
// use: use one per consuming goroutine. Readers share the read/commit cursor and
// cooperate (each item delivered once); see the package doc on which ops are safe
// across concurrent readers.
func (w *Queue[T]) NewReader() *Reader[T] {
	return &Reader[T]{w: w}
}

// Reader is a consuming view over a Queue; create it with Queue.NewReader.
type Reader[T any] struct {
	w       *Queue[T]
	scratch []byte // record copy, reused across reads
	err     error  // why the last Drain/Follow stopped; reported by Err
}

// Err reports what went wrong during the most recent Drain or Follow: nil when
// the iteration simply ran out of items, the context was cancelled, or the
// Queue was closed. An iter.Seq cannot carry an error, so check this after
// the loop — otherwise a failure is indistinguishable from an empty queue.
//
// A read, decode or commit failure ends the iteration and is reported here.
// ErrCorrupt does not: the damage is already dropped and the queue has advanced,
// so iteration continues and the first event is kept here for the loop to find
// afterwards. Stats().LostBytes and LostRecords say how much was lost.
func (r *Reader[T]) Err() error { return r.err }

// TryPeek returns the front item WITHOUT consuming it: no cursor moves, the
// item stays exactly where it is, and the next read — by any Reader — returns
// it again. ok is false when the queue is empty.
//
// It is the inspection the consume ops cannot provide: TryReserve advances the
// shared read cursor (that is why Empty can be true while Count is not zero),
// while TryPeek leaves every cursor alone. The value is decoded through this
// Reader's buffer and is valid until the Reader's next read, like every other
// delivery.
//
// A damaged head returns ErrCorrupt as a PREVIEW: nothing is dropped, nothing
// is counted, and Stats does not move — the consume op that eventually steps
// past the damage books and reports it exactly once. Likewise TryPeek does not
// surface the corruption reports owed for segments dropped at open; those
// belong to the consume ops.
func (r *Reader[T]) TryPeek() (T, bool, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.closed {
		return zero, false, ErrClosed
	}
	payload, ok, err := r.w.st.peekHead()
	if err != nil || !ok {
		return zero, false, err
	}
	r.scratch = append(r.scratch[:0], payload...) // copy out of the store's read buffer
	// Deferred so the oversized-release rule also applies on the codec-error exit;
	// otherwise one huge record the codec rejects pins its buffer for the Reader's
	// lifetime. Safe after the return value is set: see trimScratch.
	defer r.trimScratch()
	v, uerr := r.w.unmarshal(r.scratch)
	if uerr != nil {
		return zero, false, fmt.Errorf("%w: %w", ErrCodec, uerr)
	}
	return v, true, nil
}

// TryReserve returns the front item and its offset without committing; ok is
// false when empty. Pass the offset to Commit (or call Take) to consume it.
func (r *Reader[T]) TryReserve() (T, bool, int64, error) {
	var zero T
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	defer r.w.signalSpace() // a quarantine on the read path can advance the commit cursor
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
	defer r.w.signalSpace() // the commit may have freed capacity a producer waits on
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
	defer r.w.signalSpace() // a quarantine on the read path can advance the commit cursor
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
	defer r.w.signalSpace() // the commit may have freed capacity a producer waits on
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
// An offset that falls inside a record rather than on a boundary is not an
// error: the commit stops at the last record ending at or before it — the bias
// everywhere is to redeliver, never to retire something nobody said was done.
func (r *Reader[T]) Commit(offset int64) error {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	defer r.w.signalSpace() // the commit may have freed capacity a producer waits on
	if r.w.closed {
		return ErrClosed
	}
	// The read cursor is the only bound that has to be stated: it never passes the
	// write cursor, so testing that as well would be dead.
	if offset > r.w.st.headOffset() {
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
//
// Skip acts on the SHARED head, not on a record this Reader holds. With several
// cooperating Readers it discards whatever is at the cursor when it runs, which
// may be a record another Reader would have handled — so call it from one
// consumer, or coordinate. It is the one consume operation that destroys a record
// without reading it, and the loss is not counted as corruption.
//
// Like every consume op, Skip can also surface a pending corruption report:
// ok=false with ErrCorrupt means the queue collected a loss (and may have
// stepped past damaged data), not that the record Skip was aimed at is gone —
// call it again to skip the record now at the head.
func (r *Reader[T]) Skip() (bool, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	defer r.w.signalSpace() // the commit may have freed capacity a producer waits on
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
// yields new ones until ctx is cancelled or the Queue is closed. Each item is
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
	defer w.signalSpace() // iterators commit as they read; producers may be waiting
	for {
		if w.closed {
			return zero, false, nil
		}
		// end is a snapshot and writeOff only grows, so reaching it is the whole
		// stopping condition — but drained() also insists the corruption backlog is
		// paid down, or losses dropped at open would never be reported to anyone.
		// Two stop conditions, not three: empty() is drained(writeOff) and writeOff
		// only ever grows past the snapshot, so empty() implies drained(end) and a
		// bounded iteration never needs to wait.
		if !follow {
			if w.st.drained(end) {
				return zero, false, nil
			}
		} else if w.st.empty() {
			if err := w.waitLocked(ctx); err != nil {
				return zero, false, nil // context done: an ordinary end of iteration
			}
			continue
		}
		before := w.st.progress()
		v, off, ok, err := r.read()
		if err != nil {
			// ErrCodec is checked FIRST and always stops the iteration. A codec
			// error may wrap anything the caller likes, ErrCorrupt included, so the
			// two must be told apart by which sentinel came from the library rather
			// than by what happens to be in the chain. It also leaves the record at
			// the head, so continuing would re-read it forever.
			//
			// Corruption the store actually stepped past is the opposite case: the
			// damage is already dropped and everything behind it is still
			// deliverable, so one bad record must not silently truncate the drain.
			// The first event is kept for Err, and Stats().LostBytes counts them all.
			// The progress check is what makes that safe — damage the store could
			// NOT step past reports the same error from the same offset forever.
			if !errors.Is(err, ErrCodec) && errors.Is(err, ErrCorrupt) && w.st.progress() != before {
				if r.err == nil {
					r.err = err
				}
				continue
			}
			return zero, false, err
		}
		if !ok {
			return zero, false, nil
		}
		// Commit before yielding, under the read's lock (like Take): atomic and
		// in cursor order, so concurrent iterations cooperate. Cost: at-most-once.
		// A commit that fails stops the iteration without yielding, so the item is
		// replayed after a reopen rather than handed out as if it were consumed.
		if err := w.st.commitTo(off); err != nil {
			// Deliberately NOT un-counted: Delivered counts records read out of the
			// store, and this one was read — only its commit failed. Nor may the read
			// cursor be rewound, because a partially successful commit advances the
			// commit cursor and drags the read cursor with it; putting it back would
			// leave it behind the commit cursor, addressing reclaimable records.
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

	// Put the head back on the record unless it is actually handed over — the same
	// stance as a corrupt record, so a codec failure can never consume one into
	// thin air. A defer rather than a branch because UnmarshalFunc may panic:
	// recovering that used to leave the record consumed AND, on the Take path,
	// committed by the caller's deferred unlock.
	delivered := false
	defer func() {
		if !delivered {
			r.w.st.rewindHead(start)
		}
	}()
	defer r.trimScratch() // on the codec exit too; see TryPeek
	v, err := r.w.unmarshal(r.scratch)
	if err != nil {
		// Wrap it so a codec error can never impersonate a library sentinel. A
		// UnmarshalFunc is free to return anything, including something that wraps
		// ErrCorrupt — and then errors.Is(err, ErrCorrupt) would tell the caller
		// that data on disk was damaged and dropped, while Stats shows no loss at
		// all and the record is still queued. ErrCodec keeps the two apart, and
		// the original error stays reachable through Unwrap.
		return zero, 0, false, fmt.Errorf("%w: %w", ErrCodec, err)
	}
	delivered = true
	return v, off, true, nil
}

// trimScratch releases a scratch buffer an oversized record grew past the
// ordinary geometry, so one exceptional record does not stay resident for the
// Reader's lifetime. Called after the value is handed over; anything in T that
// aliases the old array keeps it alive on its own, so the release only ever
// costs the next read one allocation.
func (r *Reader[T]) trimScratch() {
	r.scratch = trimOver(r.scratch, r.w.st.segmentSize)
}

// Requeue moves the record at the head of the queue to the BACK, without
// decoding it; ok is false when the queue is empty.
//
// It is the answer to a poison record — one the consumer cannot process and that
// would otherwise block the head forever. Skip is the other answer and it
// destroys the record; Requeue keeps it and lets everything behind it drain, so a
// single unprocessable item costs a reordering rather than either data loss or a
// stalled queue. Watch for it with Stats(): Delivered climbing while Committed
// stays flat on a non-empty backlog is a head record nobody can retire.
//
// The record is re-appended and only then committed at the head, in that order,
// because the reverse would lose it outright if the append failed. That ordering
// has a consequence: if the append succeeds and the commit does not, the record
// exists twice — once at the tail and once still at the head — which is what
// at-least-once already permits. A failed append moves nothing and leaves the
// record where it was.
//
// The re-append is EXEMPT from MaxBytes and MaxSegments. The rotation is
// backlog-neutral — the tail copy is followed immediately by the commit that
// retires the head original, so the net backlog is unchanged and the overshoot
// is transiently one record (at worst one segment). Enforcing the caps here
// inverted the method's purpose: commits are a cursor, so nothing behind an
// unprocessable head can retire first, and a poison head at a FULL queue —
// exactly when rotation matters most — could then never be moved, wedging the
// queue and pinning its disk across restarts.
//
// Two caveats. It BREAKS FIFO order for the record it moves, which is the point;
// a queue whose ordering is load-bearing should not use it. And like Skip it acts
// on the SHARED head rather than on a record this Reader is holding, so with
// several cooperating Readers it moves whatever is at the cursor when it runs —
// call it from one consumer, or coordinate.
func (r *Reader[T]) Requeue() (bool, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	defer r.w.signalSpace() // the head commit may free capacity a producer waits on
	if r.w.closed {
		return false, ErrClosed
	}
	// Quiesce any in-flight flush BEFORE the takeHead: the rotation must be
	// atomic under one continuous lock hold (its rewindHead on failure may not
	// undo another reader's progress), so its append runs the synchronous
	// solo path, which requires nothing staged and no leader mid-span.
	r.w.waitFlushQuiescedLocked()
	if r.w.closed {
		return false, ErrClosed
	}
	start := r.w.st.headOffset()
	payload, off, ok, err := r.w.st.takeHead()
	if err != nil || !ok {
		return false, err
	}
	// Copy out of the store's shared read buffer before appending: the append path
	// owns writeBuf, not readBuf, but every other consume op copies under the lock
	// for exactly this reason and a single-caller exception is how that stops being
	// true. The Reader's scratch is already the buffer for it.
	r.scratch = append(r.scratch[:0], payload...)
	if err := r.w.st.appendRecord(r.scratch, true); err != nil {
		r.w.st.rewindHead(start) // nothing was published; leave it at the head
		return false, err
	}
	r.w.signal() // a waiter blocked on an empty queue can have this one
	cerr := r.w.st.commitTo(off)
	r.trimScratch() // the copy served its purpose; don't pin an oversized one
	return true, cerr
}
