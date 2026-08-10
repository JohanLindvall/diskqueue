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
//
// Add and AddWait call it BEFORE taking the Queue's lock (into a pooled,
// per-call buffer), so an expensive codec no longer serializes producers or
// stalls consumers; AddBatch calls it with the lock held. Either way it must
// not call back into the Queue or any of its Readers — the mutex is not
// reentrant, and doing so deadlocks on the paths that hold it.
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
	// ErrClosed is returned by every operation that would touch the store once the
	// Queue has been closed. The pure observers are deliberately exempt and keep
	// reporting the final state: Count, Empty, Size, Stats and Err.
	ErrClosed = errors.New("diskqueue: closed")
	// ErrFull is returned by Add when the write cannot be admitted right now:
	// either a new segment would exceed Options.MaxSegments, or the uncommitted
	// backlog would exceed Options.MaxBytes. It is the TRANSIENT refusal — it
	// clears as the consumer commits — where ErrRecordTooLarge is permanent.
	ErrFull = errors.New("diskqueue: full")
	// ErrInvalidOffset is returned by Commit for an offset beyond the last record.
	ErrInvalidOffset = errors.New("diskqueue: invalid offset")
	// ErrRecordTooLarge is returned by Add for a record that can NEVER be
	// admitted, whatever the queue does next. Two things earn it: a framed length
	// beyond Options.MaxBytes, which no amount of draining changes; and — only on a
	// 32-bit build, and only for a payload near the addressable maximum — a framed
	// length this platform cannot index, which would otherwise wrap negative and
	// panic. It is the permanent refusal, where ErrFull is the transient one. A
	// record merely larger than SegmentSize is not an error — it is stored in a
	// segment sized to itself.
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
	// ErrCodec wraps an error returned by the caller's UnmarshalFunc. The record
	// is left at the head of the queue — a codec failure is not data loss, and the
	// same record is offered again — so use Reader.Skip to step over one the codec
	// will never accept.
	//
	// It exists to keep a codec error from impersonating a library sentinel: a
	// UnmarshalFunc may return anything, and an error that happened to wrap
	// ErrCorrupt would otherwise tell the caller that data on disk was damaged and
	// dropped, while nothing was damaged and nothing was dropped. The codec's own
	// error stays reachable through errors.Unwrap, which means it can still carry
	// ErrCorrupt — so TEST ErrCodec BEFORE ErrCorrupt. Only ErrCorrupt that is not
	// also ErrCodec means the queue lost data.
	ErrCodec = errors.New("diskqueue: unmarshal failed")
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
	// 4 KiB and rounded up to a multiple of 4 KiB — a fixed constant, deliberately
	// NOT the host's page size, so a store created on a 4 KiB-page machine still
	// reopens on a 64 KiB-page one. Values above 2^48-4096 are clamped to that
	// ceiling, which is what the 6-byte geometry field in each header can hold.
	// It is not a ceiling on record size: a
	// record too big for the geometry gets a segment sized to exactly itself.
	// Fixed at creation: reopening with a different (post-rounding) value is
	// rejected with ErrSegmentSizeMismatch.
	SegmentSize int64

	// MaxSegments caps how many segment files are kept at once; once reached, Add
	// returns ErrFull until a segment is committed and reclaimed. The footprint is
	// about MaxSegments × SegmentSize bytes — oversized records excepted, since
	// their segments are as long as the record; Stats().DiskBytes reports the real
	// number. 0 selects the default of 32; a negative value means unbounded.
	MaxSegments int

	// MaxBytes caps the uncommitted backlog in BYTES — the same number Size and
	// Stats().BacklogBytes report. Past it, Add returns ErrFull and the queue is
	// left untouched, so the caller chooses: block, drop, or shed load upstream.
	// 0 (the default) applies no byte cap, leaving MaxSegments as the only bound.
	//
	// It is worth setting because MaxSegments bounds the FILE COUNT, and the
	// footprint that follows from it (MaxSegments × SegmentSize) is a ceiling on
	// disk rather than a budget on the backlog: a queue holding one record per
	// segment is nowhere near its byte cap but may be at its segment cap. Sizing an
	// outage budget in bytes is the thing operators actually want to do, and
	// BacklogBytes/MaxBytes is the utilisation ratio to alert on — "70% and
	// climbing" is a signal, a bare byte count is not.
	//
	// The two caps compose: whichever binds first returns ErrFull. A record larger
	// than the cap itself can never be accepted and is refused with
	// ErrRecordTooLarge, which is permanent, rather than ErrFull, which is not.
	MaxBytes int64

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
	// UnsyncedBytes is record bytes that are written but not yet fsync'd: what a
	// power loss would cost right now, and how far a deferred sync policy has run
	// ahead of the last flush. It is always zero under the default per-op policy,
	// which fsyncs before Add returns, and climbs under NoSync or SyncEvery > 1
	// until a Sync, a batch flush or Close brings it back to zero.
	//
	// One window it does not cover, by construction: under group commit a span is
	// published — and therefore readable and counted in Backlog — for the duration
	// of its header fsync, during which those bytes are not yet durable. The gauge
	// stays zero there because the per-op policy's contract is that it is zero; the
	// Add that owns the span has not returned yet.
	//
	// A process crash does not lose these bytes — the kernel owns the pages — so
	// this measures exposure to power loss and kernel panic specifically. Watch it
	// against SyncInterval: if it keeps climbing, the backstop is not keeping up.
	UnsyncedBytes int64
	// InFlightBytes is the bytes handed to a reader but not yet committed —
	// BacklogBytes minus what is still unread. It is the state Rewind exists to
	// undo, and the number behind the documented oddity that Empty can be true
	// while Count is not zero. Climbing with a flat Committed means consumers are
	// taking work and not acknowledging it.
	InFlightBytes int64
	Segments      int   // live segment files
	MaxSegments   int   // the configured segment-count cap; 0 when unbounded
	MaxBytes      int64 // the configured backlog byte cap; 0 when unbounded
	DiskBytes     int64 // what the segment files occupy, including preallocated slack

	// Counters since New.
	Added uint64 // records accepted by Add
	// Delivered counts records READ OUT OF THE STORE by a Reader, redeliveries
	// included — one step earlier than "processed". A record whose COMMIT then
	// failed is counted, because the read is what happened and the record replays.
	// A record the codec REJECTED is not: Reader.read puts it back at the head, and
	// un-counts the delivery with it, so a permanently-failing UnmarshalFunc does
	// not inflate this. Compare against Committed to see work taken and not retired.
	Delivered uint64
	// Committed counts records retired by a commit. A record retired by the
	// corruption quarantine is included even though it reached no consumer, so
	// Committed can exceed Delivered on a damaged queue; those records are in the
	// Lost* fields too.
	Committed uint64
	Full      uint64 // Adds refused with ErrFull
	// Committed can exceed Delivered: a record dropped for a bad checksum is retired
	// without ever being handed out, and a quarantined segment retires its whole tail.
	// Both are counted in the Lost* fields too.
	//
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
	//
	// It counts EVENTS, not damaged regions, and one region can produce more than
	// one: a segment the read path quarantines is quarantined again when the commit
	// walk later crosses it, so a single unframable record can read as Corruptions=2
	// with LostSegments=1. The byte and record figures are booked once; this one
	// tracks the reports actually handed to consumers.
	Corruptions uint64
}

// Queue is a generic persistent FIFO queue of T.
type Queue[T any] struct {
	marshal   MarshalFunc[T]
	unmarshal UnmarshalFunc[T]

	mu     sync.Mutex
	st     *store
	closed bool

	// scratch is reused by AddBatch (which marshals under the lock) to
	// serialize values without allocating; Add and AddWait marshal outside the
	// lock through bufs instead.
	scratch []byte

	// bufs pools marshal buffers so Add and AddWait can serialize BEFORE
	// taking the lock — concurrent producers marshal concurrently — without
	// allocating on the hot path (the pool hands the same *marshalBuf back).
	bufs sync.Pool

	// notify is lazily created by a blocked consumer and closed by Add to wake
	// waiters; nil when nobody waits, keeping Add alloc-free.
	notify chan struct{}

	// spaceNotify wakes producers blocked in AddWait; consume ops close it
	// after a commit may have freed capacity. nil when nobody waits.
	spaceNotify chan struct{}

	// flushDrained wakes goroutines waiting for the store to quiesce — no
	// flush leader in flight, nothing staged. Cycling, AddBatch, Requeue and
	// Close all need that before touching what a span could still move.
	// nil when nobody waits.
	flushDrained chan struct{}

	// quiesceWant counts goroutines currently inside waitFlushQuiescedLocked, and
	// quiesceRelease parks arriving producers while any of them are. Without that
	// barrier the flush leader keeps finding a follower staged into its last span
	// and never leaves its loop, so a quiesce waits behind the producer stream
	// rather than behind the flush — seconds, not milliseconds. Both are zero/nil
	// when nobody is quiescing, so the uncontended Add path is one int compare.
	quiesceWant    int
	quiesceRelease chan struct{}

	// syncStop/syncDone coordinate the optional background syncer (SyncInterval);
	// both nil when it is not running.
	syncStop chan struct{}
	syncDone chan struct{}

	// syncMu serializes whole off-lock flushes (syncOffLock): it is held for the
	// full pin → fdatasync → clear span, so two concurrent Syncs can never
	// interleave their pin/clear cycles. A plain mutex rather than an in-progress
	// flag + wait loop on purpose — the mutex IS that machinery, with the parking
	// and the wakeup supplied by the runtime, and it doubles as Close's barrier:
	// acquiring it is how Close waits for every in-flight pin to drain before the
	// store's handles go away. Lock order: syncMu is always taken BEFORE w.mu and
	// never under it, so a holder blocked on w.mu can always be released by the
	// flush leader that holds it.
	syncMu sync.Mutex
}

// New opens (creating if necessary) a Queue under the directory path. The segment
// count, durability, and recovery behaviour are tuned via Options (see
// Options.MaxSegments for the file-count cap, which defaults to 32). The
// variadic opts exists only to make the whole argument optional: the first
// value is used and any further ones are ignored.
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
	// Set after the open rather than passed through it: the byte cap governs what
	// Add will accept, not how the store is laid out, so recovery never consults it
	// and a store written under one cap reopens cleanly under another.
	st.maxBytes = max(opt.MaxBytes, 0)
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
	// Every header states the SegmentSize it was created under, in a 6-byte
	// field — a 256 TiB ceiling. Clamp rather than let a preposterous
	// configuration silently truncate the field into a spurious
	// ErrSegmentSizeMismatch on reopen.
	if maxSeg := int64(1)<<48 - segmentAlign; c > maxSeg {
		return maxSeg
	}
	if c%segmentAlign != 0 {
		c = (c/segmentAlign + 1) * segmentAlign
	}
	return c
}

// marshalBuf is a pooled marshal buffer. The pool stores pointers so a warm
// Get/Put cycle allocates nothing.
type marshalBuf struct{ b []byte }

// marshalValue serializes data into a pooled buffer WITHOUT the queue lock, so
// codecs run concurrently across producers and never stall a consumer. The
// returned payload aliases the buffer; release it with putBuf once the append
// no longer needs the bytes (append copies them into the frame buffer).
func (w *Queue[T]) marshalValue(data T) (*marshalBuf, []byte, error) {
	mb, _ := w.bufs.Get().(*marshalBuf)
	if mb == nil {
		mb = new(marshalBuf)
	}
	b, err := w.marshal(mb.b[:0], data)
	if b != nil {
		mb.b = b // retain grown capacity for reuse, even from a failed marshal
	}
	if err != nil {
		w.putBuf(mb)
		return nil, nil, err
	}
	return mb, b, nil
}

// putBuf returns a marshal buffer to the pool, applying the package-wide
// oversized-release rule so one huge record does not stay resident in the pool
// for the queue's lifetime. segmentSize is immutable after New, so reading it
// without the lock is sound.
func (w *Queue[T]) putBuf(mb *marshalBuf) {
	mb.b = trimOver(mb.b, w.st.segmentSize)
	w.bufs.Put(mb)
}

// Add appends data to the back of the log.
//
// A write that cannot be placed at all (ErrFull, ErrRecordTooLarge, a failed
// pwrite) leaves the queue untouched: the error means the item is not in it.
//
// A durability failure is the one AMBIGUOUS answer. If the error wraps ErrIO the
// item may or may not be in the log, and the two arms are not distinguishable
// from the error: a failed HEADER fsync leaves the record real to everything
// short of a power loss (its bytes and the header publishing them both reached
// the page cache), while a failed DATA fsync publishes nothing at all. Either way
// the queue is poisoned and every later operation repeats the error, so the only
// recourse is to close and reopen — and a reopen answers the question definitively
// through Count and Stats. Treat an ErrIO as at-least-once ambiguous rather than
// as a confirmed placement.
//
// It can block. Besides waiting for its own flush, an Add yields to any concurrent
// Sync, AddBatch, Requeue or Close that is quiescing the store — without that, a
// steady producer stream starves those operations for seconds. The wait is bounded
// by the flush being quiesced, not by the caller.
//
// Serialization happens before the lock, and under the default per-op policy
// the two fsyncs are SHARED: concurrent Adds that arrive while a flush is in
// progress ride the next one (group commit), so per-record durability scales
// with producers instead of serializing on the disk. Every record is still
// individually durable when its Add returns.
func (w *Queue[T]) Add(data T) error {
	mb, payload, err := w.marshalValue(data)
	if err != nil {
		return err
	}
	defer w.putBuf(mb)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.addBytesLocked(payload)
}

// AddSized is Add, also reporting the framed size the record occupies on disk —
// length prefix, payload and checksum trailer. That is the unit MaxBytes and
// the Stats byte gauges count, so a producer metering its own throughput agrees
// with the store's accounting (Reader.LastBytes is the consuming side of the
// same number). The size is 0 when the add failed.
func (w *Queue[T]) AddSized(data T) (int64, error) {
	mb, payload, err := w.marshalValue(data)
	if err != nil {
		return 0, err
	}
	defer w.putBuf(mb)
	n := framedLen(len(payload))
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if err := w.addBytesLocked(payload); err != nil {
		return 0, err
	}
	return n, nil
}

// AddWait is Add with backpressure: where Add answers ErrFull, AddWait blocks
// until a commit frees capacity (or ctx is done) and then retries. Every other
// error — ErrRecordTooLarge included, which no amount of draining changes —
// returns immediately, exactly as from Add.
//
// Each refused attempt still counts in Stats().Full, so that counter reads as
// "attempts refused" rather than "items dropped" when producers wait.
//
// ctx bounds the ErrFull wait, not the whole call: like Add, this yields to a
// concurrent quiesce and waits on its own flush, and neither of those consults ctx.
// A deadline can therefore be overshot by the length of a flush. It is a delay, not
// a hang — every such wait is released by an operation already in flight.
func (w *Queue[T]) AddWait(ctx context.Context, data T) error {
	mb, payload, err := w.marshalValue(data)
	if err != nil {
		return err
	}
	defer w.putBuf(mb)
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.closed {
			return ErrClosed
		}
		err := w.addBytesLocked(payload)
		if !errors.Is(err, ErrFull) {
			return err
		}
		if werr := w.waitSpaceLocked(ctx); werr != nil {
			return werr
		}
	}
}

// AddBatch appends items in order, amortizing the lock and — under the per-op
// policy — the fsyncs across the whole batch: the records' bytes go down
// first, one data fsync covers them all, then one header write and one header
// fsync publish them together (per segment crossed). Every published record is
// durable when AddBatch returns.
//
// It returns how many leading items are in the queue. n < len(items) comes
// with the error that stopped the batch (ErrFull, a marshal error, an I/O
// failure); the first n items are placed and durable regardless.
//
// Unlike Add and AddWait, which marshal BEFORE taking the lock, AddBatch calls
// MarshalFunc with the queue mutex held. A codec that panics there is therefore a
// store-state hazard as well as a caller-visible one; the panic is passed through
// unchanged, with the staged-but-unpublished tail discarded first so the queue stays
// usable and closable. A power loss
// during AddBatch truncates it cleanly to a published prefix — never a torn
// record, never a phantom.
func (w *Queue[T]) AddBatch(items []T) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	before := w.st.writeOffset()
	n, err := w.addBatchLocked(items)
	w.dropOversizedScratch()
	if w.st.writeOffset() != before {
		w.signal()
	}
	return n, err
}

// addBatchLocked stages every record and publishes one span per segment crossed,
// on EVERY durability policy. The per-op policy gets the fsyncs amortized; the
// deferred policies get the header writes amortized, which is the cost that
// dominates there — the plain append path writes a 64-byte header per record, so
// a batch used to cost two pwrites per record on exactly the policies chosen for
// throughput.
//
// Every early return either publishes what is staged or discards it — and the
// deferred recover above covers the one exit that is not a return, a panic out of
// the caller's MarshalFunc — so the store is never left holding a staged tail no
// leader will settle. Each publish call is
// sequenced BEFORE the return statement that reports its count: reading
// `published` in the same return that mutates it through a pointer would depend on
// an evaluation order the Go spec leaves unspecified.
func (w *Queue[T]) addBatchLocked(items []T) (int, error) {
	st := w.st
	// AddBatch is the one entry point that runs the caller's MarshalFunc under the
	// queue mutex, so a panicking codec unwinds THROUGH the staging loop, past every
	// publish and discard site. Left staged, that tail is a span no leader will ever
	// settle — and on the deferred policies there is no leader at all — so every
	// later quiesce, Close included, would block forever on a store the caller can
	// still Add to. Discard and re-panic: the caller's panic is theirs to handle,
	// the store's state is not.
	defer func() {
		if r := recover(); r != nil {
			st.discardStaged()
			panic(r)
		}
	}()
	// Quiesce first so the batch is the only thing staging — its spans then
	// discard cleanly on failure.
	w.waitFlushQuiescedLocked()
	if w.closed {
		return 0, ErrClosed // Close ran while the quiesce had the lock released
	}
	published := 0
	for _, it := range items {
		b, err := w.marshal(w.scratch[:0], it)
		if b != nil {
			w.scratch = b
		}
		if err != nil {
			perr := w.publishBatchLocked(&published)
			return published, errors.Join(err, perr)
		}
		recLen := framedLen(len(b))
		if st.ioErr != nil {
			// Discard rather than publish: the store is latched, so publishing would
			// fail anyway — and returning with a staged tail in place leaves
			// pendingBytes with no leader to settle it, which wedges every later
			// quiesce (Close included) forever.
			st.discardStaged()
			return published, st.ioErr
		}
		if err := st.admitRecord(recLen, false); err != nil {
			perr := w.publishBatchLocked(&published)
			return published, errors.Join(err, perr)
		}
		if st.needsCycle(recLen) {
			// Records never span files, and neither do spans: publish what is
			// staged before the active file is replaced.
			if err := w.publishBatchLocked(&published); err != nil {
				return published, err
			}
			if err := st.cycle(recLen, false); err != nil {
				if errors.Is(err, ErrFull) {
					st.nFull++
				}
				return published, err
			}
		}
		if err := st.stagePending(b); err != nil {
			perr := w.publishBatchLocked(&published)
			return published, errors.Join(err, perr)
		}
		// SyncEvery bounds the number of unsynced WRITES, and that bound has to hold
		// inside a batch too: publishing only at the end would make the peak exposure
		// len(items) rather than N. Publish at each tick boundary instead, which is
		// where the old per-record loop would have flushed anyway.
		if st.batched() && st.unsynced+int(st.pendingRecs) >= st.syncEvery {
			if err := w.publishBatchLocked(&published); err != nil {
				return published, err
			}
		}
	}
	// Sequenced, not folded into the return: reading `published` in the same return
	// statement that mutates it through a pointer depends on an evaluation order the
	// Go spec leaves unspecified — the trap this function's own doc names.
	perr := w.publishBatchLocked(&published)
	return published, perr
}

// publishBatchLocked makes every pending staged record durable without
// releasing the lock: data fsync, publish, header fsync — the same span shape
// group commit uses, amortized across a batch instead of across concurrent
// producers. published is advanced by the records that became real: on the
// fsync-after-publish failure arm the records stay (the header reached the
// page cache), exactly like a solo append's last arm, and the latch carries
// the bad news.
func (w *Queue[T]) publishBatchLocked(published *int) error {
	st := w.st
	if st.pendingBytes == 0 {
		return nil
	}
	st.takeSpan()
	af := st.active()
	recs := int(st.inFlightRecs)
	span := st.inFlight
	if !st.perOp() {
		// Deferred policies: the record bytes and the header both land in the page
		// cache and one later flush covers them, exactly as the plain append path
		// does. A torn tail from a power loss between flushes is caught by the
		// per-record checksum.
		if err := st.publishSpan(); err != nil {
			return err // the span and anything behind it were discarded
		}
		*published += recs
		st.unsyncedBytes += span
		if st.batched() {
			// Count every record, not the span: SyncEvery is documented as a count
			// of writes, and collapsing a batch into one tick would silently widen
			// the power-loss window it exists to bound.
			return st.recordOps(recs)
		}
		return nil
	}
	if err := faultPoint("append.syncData"); err != nil {
		st.discardStaged()
		return st.failIO(err)
	}
	if err := datasync(af.f); err != nil {
		st.discardStaged()
		return st.failIO(err)
	}
	if err := st.publishSpan(); err != nil {
		return err // the span and anything behind it were discarded
	}
	*published += recs
	if err := faultPoint("append.syncHeader"); err != nil {
		return st.failIO(err)
	}
	if err := datasync(af.f); err != nil {
		return st.failIO(err)
	}
	af.dirty = false
	return nil
}

// addBytesLocked routes one serialized record to the policy's append path:
// the staged span machinery under per-op durability, the plain append
// otherwise. Caller holds w.mu and has checked closed.
func (w *Queue[T]) addBytesLocked(payload []byte) error {
	if w.st.perOp() {
		return w.addDurableLocked(payload, false)
	}
	before := w.st.writeOffset()
	err := w.st.append(payload)
	// Wake waiters whenever the record landed, so a durability error doesn't
	// also strand a blocked consumer on a record that is in the log.
	if w.st.writeOffset() != before {
		w.signal()
	}
	return err
}

// addDurableLocked is the per-op append: durable when it returns, two fsyncs —
// but shared. The record is staged (bytes written past the published extent,
// header untouched), then this goroutine either leads a flush span or waits
// for the leader already running to cover it with the next one. A solo Add
// leads a span of one and allocates nothing; concurrent Adds amortize the data
// fsync, the header write and the header fsync across every record in the
// span. Failure semantics per record are exactly the solo append's: a failed
// data fsync latches with nothing published, a failed header write discards
// with nothing latched, a failed header fsync latches with the records real in
// the page cache.
func (w *Queue[T]) addDurableLocked(payload []byte, force bool) error {
	st := w.st
	recLen := framedLen(len(payload))
	for {
		// Re-checked every pass: a quiesce below releases the lock, and the
		// store may have closed or latched while it was away — staging onto a
		// closed store would reopen handles Close just released.
		if w.closed {
			return ErrClosed
		}
		if st.ioErr != nil {
			return st.ioErr
		}
		// Yield to anyone waiting to quiesce, before staging rather than after: a
		// staged record is what keeps the waiter's predicate true.
		if w.quiesceWant > 0 {
			w.waitQuiesceReleaseLocked()
			continue
		}
		if err := st.admitRecord(recLen, force); err != nil {
			return err
		}
		if !st.needsCycle(recLen) {
			break
		}
		// The active file is full behind its staged tail. Quiesce before
		// cycling — every staged record lives in the active file, which is
		// what makes a failed span's rollback one contiguous tail — and then
		// re-evaluate everything: the world changed while the lock was away.
		if st.flushing || st.stagedBytes() > 0 {
			w.waitFlushQuiescedLocked()
			continue
		}
		if err := st.cycle(recLen, force); err != nil {
			if errors.Is(err, ErrFull) {
				st.nFull++
			}
			return err
		}
	}
	if err := st.stagePending(payload); err != nil {
		return err
	}
	if st.flushing {
		// A leader is mid-flush: this record joins the next span. The group is
		// allocated by the first follower of the window and shared by the rest.
		g := st.curGroup
		if g == nil {
			g = &flushGroup{done: make(chan struct{})}
			st.curGroup = g
		}
		done := g.done
		w.mu.Unlock()
		<-done
		w.mu.Lock()
		return g.err
	}
	return w.leadFlushLocked()
}

// leadFlushLocked drives flush spans until nothing is pending: take the span,
// release the lock, fsync the data, retake the lock, publish, release, fsync
// the header. Producers that arrive during the fsyncs stage behind the span
// and are flushed by the next iteration — that sharing is the whole point.
// The leader's own verdict is the first span's; each follower group gets its
// span's verdict through its done channel.
func (w *Queue[T]) leadFlushLocked() error {
	st := w.st
	st.flushing = true
	var mine error
	first := true
	for {
		g := st.curGroup
		st.curGroup = nil
		st.takeSpan()
		af := st.active()
		f := af.f
		var err error
		if ferr := faultPoint("append.syncData"); ferr != nil {
			err = ferr
		} else {
			w.mu.Unlock()
			err = datasync(f)
			w.mu.Lock()
		}
		switch {
		case err != nil:
			// The span's bytes may be gone and nothing was published: discard
			// the span and the tail behind it, and latch — this is an fsync.
			st.discardStaged()
			err = st.failIO(err)
		default:
			if err = st.publishSpan(); err == nil {
				w.signal() // the span's records are readable from here on
				if ferr := faultPoint("append.syncHeader"); ferr != nil {
					err = st.failIO(ferr)
				} else {
					w.mu.Unlock()
					herr := datasync(f)
					w.mu.Lock()
					if herr != nil {
						// The header reached the page cache: the records are
						// real to everything short of a power loss. They stay,
						// and the store is poisoned.
						err = st.failIO(herr)
					}
				}
				// dirty deliberately stays set: a commit may have rewritten the
				// header while the lock was away, and clearing it here would
				// skip that write's flush. The cost is one redundant fsync at
				// the next sync or close.
			}
			// A failed publishSpan discarded the span and everything behind it.
		}
		if first {
			mine, first = err, false
		}
		if g != nil {
			g.err = err
			close(g.done)
		}
		if err != nil {
			// Whatever was staged behind the failed span is discarded (or the
			// store is latched); fail the waiting followers rather than
			// flushing records that no longer exist.
			if ng := st.curGroup; ng != nil {
				st.curGroup = nil
				ng.err = err
				close(ng.done)
			}
			st.discardStaged()
			break
		}
		if st.curGroup == nil {
			break
		}
	}
	st.flushing = false
	w.signalFlushDrained()
	return mine
}

// waitFlushQuiescedLocked blocks until no flush leader is running and nothing
// is staged, reacquiring the lock before returning. On return the on-disk view
// is settled; with the lock then held continuously, nothing can begin staging
// underneath the caller.
func (w *Queue[T]) waitFlushQuiescedLocked() {
	// Hold arriving producers off for the duration of the wait. The count, not a
	// bool: several goroutines can be quiescing at once, and the last one out is
	// what releases the producers.
	w.quiesceWant++
	defer func() {
		w.quiesceWant--
		if w.quiesceWant == 0 && w.quiesceRelease != nil {
			close(w.quiesceRelease)
			w.quiesceRelease = nil
		}
	}()
	for w.st.flushing || w.st.stagedBytes() > 0 {
		if w.flushDrained == nil {
			w.flushDrained = make(chan struct{})
		}
		ch := w.flushDrained
		w.mu.Unlock()
		<-ch
		w.mu.Lock()
	}
}

// waitQuiesceReleaseLocked parks a producer until no goroutine is quiescing,
// reacquiring the lock before returning. The caller must hold w.mu and must
// re-evaluate everything afterwards — the lock was released.
//
// It is deliberately NOT waitFlushQuiescedLocked. Sending producers there
// livelocks: a producer whose predicate is already false returns immediately
// still holding the mutex, re-checks the barrier, re-enters, and spins on the
// lock — starving the very waiter it was meant to yield to. A separate channel,
// closed only when the last quiescer leaves, is what actually hands the lock over.
// It takes no context: the caller's deadline is re-checked on the loop it returns
// to, so a parked producer can overshoot by however long the quiesce takes — bounded
// by the flush it is waiting on, not unbounded.
func (w *Queue[T]) waitQuiesceReleaseLocked() {
	if w.quiesceRelease == nil {
		w.quiesceRelease = make(chan struct{})
	}
	ch := w.quiesceRelease
	w.mu.Unlock()
	<-ch
	w.mu.Lock()
}

// signalFlushDrained wakes everything blocked in waitFlushQuiescedLocked.
// The caller must hold w.mu.
func (w *Queue[T]) signalFlushDrained() {
	if w.flushDrained != nil {
		close(w.flushDrained)
		w.flushDrained = nil
	}
}

// waitSpaceLocked releases the lock, blocks until a consume op signals that a
// commit may have freed capacity (or ctx is done), then reacquires it. The
// caller must hold w.mu. Wakeups are permission to retry, not a guarantee of
// room: AddWait re-checks the caps and may wait again.
func (w *Queue[T]) waitSpaceLocked(ctx context.Context) error {
	if w.spaceNotify == nil {
		w.spaceNotify = make(chan struct{})
	}
	ch := w.spaceNotify
	// Touch ctx while the lock is held, for the same reason waitLocked does: a
	// nil ctx should panic somewhere recover() can still work.
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

// signalSpace wakes producers blocked in AddWait. Consume ops call it after
// any commit that may have freed capacity; spurious wakes are fine (the waiter
// re-checks). The caller must hold w.mu.
func (w *Queue[T]) signalSpace() {
	if w.spaceNotify != nil {
		close(w.spaceNotify)
		w.spaceNotify = nil
	}
}

// dropOversizedScratch releases the marshal buffer once it has grown past the
// segment geometry — the size of the largest ORDINARY record, and therefore of
// anything the steady state ever needs. Oversized records are admitted (into
// segments of their own), so this runs on success as well as failure; see
// trimOver for the package-wide policy.
func (w *Queue[T]) dropOversizedScratch() {
	w.scratch = trimOver(w.scratch, w.st.segmentSize)
}

// Empty reports whether there are no items available to read.
//
// It also stays false while corruption reports are owed — losses from segments
// dropped at open, which no read of their own will ever fail on. Only a consume
// op discharges those (TryPeek deliberately does not), so a blocking consumer
// wakes up, collects each ErrCorrupt, and only then sees an empty queue. That is
// why Empty can be false with Count, Size and TryPeek all saying nothing is there.
//
// It remains readable after Close and reports the final observed state.
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
// Offsets handed out before a Rewind become invalid with it: Commit rejects
// anything past the shared read cursor, so a Commit of a pre-Rewind offset answers
// ErrInvalidOffset. That is the nack doing its job — the record was returned to the
// queue — but a consumer holding reserved offsets has to expect it.
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
//
// The per-file fdatasyncs run WITHOUT the queue mutex held, so a large flush —
// the SyncInterval backstop over a deep unsynced backlog, say — does not stall
// every concurrent Add and read for the duration of the disk write-back.
// Everything Sync promises still holds: on a nil return, every byte written
// before the call is durable (bytes written DURING it may or may not be, and
// stay reported in Stats().UnsyncedBytes either way).
func (w *Queue[T]) Sync() error { return w.syncOffLock() }

// syncOffLock is the off-lock flush behind Sync and the SyncInterval backstop.
// Dirty files are pinned under the lock (a pinned file is never closed by
// eviction or reclamation — see dataFile.pins), the fdatasyncs run without it,
// and each file's dirty flag is cleared on the way back only if its writeSeq is
// unchanged, so bytes staged while the flush was in flight stay marked dirty
// and stay counted: the unsynced gauges subtract the pre-flush snapshot rather
// than resetting, which is what keeps them honest under concurrent producers.
//
// syncMu is held for the whole span. That serializes concurrent Syncs — two
// interleaved pin/clear cycles could otherwise each clear flags the other's
// writes re-dirtied — and it is the barrier Close takes to wait out in-flight
// pins. Holding it also means the STORE cannot close underneath the flush
// (Close acquires syncMu before st.close()), so the tail below never needs to
// distinguish a closed queue: the closed flag only refuses NEW operations,
// while this one was admitted while the queue was open and its handles are
// guaranteed to outlive it.
func (w *Queue[T]) syncOffLock() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	// Let any in-flight flush span settle first, so Sync's promise covers a
	// stable extent rather than racing a leader's publication.
	w.waitFlushQuiescedLocked()
	if w.closed {
		return ErrClosed
	}
	st := w.st
	// The latch check sits AFTER the quiesce, where st.sync's used to: a leader
	// that failed while the quiesce had the lock released must be reported, not
	// flushed over.
	if st.ioErr != nil {
		return st.ioErr
	}
	// Pin every dirty file and capture its handle and write seq. A dirty file
	// whose handle was evicted is reopened here (dirty means "has unsynced
	// bytes", not "is open"); a reopen that fails is NOT latched — the data is
	// still dirty and the next flush retries it, flushFile's own contract — but
	// it is reported, and it keeps the gauges standing.
	type target struct {
		df  *dataFile
		f   *os.File
		seq uint64
		err error
	}
	var targets []target
	var errs error
	for _, df := range st.files {
		if !df.dirty {
			continue
		}
		if err := st.ensureOpen(df); err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		df.pins++
		targets = append(targets, target{df: df, f: df.f, seq: df.writeSeq})
	}
	snapOps, snapBytes, snapEpoch := st.unsynced, st.unsyncedBytes, st.flushEpoch
	w.mu.Unlock()

	for i := range targets {
		if err := faultPoint("sync.file"); err != nil {
			targets[i].err = err
			continue
		}
		targets[i].err = datasync(targets[i].f) // keep going; flush what can be flushed
	}

	w.mu.Lock()
	for i := range targets {
		tg := &targets[i]
		tg.df.pins--
		switch {
		case tg.err != nil:
			// An fsync failure latches, exactly as the in-lock flush did: the
			// pages may already be dropped, and a retry would report success over
			// data that is gone. The file's dirty flag deliberately stays set —
			// over-reporting exposure, never under-reporting it.
			errs = errors.Join(errs, st.failIO(tg.err))
		case tg.df.writeSeq == tg.seq:
			tg.df.dirty = false
		}
	}
	if errs != nil {
		return errs
	}
	if st.ioErr != nil {
		// Latched by an interleaved in-lock flush — a SyncEvery boundary, a dirty
		// eviction — while the fdatasyncs here were in flight. Some file's
		// writeback failed, so this Sync may not answer "durable" just because
		// its own targets succeeded; the gauges stay standing with the latch.
		return st.ioErr
	}
	// Every file that was dirty at the snapshot flushed clean; what producers
	// wrote during the fdatasyncs remains dirty, seq-guarded above, and remains
	// counted here — unless an interleaved flushBatch already zeroed the gauges
	// (the epoch moved), in which case they now describe only what accumulated
	// since it and the stale snapshot must not be subtracted from that.
	if st.flushEpoch == snapEpoch {
		st.unsynced = max(st.unsynced-snapOps, 0)
		st.unsyncedBytes = max(st.unsyncedBytes-snapBytes, 0)
	}
	return nil
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
	w.signalSpace() // producers blocked in AddWait must wake to see closed
	w.mu.Unlock()

	// Stop the background syncer before closing the store. The lock is released
	// so the syncer (which takes it each tick) can observe closed and exit; it
	// won't touch the store once closed is set.
	if w.syncStop != nil {
		close(w.syncStop)
		<-w.syncDone
	}

	// Wait out any in-flight Sync before the handles go away: its fdatasyncs run
	// off the queue mutex against pinned files, and st.close() below closes every
	// one of them. Acquiring syncMu IS that wait — syncOffLock holds it for its
	// whole span — and holding it across the close parks any Sync arriving now
	// until the store is closed, where it observes the closed flag and returns
	// ErrClosed without touching a handle. Ordered AFTER the syncer stop above:
	// the syncer's own flush holds syncMu too, so taking it first would deadlock
	// the <-syncDone against a tick that can never finish.
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()
	// A flush leader may still be between fsyncs (closed only stops NEW
	// operations). Let it finish before the handles go away underneath it —
	// its followers get their verdicts, and the store closes quiesced.
	w.waitFlushQuiescedLocked()
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
			// Nowhere to return it to, but a failed fsync latches inside the
			// store, so the next Add/Sync/Close — or Err — reports it. The
			// off-lock flush checks closed itself, and Close waits for this
			// tick's flush (via syncMu) before it stops the loop.
			_ = w.syncOffLock()
		}
	}
}

// waitLocked releases the lock, blocks until Add signals or ctx is done, then
// reacquires it. The caller must hold w.mu.
//
// The channel is created per wait and closed to broadcast, so a consumer that
// keeps up — and therefore parks between records — costs one channel allocation
// per record. That is inherent to close-as-broadcast and is the only allocation
// on any hot path; every non-blocking operation stays at zero.
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
