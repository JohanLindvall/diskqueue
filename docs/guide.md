# User guide

[Back to the README](../README.md) · [Runnable examples](../examples/README.md) ·
[Go API reference](https://pkg.go.dev/github.com/JohanLindvall/diskqueue)

- [Usage](#usage)
- [API](#api)
- [Semantics](#semantics)
- [Recovery](#recovery)
- [Failure handling](#failure-handling)
- [Options](#options)
- [Storage layout](#storage-layout)

## Usage

Start with the complete [quick start](../README.md#usage) or run the
[basic and JSON replay examples](../examples/README.md). The snippets below
assume an open queue `w`, a context `ctx`, and your application's `process`
function. Use one `Reader` per consuming goroutine.

### Codecs and value ownership

The queue stores bytes; your codec determines how application values are
serialized. `MarshalFunc` should append to the supplied buffer and return it.
`Add` and `AddWait` can call the codec concurrently, so avoid shared mutable
codec state unless you synchronize it.

`UnmarshalFunc` receives a reader-owned buffer that is reused on that reader's
next read. If the returned value contains slices pointing into it, copy those
bytes when they need to live longer. The string and JSON examples create owned
values; the fixed-width integer codec in [example_test.go](../example_test.go)
avoids allocations.

Codecs must not call back into the queue or its readers: decoding and
`AddBatch` encoding run under the queue lock. Reopen a queue with a compatible
codec and the same `SegmentSize` and `StampRecords` settings.

<a id="reserve--commit-at-least-once-with-explicit-acknowledgement"></a>

### Reserve and acknowledge

When you need to process an item before acknowledging it, use `Reserve` instead
of `Take`, and acknowledge with `Ack`:

```go
r := w.NewReader()
v, ok, offset, err := r.Reserve(ctx)
if err != nil {
	return err
}
if !ok {
	return nil
}
if err := process(v); err != nil {
	return err // leave the item unacknowledged so it can replay
}
if err := r.Ack(offset); err != nil {
	return err
}
```

`Ack` retires one record and is safe to call in any order, so several workers can
use this pattern against the same queue. `Commit(offset)` retires the offset *and
everything before it*, which is the cheaper way to retire a batch one goroutine
reserved itself — but with several workers it retires whatever the others are
still holding, so reach for it only when a single consumer owns the cursor.

Make handlers idempotent: a crash after processing and before a durable
acknowledgement can replay the same item. An unacknowledged reservation has no
automatic retry timer. Stop all workers before using `w.Rewind()` to return
in-flight work to the shared cursor, or close and reopen the queue. `Requeue`
acts on the unread head, not on an item already returned by `Reserve`.

### Iterating (consuming)

Both iterators **consume**: each item is committed as it is read (before your
loop body runs), exactly like `Take`, drawing from the same cursor as
`Reserve`/`Take`, so an item is never delivered twice.

```go
r := w.NewReader()

// Drain: drains the items present right now, oldest to newest, then ends.
for v := range r.Drain(ctx) {
	process(v) // already committed before this runs
}
if err := r.Err(); err != nil {
	return err
}

// Follow: drains existing items, then blocks and yields new ones as they
// arrive, committing each as it is read, until ctx is cancelled.
for v := range r.Follow(ctx) {
	process(v)
}
if err := r.Err(); err != nil {
	return err
}
```

Because the commit happens at read time, an item that fails (or a loop that
stops early via `break` or ctx cancellation) is **not** replayed — `Drain`/`Follow`
are at-most-once, like `Take`. If you need to acknowledge only after successful
processing (at-least-once), use `Reserve`/`Ack`.

## API

`Queue[T]` — produce and manage the log:

| Method | Description |
| --- | --- |
| `New[T](path, marshal, unmarshal, ...Options)` | Open/create a Queue at `path` (segment cap via `Options.MaxSegments`, default 32). |
| `Add(v T) error` | Append an item. Returns `ErrFull` at `MaxSegments` or `MaxBytes` (transient), `ErrRecordTooLarge` if it can never fit under `MaxBytes` (permanent). A record too big for one segment gets a segment of its own. Serialization runs before the lock, and under the default policy concurrent `Add`s share their fsyncs (group commit) while each record stays individually durable. |
| `AddSized(v T) (int64, error)` | `Add`, also reporting the framed size the record occupies on disk — the unit `MaxBytes` and the byte gauges count (`Reader.LastBytes` is the consuming side of the same number). |
| `AddWait(ctx, v T) error` | `Add` with backpressure: blocks while the queue is full, until a commit frees capacity or `ctx` is done. `ErrRecordTooLarge` still returns immediately. |
| `AddBatch(items []T) (int, error)` | Append items in order with header writes and syncs shared across each published span. Returns how many leading items were published. Durability follows the selected policy; a final header-sync failure returns `ErrIO` with the published count and uncertain durability. |
| `NewReader() *Reader[T]` | Create a Reader to consume items (one per consuming goroutine). |
| `Empty() bool` | Whether anything is available to read. |
| `Count() int` | Number of items added but not yet committed. |
| `Size() int64` | Bytes of uncommitted records (roughly what's retained on disk). |
| `Sync() error` | `fsync` the files to stable storage. The per-file fdatasyncs run off the queue's lock (the files pinned against eviction/reclamation instead), so a large flush — the `SyncInterval` backstop included — does not stall concurrent producers and consumers; records written during the flush stay in `UnsyncedBytes` for the next one. Concurrent Syncs serialize. |
| `Stats() Stats` | Gauges and lifetime counters, including every loss path. `UnsyncedBytes` is what a power loss would cost right now; `BacklogBytes`/`MaxBytes` is the utilisation to alert on. See **Recovery**. |
| `Rewind() (int64, error)` | Return every delivered-but-uncommitted record to the queue. The nack for `Reserve`. Watch `Stats().InFlightBytes` to know when it is needed. |
| `Err() error` | The latched durability failure, or nil. See **Failure handling**. |
| `Close() error` | Flush and close (releases the directory lock). |

`Reader[T]` — consume items (created via `NewReader`):

| Method | Description |
| --- | --- |
| `TryPeek() (T, bool, error)` | Look at the front item without consuming it: no cursor moves, and the next read (by any Reader) returns the same item. A damaged head previews as `ErrCorrupt` with nothing dropped or counted. |
| `TryReserve() (T, bool, int64, error)` | Non-blocking read; returns the item and its offset. |
| `LastBytes() int64` | Framed on-disk size of the record the last successful consuming read returned — the unit `MaxBytes` and the byte gauges count (`AddSized` is the producing side of the same number; the `StampRecords` stamp, when enabled, is part of it). |
| `LastAge() time.Duration` | How long the record the last successful consuming read returned had waited in the queue — measured from the stamp `Options.StampRecords` lays down, so it survives a reopen and a `Requeue` keeps it accumulating. Defined as 0 when `StampRecords` is off. |
| `TryTake() (T, bool, error)` | Non-blocking read + commit. |
| `Reserve(ctx) (T, bool, int64, error)` | Block until an item is available, then read it. |
| `Take(ctx) (T, bool, error)` | Block until an item is available, then read + commit. |
| `Ack(offset int64) error` | Mark the **single** entry at `offset` consumed. Safe to call in any order, so this is the acknowledgement for several cooperating workers. The retire reaches disk only across a contiguous run of acknowledged records, so an unacknowledged record delays (never loses) the reclamation of everything behind it. |
| `AckBatch(offsets ...int64) error` | `Ack` for a batch: one lock acquisition and at most one commit for the whole run. Per offset it keeps `Ack`'s contract (idempotent; unknown offsets report `ErrInvalidOffset` after the valid ones are still acknowledged). |
| `Commit(offset int64) error` | Mark the entry at `offset` **and all before it** consumed; reclaim space. Cheap for retiring a batch one goroutine reserved itself — but with several workers it retires their in-flight records too, so prefer `Ack` there. |
| `Skip() (bool, error)` | Consume and retire the head record without decoding it — discards it. The retire is per-record (through the reservation ledger), so it never takes a competing worker's reserved record with it; behind an outstanding reservation it becomes durable once that reservation acknowledges. |
| `Requeue() (bool, error)` | Move the head record to the **back** without decoding it. The non-destructive way past a poison record: it costs a reordering instead of data loss or a stalled head. |
| `Drain(ctx) iter.Seq[T]` | Drain items present at call time (commits each). |
| `Follow(ctx) iter.Seq[T]` | Drain existing then future items until `ctx` is cancelled (commits each). |
| `Err() error` | Why the last `Drain`/`Follow` stopped; nil if it simply ran out. |

## Semantics

- **Offsets.** `Reserve`/`TryReserve` return a monotonically increasing offset
  within a session. Pass it to `Ack` to acknowledge that record, or to `Commit`
  to acknowledge that record and everything before it. Do not persist offsets
  across a reopen or keep using old reservations after `Rewind`. `Take` commits
  implicitly.
- **`Options.MaxSegments` bounds the number of segment files** kept on disk at
  once, so the footprint is about `MaxSegments × SegmentSize`. When the active
  segment fills and that many segments are already live, `Add` returns `ErrFull`
  until a whole segment is committed and reclaimed. A record too large for the
  geometry is not refused — it gets a segment sized to itself; `ErrRecordTooLarge`
  is reserved for a record that exceeds `Options.MaxBytes`, which no draining can
  ever admit. The default (a zero value) is 32; a negative value means unbounded.
- **At-least-once.** The read cursor is reset to the persisted commit cursor on
  open, so after a restart any items added but not committed are replayed. Use
  `Reserve`/`Ack` for explicit acknowledgement.
- **Durability.** Writes go into the page cache via `pwrite` and survive a
  process crash. By default each write and commit is `fsync`'d for
  power-loss durability; set `NoSync` to skip the per-operation fsync, or
  `SyncEvery: N` to batch one fsync over N operations (optionally with a
  `SyncInterval` wall-clock backstop). `Sync` flushes on demand and `Close`
  always flushes — including under `NoSync`, which means "no fsync per
  operation", not "never". Segment creation and removal are additionally made
  durable with a directory `fsync`; that is a POSIX notion, so it is skipped on
  Windows, plan9 and js/wasm, and on Unix filesystems that do not implement it —
  a power loss there can lose a freshly cycled segment's directory entry.
- **Integrity.** Every record and every file header stores an `xxhash64`, verified
  on read and on open. A segment that has *lost* bytes is corruption too, not a
  geometry mismatch: since segments are preallocated to a known length, a file of
  the wrong size is read as `ErrSegmentSizeMismatch` only when its own header
  agrees it is complete. See **Recovery** for what a failed check costs.
- **Reclamation.** Disk is freed a whole file at a time, once every record in it
  is committed. It happens on the write that cycles to a new file *and* at the end
  of a commit, so a consumer draining a backlog with the producer stopped still
  frees disk — you do not need another `Add` to get the space back. A file that
  will not unlink stays counted (`Stats().Unreclaimed`), so `MaxSegments` keeps
  describing what is really on disk, and the next reclamation retries it.

## Recovery

Recovery is not an option to switch on — it is the behaviour. Two rules run
through all of it:

**Corruption degrades to reported loss, never to corrupt output and never to a
wedged queue.** Damaged data is dropped and counted, not handed onward as
plausible-looking records, and there is no failure mode where the consumer waits
forever on bytes that will never parse. A queue that answers `ErrCorrupt` to
every read until someone edits the directory by hand is not more careful than one
that drops the record — it is just unavailable as well as damaged.

**Every loss path is observable.** Each event surfaces as exactly one
`ErrCorrupt` from a read, and `Stats()` carries the magnitude the error cannot:
`LostRecords`, `LostSegments`, `LostBytes` (a lower bound), `DiscardedBytes` and
`ForeignSegments`. `increase(LostBytes) > 0` means corruption destroyed data.

| Damage | Cost |
| --- | --- |
| Crash mid-`Add` (torn record at the tail) | nothing: the header never published it, and the `Add` never returned |
| Crash between `Add` and the header fsync | the record is invisible on reopen — a clean truncation, not an error |
| One flipped byte in a record **payload** | **one record**: its length still framed it, so exactly that record is dropped |
| One flipped byte in a record **length** | **up to one segment**: the frame boundaries behind it are gone with it |
| Header rewrite torn by a power cut (magic/version intact, checksum bad) | the segment is **rebuilt** at open from a checksum-verified walk of its records — nothing verified is lost; the commit position is the one casualty, so that segment and everything after it replays, and one event is reported |
| Damaged segment header (magic gone, or a header shorter than 64 bytes) | that segment is dropped at open — wherever it sits — and counted; the rest stay readable |
| Segment truncated by the filesystem | the records still present survive; the cut tail is counted in `DiscardedBytes` |
| Segment file deleted underneath a running queue | the segment is abandoned, one `ErrCorrupt`, the queue keeps making progress |
| Unfinished segment create (zero-length, or reserved and still all zeros) | removed silently — it never held a record, so it is not a loss event |
| Unknown format version | segment dropped silently, counted in `ForeignSegments` — a format change is not data loss |
| Segment that cannot be *read* (`EACCES`, `EMFILE`, `EIO`) | **not** damage: the open fails and nothing is deleted, because "I could not look" is not evidence |

"One bad byte costs one record" is true of payloads, not of length prefixes. A
damaged length is always *detected* — the checksum it points at no longer
matches — but it cannot be *repaired*: damaged upward it overruns the segment,
damaged downward it desynchronizes the walk into the middle of a payload, and
either way the record boundaries from that point on are gone. The rest of that
segment is abandoned, so one bad byte there can cost up to `SegmentSize`.

Reading is where losses are reported, so a consumer that never reads never learns
of them; `Stats()` is readable at any time, including after
`Close`.

Damage is retired through the acknowledgement ledger when earlier reservations
are outstanding, including reservations in the damaged segment itself. An `Ack`
that closes the gap retires the reported damage without reporting it again and
without retiring records appended afterward.

Recovery must flush corrected cursors and counts before opening succeeds, even
under `NoSync`. A required repair that cannot be written fails the open so it can
be retried. For a torn header, later commit headers are reset before the salvaged
header is replaced; an interrupted repair therefore preserves the replay decision even
when no records survived in the salvaged segment.

```go
for {
    v, ok, err := r.TryTake()
    switch {
    case errors.Is(err, diskqueue.ErrCodec):
        // A decoder can wrap ErrCorrupt too; this record has NOT been dropped.
        return err
    case errors.Is(err, diskqueue.ErrCorrupt):
        // Damage was dropped and the queue advanced. Count it and go round again.
        slog.Warn("diskqueue read", "err", err, "lost", w.Stats().LostBytes)
    case err != nil:
        return err
    case !ok:
        return nil
    default:
        process(v)
    }
}
```

`Drain` and `Follow` cannot carry an error per item, so they do not stop on
corruption — the damage is already dropped and the queue has moved on, and
stopping would strand every record behind it. They keep iterating and record the
first event in `Reader.Err()`.

## Failure handling

An I/O error never panics and never wedges the queue — it comes back as an error,
and the queue is left in a state you can reason about.

- **A write that fails did not happen.** `Add` returning `ErrFull`,
  `ErrRecordTooLarge` or a failed `pwrite` leaves the queue byte-for-byte as it
  was: the item is not in it, and retrying is well defined. A segment that cannot
  be created is unlinked again, so a failed `Add` leaves no stub behind.
  A partial header overwrite is restored to the preceding published image; if
  that restoration fails, the queue latches `ErrIO` and placement is uncertain.
- **A failed `fsync` poisons the queue.** Linux reports a writeback error *once*
  and then discards the dirty pages, so a second `fsync` can return success over
  data that is already gone. Rather than claim a durability it cannot deliver, the
  queue latches the first such failure: `Add`, `Commit` and `Sync` all return an
  error wrapping `ErrIO` from then on (the original errno is wrapped too, so
  `errors.Is(err, syscall.ENOSPC)` still works), and `Queue.Err()` reports it
  without performing an operation. **Reads keep working**, so a consumer can drain
  what is there; to write again, `Close` and reopen — whatever was durable is
  still on disk and uncommitted records replay.
- **Commits report failure.** `Take`/`TryTake` return the item *and* a non-nil
  error when the read succeeded but its commit did not reach disk: the item is
  yours to process and may replay after a reopen. A failed header write leaves
  the affected segment's retirement unchanged and can be retried; a failed final
  sync leaves durability uncertain. `Commit` returns the same error directly.
- **Iterators report through `Err()`.** An `iter.Seq` cannot carry an error, so
  `Drain`/`Follow` stop and record it: check `r.Err()` after the loop. Nil means
  the iteration ended because the queue was drained, the context was cancelled, or
  the queue was closed.
- **A record that could not be delivered stays put.** If `UnmarshalFunc` returns
  an error the read cursor is rewound and the error wraps `ErrCodec`, so a
  transient decode failure cannot silently consume an item. A record the codec
  will *never* accept is therefore offered again on every read — call `Skip` to
  step over it deliberately. Check `ErrCodec` before `ErrCorrupt`: the codec's
  own error can wrap a library sentinel without indicating actual disk damage.
- **`Commit` is bounded by the read cursor.** An offset past what has been read
  returns `ErrInvalidOffset` instead of reclaiming (and deleting) records nobody
  has seen.
- **Recovery never destroys what it could not read.** A segment is dropped only
  when it is *readable and damaged*. One that merely could not be opened —
  `EACCES`, `EMFILE`, a device error — fails the open instead, so a transient
  permission blip cannot unlink good data. A segment that has *disappeared* is
  corruption, and is abandoned like any other.
- **One writer per directory.** `New` takes an advisory `flock` on the queue
  directory (Unix; a no-op where the standard library has no `flock`). A second
  open of the same directory fails with `ErrLocked`.
- **Value lifetime.** A `Reader` copies each record into its own reusable scratch
  buffer before calling `UnmarshalFunc`, so the value (and anything in `T` that
  aliases it) never aliases the store's reused read buffer. It is valid until
  the next read on that same `Reader`; copy it inside `UnmarshalFunc` if you need
  it to outlive that. The copy reuses the buffer, so it allocates nothing once
  warm.
- **Concurrency.** A `Queue` is safe for concurrent use. A single `Reader` is *not*
  — use one `Reader` per consuming goroutine. Multiple Readers share the one
  read/commit cursor and cooperate to consume the stream (each item delivered
  once). `Take`/`TryTake` and the `Drain`/`Follow` iterators read and commit
  atomically under the lock, in cursor order, so they are safe for *concurrent*
  cooperating readers. `Reserve` is the only deferred path, and it has two
  acknowledgements. `Commit` advances the shared prefix cursor *after* an unlocked
  processing window, retiring the offset **and everything before it** — cheap for a
  batch retire, but with several workers one consumer committing out of order
  reclaims another's in-flight record, so use it from a single consumer. `Ack`
  retires **one** record and is safe in any order, which is what competing workers
  want; it reaches disk only across a contiguous run of acknowledged records, so a
  slow worker delays (never loses) the retire of everything behind it. The blocking
  `Reserve`/`Take` and the `Follow` iterator wait
  for new data and honour their context; `Drain`/`Follow` release the lock between
  yields, so other methods may be called from inside the iteration.

## Options

```go
diskqueue.New[T](path, marshal, unmarshal, diskqueue.Options{
	MaxSegments:  0,    // 0 = 32 default; N>0 = cap live files (ErrFull); <0 = unbounded
	MaxBytes:     0,    // 0 = no byte cap; N>0 = cap the uncommitted backlog in bytes (ErrFull)
	NoSync:       false, // default: sync each write/commit for power-loss durability
	SyncEvery:    0,    // 0/1 = fsync every op; N>1 = batch the fsync over N ops
	SyncInterval: 0,    // >0 = background flush every interval (backstop for SyncEvery)
	SegmentSize:  0,    // 0 = 8 MiB default; floored and rounded up to 4 KiB
	MaxOpenFiles: 0,    // 0 = keep every touched segment open; N = cap open files (LRU), min 3
	StampRecords: false, // true = stamp each record's enqueue time into its payload; read back as Reader.LastAge
})
```

The two caps compose and answer different questions. `MaxSegments` bounds the
**file count**, so the footprint that follows from it is `MaxSegments ×
SegmentSize` — a ceiling on disk rather than a budget on the backlog, and a queue
holding one record per segment can be nowhere near its byte cap while at its
segment cap. `MaxBytes` bounds the **uncommitted backlog in bytes**, which is what
an outage budget is actually sized in; `Stats().BacklogBytes / Stats().MaxBytes` is
the utilisation ratio to alert on. Whichever binds first returns `ErrFull`, and the
queue is left untouched, so the caller chooses: block, drop, or shed load upstream.

Volume sizing under a `MaxBytes` budget (`MaxSegments` unbounded) has an exact
bound, not just "headroom": segments are preallocated whole and reclaimed whole,
so on a healthy queue

```
Stats().DiskBytes  ≤  MaxBytes + 2 × SegmentSize + 64 bytes per segment
```

— up to one segment of already-committed records not yet reclaimable (a file
unlinks only once **every** record in it is committed) and up to one segment of
preallocated slack in the active file, plus each segment's 64-byte header. At the
default 8 MiB geometry a 512 MiB budget measures 520 MiB on disk (1.6 %
overhead). Two prints against the bound: `Stats().Unreclaimed` climbing means
files that will not unlink are accumulating outside it, and a consumer that
reserves without acknowledging pins segments inside `MaxBytes` without freeing
them. Reopening a backlog of any depth is not a sizing concern either way:
recovery reads one 64-byte header per segment and no records — sub-millisecond
for a 512 MiB / 65-segment backlog.

A record larger than `MaxBytes` can never be accepted on an empty queue either, so
it is refused with `ErrRecordTooLarge` — permanent, where `ErrFull` is transient
and clears as the consumer drains.

`SyncEvery` trades a bounded power-loss window for throughput: with `SyncEvery: N`
the fsync cost is amortized over N writes/commits, so up to the last N unsynced
operations can be lost on power loss (they still survive a process crash, and a
torn tail is caught by the per-record checksum). `Close` and `Sync` always flush,
including segment directory entries on supported platforms, even under `NoSync`.
`Stats().UnsyncedBytes` reports that window as a number — zero under the default
per-op policy, climbing under `NoSync` or `SyncEvery > 1` until a flush. If it
keeps climbing, the `SyncInterval` backstop is not keeping up.
Measure the tradeoff on your deployment filesystem using the
[benchmark commands](../README.md#performance). Throughput depends heavily on
the storage device, filesystem, sync policy, payload, and producer concurrency.

`SegmentSize` is fixed when the store is created, floored at 4 KiB and rounded up
to a multiple of 4 KiB (a fixed constant, not the host's page size — otherwise a
store created on a 4 KiB-page machine would not reopen on a 64 KiB-page one).
Reopening an existing store with a different (post-rounding) `SegmentSize` is
rejected with `ErrSegmentSizeMismatch` rather than truncating the files and
discarding committed records.

It is not, however, a ceiling on record size. Records never span segments — that is
the invariant — but a record too large for the configured geometry gets a segment
sized to exactly itself, marked as such in that segment's header so a reopen can
tell it apart from a store built at a different `SegmentSize`. Such a segment holds
that one record and nothing else, and `Stats().DiskBytes` reports the real
footprint rather than `segments × SegmentSize`.

`StampRecords` moves the "when was this enqueued" envelope into the store: each
record is serialized with an 8-byte big-endian unix-nano stamp prepended
**inside** its payload — checksummed, framed and counted like any other payload
byte, so `AddSized`, `Reader.LastBytes`, `MaxBytes` and the byte gauges all keep
agreeing on one unit. The `Reader` strips the stamp back off before the payload
reaches `UnmarshalFunc` and reports the wait as `Reader.LastAge`; `Requeue`
moves the raw payload, stamp included, so a rotated record's age keeps
accumulating instead of resetting. The stamp changes what records *contain*,
not the on-disk format, so consistency across reopens is the caller's contract
exactly as the codec already is: reopen a store with the same `StampRecords` it
was written with. Flipping it over an existing backlog mis-frames every record
— codec-level garbage, surfaced as `ErrCodec` (a stamped record shorter than
its stamp included, never a panic) with `Skip` the way past each one.

## Storage layout


The log lives in a directory of numbered files `data.00000001`, `data.00000002`,
… each `SegmentSize` bytes, at most `MaxSegments` of them at once. Segments are
**preallocated** — `fallocate(2)` on Linux, `ftruncate` elsewhere — so a full
filesystem is reported when a segment is created, while the store is still clean,
instead of arriving as an `ENOSPC` in the middle of a record; the footprint is
what `MaxSegments × SegmentSize` says it is, plus any oversized records' segments
(`Stats().DiskBytes` reports the real number). That guarantee needs real
`fallocate`: on non-Linux platforms, and on Linux over NFS, 9p, some FUSE and
overlay mounts, it falls back to `ftruncate`, which reserves nothing — there
`ENOSPC` still arrives during an `Add` (which then leaves the queue unchanged),
and `Stats().DiskBytes` reports apparent size over a sparse file. The directory itself is held open
for the lifetime of the queue and carries an advisory `flock`, so a second
`New` on the same directory fails with `ErrLocked` rather than silently
interleaving writes into the same segments — on platforms whose standard library
has flock; on Windows, Solaris, AIX, plan9 and js the lock is a no-op and
nothing prevents two processes from opening the same directory. Each file
begins with a 64-byte header — a magic number, a format version, the commit
cursor (persisted read position), write cursor (data end), written and committed
record counts, the segment's own capacity, the `SegmentSize` the store was created
with (these two are the authority on geometry; the file's length is evidence
measured against them rather than evidence on its own), and an
`xxhash64` over the header itself — followed by records,
each `uvarint(len) || payload || xxhash64(payload)` (an 8-byte little-endian
checksum trailer). Because the cursors and counts all live in the header,
reopening reads no records at all; the header checksum is verified on open (a
segment that fails it is dropped and counted — see **Recovery**), and each
record's checksum is verified on read. Records are written with `pwrite` and read back with
`pread` into reused buffers; each file's handle is opened on demand and the
least-recently-used handles are closed once `MaxOpenFiles` are open, so a deep
backlog does not hold an open descriptor per retained file. A file is dropped once
all its records are committed: on the next write that cycles to a new file, and
also at the end of a commit — so a consume-only or producer-stopped workload
frees disk without waiting for another write.
