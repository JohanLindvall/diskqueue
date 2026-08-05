# diskqueue

A generic, durable, FIFO **disk-backed queue** for Go — a persistent work queue
that doubles as a write-ahead log — backed by its own small file store using
plain `pread`/`pwrite`/`fsync` (its only dependency is `cespare/xxhash/v2` for
per-record checksums).

Items are appended at the back with `Add` and consumed from the front through a
`Reader`: a consumer either **takes** an item (read + commit in one step) or
**reserves** an item together with its offset and later **commits** that offset.
Committing advances a persisted read cursor; whole data files are reclaimed once
every record in them has been committed.

The package is designed for high throughput and minimal per-operation heap
allocation:

- Serialization reuses an internal scratch buffer, so `Add` performs **no heap
  allocation** once warm (given a `MarshalFunc` that appends into the supplied
  buffer).
- A `Reader` copies each record once into its own reusable scratch buffer before
  handing it to `UnmarshalFunc` — **no per-read allocation** once warm, and the
  value never aliases the store's reused read buffer.

```
BenchmarkAddTake-16    4.3 µs/op    0 B/op    0 allocs/op   (NoSync; add + take + commit)
```

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
record counts, and an `xxhash64` over the header itself — followed by records,
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

## Install

```sh
go get github.com/JohanLindvall/diskqueue
```

Requires Go 1.23+ (uses `iter.Seq`).

## Usage

```go
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/JohanLindvall/diskqueue"
)

// A zero-allocation codec: marshal appends into the provided buffer, unmarshal
// reads directly from the (zero-copy) slice.
func marshal(dst []byte, v uint64) ([]byte, error) {
	return binary.LittleEndian.AppendUint64(dst, v), nil
}

func unmarshal(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.New("bad length")
	}
	return binary.LittleEndian.Uint64(data), nil
}

func main() {
	// Keep at most 8 segment files on disk (default is 32).
	w, err := diskqueue.New[uint64]("/tmp/myqueue", marshal, unmarshal, diskqueue.Options{MaxSegments: 8})
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// Producer.
	for i := uint64(0); i < 10; i++ {
		if err := w.Add(i); err != nil {
			panic(err)
		}
	}

	// Consumer: reads go through a Reader (one per consuming goroutine).
	r := w.NewReader()
	for {
		v, ok, err := r.TryTake() // read + commit in one step
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Println(v)
	}

	// Or block until an item arrives.
	ctx := context.Background()
	v, ok, err := r.Take(ctx)
	_ = v
	_ = ok
	_ = err
}
```

### Reserve / Commit (at-least-once with explicit acknowledgement)

When you need to process an item before acknowledging it, use `Reserve`/`Commit`
instead of `Take`:

```go
r := w.NewReader()
v, ok, offset, err := r.Reserve(ctx)
if ok {
	if err := process(v); err == nil {
		r.Commit(offset) // acknowledge; everything up to offset is reclaimed
	}
	// If you don't commit, the item replays after a restart.
}
```

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

// Follow: drains existing items, then blocks and yields new ones as they
// arrive, committing each as it is read, until ctx is cancelled.
for v := range r.Follow(ctx) {
	process(v)
}
```

Because the commit happens at read time, an item that fails (or a loop that
stops early via `break` or ctx cancellation) is **not** replayed — `Drain`/`Follow`
are at-most-once, like `Take`. If you need to acknowledge only after successful
processing (at-least-once), use `Reserve`/`Commit`.

## API

`Queue[T]` — produce and manage the log:

| Method | Description |
| --- | --- |
| `New[T](path, marshal, unmarshal, ...Options)` | Open/create a Queue at `path` (segment cap via `Options.MaxSegments`, default 32). |
| `Add(v T) error` | Append an item. Returns `ErrFull` at `MaxSegments` or `MaxBytes` (transient), `ErrRecordTooLarge` if it can never fit under `MaxBytes` (permanent). A record too big for one segment gets a segment of its own. |
| `NewReader() *Reader[T]` | Create a Reader to consume items (one per consuming goroutine). |
| `Empty() bool` | Whether anything is available to read. |
| `Count() int` | Number of items added but not yet committed. |
| `Size() int64` | Bytes of uncommitted records (roughly what's retained on disk). |
| `Sync() error` | `fsync` the files to stable storage. |
| `Stats() Stats` | Gauges and lifetime counters, including every loss path. `UnsyncedBytes` is what a power loss would cost right now; `BacklogBytes`/`MaxBytes` is the utilisation to alert on. See **Recovery**. |
| `Rewind() (int64, error)` | Return every delivered-but-uncommitted record to the queue. The nack for `Reserve`. Watch `Stats().InFlightBytes` to know when it is needed. |
| `Err() error` | The latched durability failure, or nil. See **Failure handling**. |
| `Close() error` | Flush and close (releases the directory lock). |

`Reader[T]` — consume items (created via `NewReader`):

| Method | Description |
| --- | --- |
| `TryReserve() (T, bool, int64, error)` | Non-blocking read; returns the item and its offset. |
| `TryTake() (T, bool, error)` | Non-blocking read + commit. |
| `Reserve(ctx) (T, bool, int64, error)` | Block until an item is available, then read it. |
| `Take(ctx) (T, bool, error)` | Block until an item is available, then read + commit. |
| `Commit(offset int64) error` | Mark the entry at `offset` and all before it consumed; reclaim space. |
| `Skip() (bool, error)` | Consume and commit the head record without decoding it — discards it. |
| `Requeue() (bool, error)` | Move the head record to the **back** without decoding it. The non-destructive way past a poison record: it costs a reordering instead of data loss or a stalled head. |
| `Drain(ctx) iter.Seq[T]` | Drain items present at call time (commits each). |
| `Follow(ctx) iter.Seq[T]` | Drain existing then future items until `ctx` is cancelled (commits each). |
| `Err() error` | Why the last `Drain`/`Follow` stopped; nil if it simply ran out. |

## Semantics

- **Offsets.** `Reserve`/`TryReserve` return a monotonically increasing offset.
  Pass it to `Commit` to acknowledge that record and everything before it. `Take`
  commits implicitly.
- **`Options.MaxSegments` bounds the number of segment files** kept on disk at
  once, so the footprint is about `MaxSegments × SegmentSize`. When the active
  segment fills and that many segments are already live, `Add` returns `ErrFull`
  until a whole segment is committed and reclaimed. A record too large for the
  geometry is not refused — it gets a segment sized to itself; `ErrRecordTooLarge`
  is reserved for a record that exceeds `Options.MaxBytes`, which no draining can
  ever admit. The default (a zero value) is 32; a negative value means unbounded.
- **At-least-once.** The read cursor is reset to the persisted commit cursor on
  open, so after a restart any items added but not committed are replayed. Use
  `Reserve`/`Commit` for explicit acknowledgement.
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
| Damaged segment header | that segment is dropped at open — wherever it sits — and counted; the rest stay readable |
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

```go
for {
    v, ok, err := r.TryTake()
    switch {
    case errors.Is(err, ErrCorrupt):
        // Damage was dropped and the queue advanced. Count it and go round again.
        log.Warn("diskqueue read", "err", err, "lost", w.Stats().LostBytes)
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
  yours to process, and it will be delivered again after a reopen. `Commit`
  returns the same error directly.
- **Iterators report through `Err()`.** An `iter.Seq` cannot carry an error, so
  `Drain`/`Follow` stop and record it: check `r.Err()` after the loop. Nil means
  the iteration ended because the queue was drained, the context was cancelled, or
  the queue was closed.
- **A record that could not be delivered stays put.** If `UnmarshalFunc` returns
  an error the read cursor is rewound, exactly as for a corrupt record, so a
  transient decode failure cannot silently consume an item. A record the codec
  will *never* accept is therefore offered again on every read — call `Skip` to
  step over it deliberately.
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
  cooperating readers. `Reserve`/`Commit` is the only deferred path: it advances
  the shared prefix cursor *after* an unlocked processing window, so its commits
  must be issued in offset order (use a single consumer, or coordinate the
  commits) — otherwise one consumer committing out of order reclaims another's
  in-flight record. The blocking `Reserve`/`Take` and the `Follow` iterator wait
  for new data and honour their context; `Drain`/`Follow` release the lock between
  yields, so other methods may be called from inside the iteration.

## Options

```go
diskqueue.New[T](path, marshal, unmarshal, diskqueue.Options{
	MaxSegments:  0,    // 0 = 32 default; N>0 = cap live files (ErrFull); <0 = unbounded
	MaxBytes:     0,    // 0 = no byte cap; N>0 = cap the uncommitted backlog in bytes (ErrFull)
	NoSync:       true, // skip fsync per write/commit (faster, no power-loss durability)
	SyncEvery:    0,    // 0/1 = fsync every op; N>1 = batch the fsync over N ops
	SyncInterval: 0,    // >0 = background flush every interval (backstop for SyncEvery)
	SegmentSize:  0,    // 0 = 8 MiB default; floored and rounded up to 4 KiB
	MaxOpenFiles: 0,    // 0 = keep every touched segment open; N = cap open files (LRU), min 3
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

A record larger than `MaxBytes` can never be accepted on an empty queue either, so
it is refused with `ErrRecordTooLarge` — permanent, where `ErrFull` is transient
and clears as the consumer drains.

`SyncEvery` trades a bounded power-loss window for throughput: with `SyncEvery: N`
the fsync cost is amortized over N writes/commits, so up to the last N unsynced
operations can be lost on power loss (they still survive a process crash, and a
torn tail is caught by the per-record checksum). `Close` and `Sync` always flush.
`Stats().UnsyncedBytes` reports that window as a number — zero under the default
per-op policy, climbing under `NoSync` or `SyncEvery > 1` until a flush. If it
keeps climbing, the `SyncInterval` backstop is not keeping up.
The speed-up is large — on a laptop NVMe, durable `Add` goes from ~1.6 ms/op at
`SyncEvery: 1` to ~9 µs at `SyncEvery: 100` and ~1.1 µs at `SyncEvery: 1000`.

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

## License

MIT
