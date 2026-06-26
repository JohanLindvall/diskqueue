# wal

A generic, durable, FIFO **write-ahead log** (a persistent queue) for Go, backed
by its own small memory-mapped file store (dependencies: `golang.org/x/sys` for
mmap/msync and `cespare/xxhash/v2` for per-record checksums).

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
  value is never backed by a file that may be unmapped.

```
BenchmarkAddTake-22    36 ns/op    0 B/op    0 allocs/op   (NoSync)
```

## Storage layout

The log lives in a directory of numbered files `data.00000001`, `data.00000002`,
… each `SegmentSize` bytes, at most `maxSegments` of them at once. Each file
begins with a 64-byte header — a magic number, a format version, the commit
cursor (persisted read position), write cursor (data end), written and committed
record counts, and an `xxhash64` over the header itself — followed by records,
each `uvarint(len) || payload || xxhash64(payload)` (an 8-byte little-endian
checksum trailer). Because the cursors and counts all live in the header,
reopening reads no records at all; the header checksum is verified on open (a
mismatch is `ErrCorrupt`, a wrong magic/version is `ErrBadFormat`), and each
record's checksum is verified on read (a mismatch returns `ErrCorrupt` without
advancing the cursor). Segments are memory-mapped on demand and the
least-recently-used are unmapped once `MaxMapped` are live, so a deep backlog does
not hold an mmap and fd per retained file. A file is dropped once all its records
are committed — but only while writing (a new write cycling to the next file);
reads and commits never delete files.

## Install

```sh
go get github.com/JohanLindvall/wal
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

	"github.com/JohanLindvall/wal"
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
	// Keep at most 8 segment files on disk.
	w, err := wal.New[uint64]("/tmp/myqueue", 8, marshal, unmarshal)
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

`WAL[T]` — produce and manage the log:

| Method | Description |
| --- | --- |
| `New[T](path, maxSegments, marshal, unmarshal, ...Options)` | Open/create a WAL at `path`. |
| `Add(v T) error` | Append an item. Returns `ErrFull` at `maxSegments`, `ErrRecordTooLarge` if it can't fit a segment. |
| `NewReader() *Reader[T]` | Create a Reader to consume items (one per consuming goroutine). |
| `Empty() bool` | Whether anything is available to read. |
| `Count() int` | Number of items added but not yet committed. |
| `Size() int64` | Bytes of uncommitted records (roughly what's retained on disk). |
| `Sync() error` | `msync` the files to stable storage. |
| `Close() error` | Flush and close. |

`Reader[T]` — consume items (created via `NewReader`):

| Method | Description |
| --- | --- |
| `TryReserve() (T, bool, int64, error)` | Non-blocking read; returns the item and its offset. |
| `TryTake() (T, bool, error)` | Non-blocking read + commit. |
| `Reserve(ctx) (T, bool, int64, error)` | Block until an item is available, then read it. |
| `Take(ctx) (T, bool, error)` | Block until an item is available, then read + commit. |
| `Commit(offset int64) error` | Mark the entry at `offset` and all before it consumed; reclaim space. |
| `Drain(ctx) iter.Seq[T]` | Drain items present at call time (commits each). |
| `Follow(ctx) iter.Seq[T]` | Drain existing then future items until `ctx` is cancelled (commits each). |

## Semantics

- **Offsets.** `Reserve`/`TryReserve` return a monotonically increasing offset.
  Pass it to `Commit` to acknowledge that record and everything before it. `Take`
  commits implicitly.
- **`maxSegments` bounds the number of segment files** kept on disk at once, so
  the footprint is about `maxSegments × SegmentSize`. When the active segment
  fills and that many segments are already live, `Add` returns `ErrFull` until a
  whole segment is committed and reclaimed. A record (length prefix plus payload)
  too large to fit one segment is rejected with `ErrRecordTooLarge`. `maxSegments
  <= 0` means unbounded.
- **At-least-once.** The read cursor is reset to the persisted commit cursor on
  open, so after a restart any items added but not committed are replayed. Use
  `Reserve`/`Commit` for explicit acknowledgement.
- **Durability.** Writes go into the memory-mapped files and survive a process
  crash via the page cache. By default each write and commit is `msync`'d for
  power-loss durability; set `NoSync` to skip that entirely, or `SyncEvery: N` to
  batch one fsync over N operations (optionally with a `SyncInterval` wall-clock
  backstop). `Sync` flushes on demand.
- **Integrity.** Every record and every file header stores an `xxhash64`, verified
  on read and on open. A record mismatch (bit-rot or a torn write) returns
  `ErrCorrupt` and leaves the read cursor in place, so the bad record surfaces
  rather than being delivered as valid data; a bad header fails the open with
  `ErrCorrupt`, and a foreign/garbage file fails with `ErrBadFormat`.
- **Recovery.** With `RecoverCorrupt` set, corruption is recovered instead of
  surfaced: a torn trailing segment (a crash mid-cycle) is dropped on open, and a
  corrupt record quarantines the rest of its segment and the reader continues with
  the next valid record. Recovery is lossy — dropped data is gone — so each event
  is counted by `WAL.Corruptions()`. Without the flag the default is strict.
- **Reclamation.** Disk is freed a whole file at a time, once every record in a
  file is committed, and only while writing. A read-only consumer never deletes
  files; reclamation happens on the next `Add` that cycles to a new file.
- **Value lifetime.** A `Reader` copies each record into its own reusable scratch
  buffer before calling `UnmarshalFunc`, so the value (and anything in `T` that
  aliases it) is never backed by a file that may be unmapped. It is valid until
  the next read on that same `Reader`; copy it inside `UnmarshalFunc` if you need
  it to outlive that. The copy reuses the buffer, so it allocates nothing once
  warm.
- **Concurrency.** A `WAL` is safe for concurrent use. A single `Reader` is *not*
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
wal.New[T](path, maxSegments, marshal, unmarshal, wal.Options{
	NoSync:         true, // skip msync per write/commit (faster, no power-loss durability)
	SyncEvery:      0,    // 0/1 = msync every op; N>1 = batch the fsync over N ops
	SyncInterval:   0,    // >0 = background flush every interval (backstop for SyncEvery)
	SegmentSize:    0,    // 0 = 8 MiB default; floored at 4 KiB, rounded up to a page
	MaxMapped:      0,    // 0 = map every touched segment; N = cap mmaps (LRU), min 2
	RecoverCorrupt: false, // true = drop corrupt data and continue instead of erroring
})
```

`SyncEvery` trades a bounded power-loss window for throughput: with `SyncEvery: N`
the fsync cost is amortized over N writes/commits, so up to the last N unsynced
operations can be lost on power loss (they still survive a process crash, and a
torn tail is caught by the per-record checksum). `Close` and `Sync` always flush.
The speed-up is large — on a laptop NVMe, durable `Add` goes from ~1.6 ms/op at
`SyncEvery: 1` to ~9 µs at `SyncEvery: 100` and ~1.1 µs at `SyncEvery: 1000`.

`SegmentSize` is fixed when the store is created. Reopening an existing store with
a different (post-rounding) `SegmentSize` is rejected with `ErrSegmentSizeMismatch`
rather than truncating the files and discarding committed records.

## License

MIT
