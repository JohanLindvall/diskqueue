# diskqueue

A generic, durable, FIFO **disk-backed queue** for Go — a persistent work queue —
backed by [nsqio/go-diskqueue](https://github.com/nsqio/go-diskqueue), which
provides the `[]byte`-only file backend (numbered data files, a persisted
read/write cursor, and a background goroutine that surfaces records over a
channel). This package adds a generic, typed layer on top plus a `MaxSegments`
backpressure bound.

Items are appended at the back with `Add` and consumed from the front through a
`Reader`: a consumer **takes** an item (`Take`/`TryTake`) or iterates with
`Drain`/`Follow`. A read advances the persisted read cursor as the record is
delivered, and the backend reclaims whole data files once every record in them
has been read.

> **Note on history.** This package previously shipped its own mmap-backed store
> with a separate commit cursor, which gave at-least-once delivery and a
> `Reserve`/`Commit` two-phase acknowledgement API. That store has been replaced
> with go-diskqueue, which has a single read cursor and no deferred ack. As a
> result `Reserve`/`Commit` (and offset-addressed commits) are **gone**, and
> delivery is now **at-most-once** (see [Semantics](#semantics)).

## Storage layout

The log lives in a directory of files named `diskqueue.diskqueue.000001.dat`, …
each up to `SegmentSize` bytes, managed by go-diskqueue. Each record is stored as
a 4-byte length prefix followed by its payload. A separate
`diskqueue.diskqueue.meta.dat` file persists the depth and the read/write
positions. A data file is removed once it has been fully read.

`MaxSegments` is enforced by this package on top of the backend: before each
`Add` it checks the live segment-file count and returns `ErrFull` when the cap is
reached, bounding the footprint to about `MaxSegments × SegmentSize`.

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

	// Consumer: reads go through a Reader.
	r := w.NewReader()
	for {
		v, ok, err := r.TryTake()
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
	_, _, _ = v, ok, err
}
```

### Iterating (consuming)

Both iterators **consume**: each item's read cursor advances as it is delivered
(before your loop body runs), exactly like `Take`, drawing from the same cursor,
so an item is never delivered twice.

```go
r := w.NewReader()

// Drain: drains the items present right now, oldest to newest, then ends.
for v := range r.Drain(ctx) {
	process(v)
}

// Follow: drains existing items, then blocks and yields new ones as they
// arrive, until ctx is cancelled.
for v := range r.Follow(ctx) {
	process(v)
}
```

Because a record is advanced at read time, an item that fails (or a loop that
stops early via `break` or ctx cancellation) is **not** replayed — `Take`,
`Drain`, and `Follow` are all at-most-once.

## API

`DiskQueue[T]` — produce and manage the log:

| Method | Description |
| --- | --- |
| `New[T](path, marshal, unmarshal, ...Options)` | Open/create a DiskQueue at `path` (segment cap via `Options.MaxSegments`, default 32). |
| `Add(v T) error` | Append an item. Returns `ErrFull` at `MaxSegments`, `ErrRecordTooLarge` if it can't fit a segment. |
| `NewReader() *Reader[T]` | Create a Reader to consume items. |
| `Empty() bool` | Whether anything is available to read. |
| `Count() int` | Number of items added but not yet read. |
| `Size() int64` | Physical on-disk footprint of the segment files. |
| `Corruptions() int64` | Count of corruption-recovery events logged by the backend. |
| `Close() error` | Flush and close. |

`Reader[T]` — consume items (created via `NewReader`):

| Method | Description |
| --- | --- |
| `TryTake() (T, bool, error)` | Non-blocking read. |
| `Take(ctx) (T, bool, error)` | Block until an item is available, then read it. |
| `Drain(ctx) iter.Seq[T]` | Drain items present at call time. |
| `Follow(ctx) iter.Seq[T]` | Drain existing then future items until `ctx` is cancelled. |

## Semantics

- **`Options.MaxSegments` bounds the number of segment files** kept on disk at
  once, so the footprint is about `MaxSegments × SegmentSize`. When that many
  segments are already live, `Add` returns `ErrFull` until reading reclaims one.
  The bound is best-effort (checked against the files on disk), so the live count
  may briefly overshoot by a segment. A record too large to fit one segment is
  rejected with `ErrRecordTooLarge`. The default (a zero value) is 32; a negative
  value means unbounded.
- **At-most-once.** A record's read cursor advances as it is delivered and is
  persisted by the backend (governed by `SyncEvery`/`SyncInterval`, and always on
  `Close`), so a delivered-but-unprocessed record is **not** replayed after a
  crash. There is no two-phase acknowledgement.
- **Durability.** Writes go into the data files and survive a process crash via
  the page cache. `SyncEvery: N` batches one fsync over N operations; `SyncInterval`
  sets a wall-clock flush backstop (default 2s); `NoSync` minimizes fsync. `Close`
  always flushes.
- **Recovery.** go-diskqueue recovers from a torn file by renaming it with a
  `.bad` suffix and skipping to the next one. Recovery is lossy — the skipped data
  is gone — and each such event is counted by `DiskQueue.Corruptions()`.
- **Value lifetime.** Each consumed record is delivered as a freshly allocated
  slice owned by the caller, so the value handed to `UnmarshalFunc` stays valid
  indefinitely.
- **Concurrency.** A `DiskQueue` is safe for concurrent use. Readers share the one
  underlying read channel and cooperate (each item delivered once); a single
  `Reader` holds no mutable state, so one per consuming goroutine remains the
  clear idiom. `Drain` iterations are serialized so concurrent drainers split the
  stream without loss, duplication, or deadlock. The blocking `Take` and the
  `Follow` iterator wait for new data and honour their context.

## Options

```go
diskqueue.New[T](path, marshal, unmarshal, diskqueue.Options{
	MaxSegments:  0,    // 0 = 32 default; N>0 = cap live files (ErrFull); <0 = unbounded
	NoSync:       true, // minimize fsync (faster, no power-loss durability)
	SyncEvery:    0,    // 0/1 = fsync every op; N>1 = batch the fsync over N ops
	SyncInterval: 0,    // >0 = timer-driven flush period (default 2s)
	SegmentSize:  0,    // 0 = 8 MiB default; floored at 4 KiB
})
```

## License

MIT
