# CLAUDE.md

Guidance for working in this repository.

## What this is

`github.com/JohanLindvall/diskqueue` — a generic, durable FIFO disk-backed queue
for Go. The public API and behaviour are documented in [README.md](README.md);
read it first. It is a thin generic, typed layer over
[nsqio/go-diskqueue](https://github.com/nsqio/go-diskqueue) (`v1.1.0`), which
supplies the `[]byte`-only file backend; that is the only dependency.

- [diskqueue.go](diskqueue.go) — the generic `DiskQueue[T]` writer/owner (`New`,
  `Add`, `Empty`/`Count`/`Size`/`Corruptions`, `Close`, `NewReader`) wrapping a
  `godq.Interface`, plus the `MaxSegments` backpressure.
- [reader.go](reader.go) — `Reader[T]`: the consume ops (`Take`/`TryTake`/
  `Drain`/`Follow`) over the backend's shared read channel.
- [diskqueue_test.go](diskqueue_test.go) — all tests and `BenchmarkAddTake`.

## Build / test

```sh
go build ./...
go test ./...
go vet ./...
go test -run=^$ -bench=BenchmarkAddTake -benchtime=1s ./...
```

Note: `-race` needs a C compiler (cgo); run it where one is available.

## How it maps onto go-diskqueue

`godq.New(name, dir, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery,
syncTimeout, logf)` is created once in `New`. The fixed `queueName` ("diskqueue")
is the prefix for the backend's files (`diskqueue.diskqueue.NNNNNN.dat` and
`diskqueue.diskqueue.meta.dat`).

- `Add` → `Put`. We marshal into the reused `w.scratch` under `w.mu`, pre-check
  `len > maxMsgSize` (→ `ErrRecordTooLarge`, where `maxMsgSize = SegmentSize-4`),
  enforce `MaxSegments`, then `Put`. `Put` is synchronous, so reusing `scratch`
  afterwards is safe.
- `Take`/`TryTake`/`Drain`/`Follow` → receive from `dq.ReadChan()`. The backend's
  `ioLoop` surfaces records asynchronously, advancing the read cursor on delivery.
- `Count`/`Empty` → `dq.Depth()`. **Never call `dq.Empty()` — it is destructive**
  (it clears the queue). `Empty()` here means `Depth() == 0`.
- `Close` → closes `w.done` (unblocks readers) then `dq.Close()` (flushes + meta).

## Non-obvious invariants — keep these intact

- **At-most-once, single cursor.** There is no commit cursor and no `Reserve`/
  `Commit`. A record advances the persisted read position as it is *delivered*, so
  it is not replayed after a crash. Don't reintroduce offset-addressed acks against
  this backend — it has no place to store a second cursor.
- **`MaxSegments` backpressure is best-effort and lives here, not in the backend**
  (go-diskqueue is unbounded). `segmentsFull` globs the directory for live
  `*.dat` segment files (excluding `meta.dat`), but caches the count: it only
  re-globs when the cap is in reach or a `SegmentSize`'s worth of bytes has been
  written since the last refresh (`bytesSinceGlob`). So the happy path globs ~once
  per segment; the count may briefly overshoot the cap. The check runs under
  `w.mu` in `Add`. `maxSeg == 0` means unbounded (skip the check).
- **`TryTake` spins while `Depth() > 0`.** Because the backend surfaces records
  from a background goroutine, a record can be on disk a moment before it reaches
  `ReadChan()`. A naive non-blocking receive would report the queue empty while
  records exist (breaking the synchronous `Add`/`TryTake` lockstep the tests rely
  on). `TryTake` therefore retries (`runtime.Gosched()`) until a record arrives or
  `Depth()` hits 0. Don't replace this with a bare `select { default: }`.
- **`Drain` is serialized by `drainMu` and bounded by a `Depth()` snapshot.** It
  consumes up to the depth observed when iteration begins, re-checking
  `Depth() == 0` before each blocking receive so it never waits for a record
  another consumer took. The mutex prevents two concurrent drainers both blocking
  on the shared channel for the last record (which, with an uncancelled context,
  would deadlock). `Follow` is unbounded and relies on context cancellation to
  unblock.
- **Reader values don't alias a reused buffer.** Each `ReadChan()` delivery is a
  freshly allocated slice, passed straight to `unmarshal`; it stays valid
  indefinitely. (The old per-reader scratch copy is gone — the hot path is no
  longer zero-alloc, by nature of the channel backend.)
- **`Corruptions()` is derived from the backend log.** `w.logf` increments an
  atomic counter when go-diskqueue logs `"saving bad file"`. It is best-effort
  string matching; if the backend's log wording changes, update the match.
- **Sync mapping.** `NoSync` → a huge `syncEvery` plus a 24h `syncTimeout` (so the
  backend rarely fsyncs; `Close` still flushes). `SyncEvery<=1` → `1` (fsync per
  op). `SyncInterval` → `syncTimeout` (default 2s). go-diskqueue exposes **no
  on-demand flush**, which is why there is no `Sync()` method.

## Gotchas

- `New` must `os.MkdirAll(path)` first — go-diskqueue does not create the
  directory and silently fails writes if it is missing.
- `Depth()` round-trips the backend `ioLoop` over a channel; it returns the last
  value after close rather than blocking.
- The backend is goroutine-safe for `Put`/`ReadChan`/`Depth`; `w.mu` only guards
  this package's own state (`scratch`, `closed`, the segment-count cache).
- Removed vs. the old mmap store: `Reserve`/`Commit`/`TryReserve`, `Sync`,
  per-record checksums and strict `ErrCorrupt`/`ErrBadFormat`/
  `ErrSegmentSizeMismatch`, `MaxMapped`, `RecoverCorrupt`, and the zero-alloc hot
  path. `Size()` is now the physical on-disk footprint, not uncommitted bytes.
