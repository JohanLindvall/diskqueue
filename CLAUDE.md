# CLAUDE.md

Guidance for working in this repository.

## What this is

`github.com/JohanLindvall/wal` — a generic, durable FIFO queue (write-ahead log)
for Go. The public API and behaviour are documented in [README.md](README.md);
read it first. It has **no third-party dependencies** beyond `golang.org/x/sys`
(for mmap/msync) — it used to wrap `tidwall/wal`, but now ships its own store.

- [store.go](store.go) — the `store`: the mmap-backed, `[]byte`-only file backend.
- [wal.go](wal.go) — the generic `WAL[T]` writer/owner (Add, Empty/Count/Size,
  Sync/Close, NewReader) on top of `store`.
- [reader.go](reader.go) — `Reader[T]`: all consume ops (Reserve/Take/Commit/
  Drain/Follow); copies each record into its own scratch buffer.
- [store_test.go](store_test.go) — store-level unit tests (`TestStore*`).
- [wal_test.go](wal_test.go) — WAL-level tests and `BenchmarkAddTake`.

## Build / test

```sh
go build ./...
go test -race ./...
go vet ./...
go test -run=^$ -bench=BenchmarkAddTake -benchtime=1s ./...   # must stay 0 allocs/op
go test -cover ./...
```

## Storage model (store.go)

A directory of numbered files `data.00000001`, … each `SegmentSize` bytes,
preallocated and mmap'd, capped at `maxSegments` live files. 32-byte header of
four little-endian uint64 file-local values: `[0:8]` commit cursor (next
uncommitted record), `[8:16]` write cursor (data end), `[16:24]` written record
count, `[24:32]` committed record count; then `[32:cap]` records, each
`uvarint(len) || payload`. The header is the single source of truth — recovery
reads **no records at all** (data end, resume point, and `Count()` all come from
the header). The write cursor / written count are published *after* the record
bytes so only fully-written records are visible.

Three cursors, all global byte offsets into the logical stream (file `F` holds
offsets `[F.base, F.base+F.size)`):

- `writeOff` — tail; next record append position.
- `headOff` — read cursor (in memory only; reset to `commitOff` on open).
- `commitOff` — commit cursor, persisted into the header of the file it lands in.

`nWritten`/`nCommitted` are global record counts for `Count()`; each file also
mirrors its own `written`/`committed` counts into its header.

## Non-obvious invariants — keep these intact

- **Zero-alloc hot path.** `append` writes straight into the mmap; `Add`
  serializes via the reused `w.scratch` and the append-style `MarshalFunc`.
  `Reader.read` copies the mmap payload into the reused `r.scratch` (one memcpy,
  no alloc once warm) before `unmarshal`. Don't add per-op heap allocations on Add
  / read / commitTo. The benchmark guards this.
- **Readers own the returned bytes.** All consume ops live on `Reader[T]`
  ([reader.go](reader.go)); each copies the record into `r.scratch` *under the
  lock*, so the value never aliases the mmap and survives unmapping. Valid until
  the reader's next read. A `Reader` is single-goroutine; use one per consumer.
  Because of this copy the deferred-unmap below is now belt-and-suspenders, not
  load-bearing for the public API.
- **Reclamation is write-only and whole-file.** `dropCommitted` (called only from
  `cycle`, i.e. from `append`) deletes files whose every record is committed
  (`base+size <= commitOff`), never the active file. Reads/commits must never
  delete a file — this is deliberate ("only writes can cycle").
- **Immediate unmap is safe** because `Reader.read` copies the payload into
  `r.scratch` *under the lock*, and a just-read record's file is never fully
  committed (its `base+size > commitOff`), so `dropCommitted` can't unmap it. All
  store ops hold the WAL mutex, so no munmap races a read. (Touching an mmap slice
  after `munmap` is a SIGSEGV the GC can't prevent — that's why the copy matters.)
- **`maxSegments` bounds the file count.** `cycle` drops committed files, then
  returns `ErrFull` if `len(files) >= maxSegments` (0 = unbounded). So the bound
  is on *segments*, not bytes; footprint ≈ `maxSegments × segmentSize`.
- **Records never span files.** `append` cycles when `size+recLen > segmentSize`.
  A record bigger than `segmentSize` is `ErrRecordTooLarge`.
- **Recovery (`load`) reads no records.** Reopen takes each file's data end from
  its write cursor and its `written`/`committed` counts from the header (summed
  into `nWritten`/`nCommitted`), and `commitOff` from the first file whose commit
  cursor is short of its end; `headOff = commitOff`. `dropCommitted` subtracts the
  dropped file's counts (it's fully committed, so `written == committed`, keeping
  `Count` exact). Fully-committed leading files are *not* dropped on open.
- **Blocking waiters.** `waitLocked`/`signal` use a lazily-created `notify`
  channel, nil when nobody waits, so `Add` stays allocation-free.
- **`Reader.Drain`/`Follow` consume** via the shared `headOff` (like iterator-
  shaped `Take`): read, release lock, yield, then `commitTo`. `Drain` is bounded
  by a `writeOff` snapshot; `Follow` waits via `waitLocked` (which lives on WAL,
  called as `r.w.waitLocked`).

## Gotchas

- `Take`/`Drain`/`Follow` advance `headOff` and commit; `Reserve` advances
  `headOff` without committing (so `Empty()` can be true while `Count() > 0`).
- Offsets are byte positions, monotonic within a session, not stable across a
  reopen (head resets to the recovered commit cursor).
- mmap/msync are Linux/Unix via `golang.org/x/sys/unix`; this is not portable to
  Windows as written.
