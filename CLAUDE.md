# CLAUDE.md

Guidance for working in this repository.

## What this is

`github.com/JohanLindvall/diskqueue` — a generic, durable FIFO disk-backed queue
(a persistent work queue that doubles as a write-ahead log) for Go. The public API
and behaviour are documented in [README.md](README.md); read it first. Its only
dependency is `cespare/xxhash/v2` (per-record checksums); it ships its own file
store using plain `pread`/`pwrite`/`fsync` (no mmap).

- [store.go](store.go) — the `store`: the `[]byte`-only file backend (ReadAt/WriteAt).
- [diskqueue.go](diskqueue.go) — the generic `DiskQueue[T]` writer/owner (Add, Empty/Count/Size,
  Sync/Close, NewReader) on top of `store`.
- [reader.go](reader.go) — `Reader[T]`: all consume ops (Reserve/Take/Commit/
  Drain/Follow); copies each record into its own scratch buffer.
- [store_test.go](store_test.go) — store-level unit tests (`TestStore*`).
- [diskqueue_test.go](diskqueue_test.go) — DiskQueue-level tests and `BenchmarkAddTake`.

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
preallocated, capped at `maxSegments` live files, with handles opened **on demand**
(LRU, capped by `maxMapped`; the active file stays open). 64-byte LE header:
`[0:8]` magic, `[8:16]` commit cursor (next uncommitted record), `[16:24]` write
cursor (data end), `[24:32]` written count, `[32:40]` committed count, `[40]`
version, `[56:64]` xxhash64 of `[0:56]`; then records, each
`uvarint(len) || payload || xxhash64(payload)` (8-byte LE checksum trailer,
verified on read — mismatch → `ErrCorrupt`, cursor not advanced). Records are
written with one `WriteAt` (framed in the reused `s.writeBuf`) and read with
`ReadAt` into the reused `s.readBuf`. The header is the single source of truth —
recovery reads **no records at all**: `load` preads each 64-byte header, validates
magic/version (`ErrBadFormat`) and the header checksum (`ErrCorrupt`), and takes
data end, resume point and `Count()` from it. The write cursor / written count
(and the header checksum) are published *after* the record bytes (a separate
`writeHeader` + `fsync`) so only fully-written records are visible.

Each `dataFile` keeps its 64-byte header resident in memory (`df.hdr`); accessors
read `df.hdr`, and `writeHeader` writes it to page 0 with `WriteAt`. Every header
mutation goes through `df.header(mods ...func(*dataFile))`, which applies the
modifiers and rebuilds the checksum — so the checksum can't be forgotten. The
field setters (`setCommitCursor`, `setWriteCursor`, `setWrittenCount`,
`setCommittedCount`) **return** a modifier rather than writing in place, so they
compose as `header()` arguments; nothing touches the header bytes until `header()`
invokes them. The closures stay stack-allocated (escape analysis), keeping the hot
path zero-alloc.

Three cursors, all global byte offsets into the logical stream (file `F` holds
offsets `[F.base, F.base+F.size)`):

- `writeOff` — tail; next record append position.
- `headOff` — read cursor (in memory only; reset to `commitOff` on open).
- `commitOff` — commit cursor, persisted into the header of the file it lands in.

`nWritten`/`nCommitted` are global record counts for `Count()`; each file also
mirrors its own `written`/`committed` counts into its header.

## Non-obvious invariants — keep these intact

- **Zero-alloc hot path.** `append` frames the record in the reused `s.writeBuf`
  and `WriteAt`s it; `Add` serializes via the reused `w.scratch` and the
  append-style `MarshalFunc`. `Reader.read` copies the payload (a slice of the
  reused `s.readBuf`, filled by `ReadAt`) into the reused `r.scratch` (one memcpy,
  no alloc once warm) before `unmarshal`. The `s.writeBuf`/`s.readBuf` grow via
  `growBuf` (allocates only when a bigger record appears). Don't add per-op heap
  allocations on Add / read / commitTo. The benchmark guards this.
- **Readers own the returned bytes.** All consume ops live on `Reader[T]`
  ([reader.go](reader.go)); each copies the record into `r.scratch` *under the
  lock*, so the value never aliases `s.readBuf` (which the next read overwrites).
  Valid until the reader's next read. A `Reader` is single-goroutine; use one per
  consumer. This copy is load-bearing: since consume ops commit a record as they
  read it, its file may be closed/removed while the consumer still holds the value
  (see "Immediate close is safe").
- **Reclamation is whole-file, on write *and* commit.** `dropCommitted(keep)`
  deletes files whose every record is committed (`base+size <= commitOff`), except
  `keep`. It runs from two places: `cycle` (from `append`) with `keep == nil` —
  the soon-to-be-old active file may go since a new one follows immediately — and
  the end of `commitTo` with `keep == s.active()`, so a consume-only or producer-
  stopped workload reclaims disk without waiting for the next write, but never
  drops the active file (it holds the write position). The commit-path removal is
  *not* `syncDir`'d, so reclamation is best-effort: a file lingering after a crash
  is re-dropped on the next drop and never re-delivered (its records stay
  committed), so correctness doesn't depend on the removal being durable.
- **Immediate close is safe** because `Reader.read` copies the payload into
  `r.scratch` *under the lock*; the value the consumer holds is its own copy, not a
  slice of `s.readBuf`. This is load-bearing now that **commits** reclaim too:
  `Take`/`Drain`/`Follow` read-then-`commitTo` under the lock, and that same
  `commitTo` can fully commit and `dropCommitted` the just-delivered record's file
  (closing its handle and removing it) — but the scratch copy already happened
  (read precedes commit), so the held value stays valid. A concurrent `Add`'s
  `dropCommitted` can do the same. All store ops hold the DiskQueue mutex.
- **`maxSegments` bounds the file count.** `cycle` drops committed files, then
  returns `ErrFull` if `len(files) >= maxSegments` (0 = unbounded). So the bound
  is on *segments*, not bytes; footprint ≈ `maxSegments × segmentSize`.
- **Records never span files.** `append` cycles when `size+recLen > segmentSize`.
  A record bigger than `segmentSize` is `ErrRecordTooLarge`.
- **Recovery (`load`) reads no records.** Reopen preads each file's 64-byte header
  (no mapping), validates it, and takes the data end from the write cursor and the
  `written`/`committed` counts from the header (summed into `nWritten`/
  `nCommitted`), with `commitOff` from the first file whose commit cursor is short
  of its end; `headOff = commitOff`. Only the active file is opened at the end of
  `load`. `dropCommitted` subtracts the dropped file's counts (it's fully
  committed, so `written == committed`, keeping `Count` exact). Fully-committed
  leading files are *not* dropped on open.
- **Corruption is strict by default, opt-in recovery via `recoverCorrupt`.**
  `read` returns `ErrCorrupt` (not empty) for an undecodable record; `takeHead`
  surfaces a bad length/checksum as `ErrCorrupt`. With `recoverCorrupt`: `load`
  drops a torn *trailing* segment (only the highest num — earlier files hold
  committed data) and recreates a fresh file if all were dropped; `takeHead`
  calls `skipCorruptSegment`, which advances `headOff` past the segment and
  force-commits its tail when `commitOff` is already inside it (auto-commit path),
  reclaiming it. Recovery is lossy; `s.corruptions` counts events (`DiskQueue.Corruptions`).
- **Blocking waiters.** `waitLocked`/`signal` use a lazily-created `notify`
  channel, nil when nobody waits, so `Add` stays allocation-free.
- **`Reader.Drain`/`Follow` consume** via the shared `headOff` (like iterator-
  shaped `Take`): read **and `commitTo` under the lock**, then release and yield —
  so they commit-on-read (at-most-once) and are safe for concurrent cooperating
  readers. `Drain` is bounded by a `writeOff` snapshot; `Follow` waits via
  `waitLocked` (which lives on DiskQueue, called as `r.w.waitLocked`).
- **Sync policy.** `noSync` skips `fsync`; `syncEvery <= 1` syncs every write/
  commit inline; `syncEvery > 1` (`batched()`) defers to `flushBatch` every N ops.
  A torn tail from a power loss between batched flushes is caught by the per-record
  xxhash on read. `sync()`/`Close` always flush; an optional `SyncInterval`
  goroutine (`syncLoop`, stopped by `Close` via `syncStop`/`syncDone`) flushes on a
  timer as a wall-clock backstop.
- **Data-before-header durability.** The per-op `append` does **two** fsyncs:
  `WriteAt` the record, `fsync` (record bytes durable), then `writeHeader` +
  `fsync` (the write cursor that publishes them durable). Persisting the header
  first would let a power loss leave a visible record whose payload never landed (a
  torn tail the checksum flags); data-then-header guarantees a clean truncation
  instead. The recovery-fault tests pin this. Don't collapse it to one fsync on the
  per-op path.
- **Dirty tracking.** Each `dataFile` has a single `dirty` bool: `writeRecord`/
  `writeHeader` set it (page-cache writes not yet fsync'd); `flushFile` fsyncs and
  clears it, and is a no-op for a clean or closed (`f == nil`) file. Batched
  `append`/`commitTo` and `noSync` `writeHeader` the record/header into the page
  cache (so a reopen-via-`load` and `readFileHeader` see them) but defer the fsync;
  the per-op path fsyncs inline and clears `dirty`. `flushFile`/`flushBatch`/
  `sync`/`close`/`evictMapped` fsync only dirty files and skip clean ones — a file
  only read since its last flush is closed with no fsync. Under `noSync`, eviction
  just clears `dirty` and relies on kernel writeback (the page-cache bytes survive
  the handle being closed).
- **Lazy open.** Files open on demand via `ensureMapped` (`read`, `commitTo`, and
  `append` for the active file); `evictMapped` closes the LRU handle beyond
  `maxMapped`, never the active or just-opened file, fsyncing a dirty victim first
  (a clean victim is closed without fsync). `df.f == nil` means closed —
  `flushFile`/`sync`/`flushBatch`/`close` skip such files; `df.hdr` stays resident
  so accessors still work and a later `ensureMapped` just reopens the handle.
  `createFile` writes (and, unless `noSync`, fsyncs) the fresh header so a
  cycled-but-empty segment is a valid file on disk.

## Gotchas

- `Take`/`Drain`/`Follow` advance `headOff` and commit; `Reserve` advances
  `headOff` without committing (so `Empty()` can be true while `Count() > 0`).
- Offsets are byte positions, monotonic within a session, not stable across a
  reopen (head resets to the recovered commit cursor).
- I/O is `os.File` `ReadAt`/`WriteAt`/`Sync` plus a directory `fsync` (`syncDir`)
  for durable creates/removes. No mmap, so no `golang.org/x/sys` dependency; the
  directory fsync is POSIX (a no-op or error on some platforms, but fine on
  Linux/Unix).
