# CLAUDE.md

Guidance for working in this repository.

## What this is

`github.com/JohanLindvall/wal` — a generic, durable FIFO queue (write-ahead log)
for Go. The public API and behaviour are documented in [README.md](README.md);
read it first. Its only dependencies are `golang.org/x/sys` (mmap/msync) and
`cespare/xxhash/v2` (per-record checksums) — it used to wrap `tidwall/wal`, but
now ships its own store.

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
preallocated, capped at `maxSegments` live files, and mmap'd **on demand** (LRU,
capped by `maxMapped`; the active file stays mapped, the fd is closed right after
mmap). 64-byte LE header: `[0:8]` magic, `[8:16]` commit cursor (next uncommitted
record), `[16:24]` write cursor (data end), `[24:32]` written count, `[32:40]`
committed count, `[40]` version, `[56:64]` xxhash64 of `[0:56]`; then records,
each `uvarint(len) || payload || xxhash64(payload)` (8-byte LE checksum trailer,
verified on read — mismatch → `ErrCorrupt`, cursor not advanced). The header is
the single source of truth — recovery reads **no records at all**: `load` preads
each 64-byte header (no mapping), validates magic/version (`ErrBadFormat`) and the
header checksum (`ErrCorrupt`), and takes data end, resume point and `Count()`
from it. The write cursor / written count (and the header checksum) are published
*after* the record bytes so only fully-written records are visible.

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
  This copy is load-bearing: since consume ops commit a record as they read it,
  its file may be unmapped while the consumer still holds the value (see
  "Immediate unmap is safe").
- **Reclamation is write-only and whole-file.** `dropCommitted` (called only from
  `cycle`, i.e. from `append`) deletes files whose every record is committed
  (`base+size <= commitOff`), never the active file. Reads/commits must never
  delete a file — this is deliberate ("only writes can cycle").
- **Immediate unmap is safe** because `Reader.read` copies the payload into
  `r.scratch` *under the lock*; the value the consumer holds never aliases the
  mapping. This is now the *only* guarantee: `Take`/`Drain`/`Follow` commit a
  record as they read it, so a just-delivered record's file **can** be fully
  committed and unmapped by a concurrent `Add`'s `dropCommitted` while the
  consumer still holds the value — the scratch copy, not file retention, is what
  keeps it valid. All store ops hold the WAL mutex, so no munmap races the read
  itself. (Touching an mmap slice after `munmap` is a SIGSEGV the GC can't
  prevent — that's why the copy matters.)
- **`maxSegments` bounds the file count.** `cycle` drops committed files, then
  returns `ErrFull` if `len(files) >= maxSegments` (0 = unbounded). So the bound
  is on *segments*, not bytes; footprint ≈ `maxSegments × segmentSize`.
- **Records never span files.** `append` cycles when `size+recLen > segmentSize`.
  A record bigger than `segmentSize` is `ErrRecordTooLarge`.
- **Recovery (`load`) reads no records.** Reopen preads each file's 64-byte header
  (no mapping), validates it, and takes the data end from the write cursor and the
  `written`/`committed` counts from the header (summed into `nWritten`/
  `nCommitted`), with `commitOff` from the first file whose commit cursor is short
  of its end; `headOff = commitOff`. Only the active file is mapped at the end of
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
  reclaiming it. Recovery is lossy; `s.corruptions` counts events (`WAL.Corruptions`).
- **Blocking waiters.** `waitLocked`/`signal` use a lazily-created `notify`
  channel, nil when nobody waits, so `Add` stays allocation-free.
- **`Reader.Drain`/`Follow` consume** via the shared `headOff` (like iterator-
  shaped `Take`): read **and `commitTo` under the lock**, then release and yield —
  so they commit-on-read (at-most-once) and are safe for concurrent cooperating
  readers. `Drain` is bounded by a `writeOff` snapshot; `Follow` waits via
  `waitLocked` (which lives on WAL, called as `r.w.waitLocked`).
- **Sync policy.** `noSync` skips msync; `syncEvery <= 1` msyncs every write/
  commit (ordered: data then header); `syncEvery > 1` (`batched()`) defers to
  `flushBatch` every N ops. A torn tail from a power loss between batched flushes
  is caught by the per-record xxhash on read. `sync()`/`Close` always flush; an
  optional `SyncInterval` goroutine (`syncLoop`, stopped by `Close` via
  `syncStop`/`syncDone`) flushes on a timer as a wall-clock backstop.
- **Lazy mapping.** Files map on demand via `ensureMapped` (`read`, `commitTo`,
  and `append` for the active file); `evictMapped` unmaps the LRU beyond
  `maxMapped`, never the active or just-mapped file, msync'ing a victim first
  (a batched commit may have left its header dirty). `df.data == nil` means
  unmapped — `msync`/`sync`/`flushBatch`/`close` skip such files. `createFile`
  msyncs the fresh header so a cycled-but-empty segment is a valid file on disk.

## Gotchas

- `Take`/`Drain`/`Follow` advance `headOff` and commit; `Reserve` advances
  `headOff` without committing (so `Empty()` can be true while `Count() > 0`).
- Offsets are byte positions, monotonic within a session, not stable across a
  reopen (head resets to the recovered commit cursor).
- mmap/msync are Linux/Unix via `golang.org/x/sys/unix`; this is not portable to
  Windows as written.
