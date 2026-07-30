# CLAUDE.md

Guidance for working in this repository.

## What this is

`github.com/JohanLindvall/diskqueue` — a generic, durable FIFO disk-backed queue
(a persistent work queue that doubles as a write-ahead log) for Go. The public API
and behaviour are documented in [README.md](README.md); read it first. Its only
dependency is `cespare/xxhash/v2` (per-record checksums); it ships its own file
store using plain `pread`/`pwrite`/`fsync` (no mmap).

- [header.go](header.go) — the on-disk format alone: the constants, `dataFile`, the 64-byte
  header accessors/setters and the checksum. Pure — no I/O, no knowledge of the store.
  Changing anything here changes what is written to disk.
- [record.go](record.go) — the record frame both ways: `writeRecord`, `recordAt`/`recordLen`/
  `frameEnd`, and the guards (`fitsInRecord`, `shortReadIsCorrupt`, `growBuf`, `uvarintLen`).
- [recovery.go](recovery.go) — the open path and nothing else: `load`, `loadFile`,
  `startFresh`, `dropSegment`, `removeFile`, `readHeader`.
- [store.go](store.go) — the `store`: the `[]byte`-only file backend (ReadAt/WriteAt). Keeps
  everything whose ordering is the invariant — the handle LRU, the segment lifecycle, the
  durability policy, `append`, the read/quarantine path, `commitTo` and the gauges. These are
  deliberately *not* split further: pulling `writeHeader`/`flushFile` away from `append` and
  `commitTo` would separate the calls whose order is the whole point (see the
  data-before-header invariant below).
- [diskqueue.go](diskqueue.go) — the generic `Queue[T]` writer/owner (Add, Empty/Count/Size,
  Stats/Rewind/Sync/Err/Close, NewReader) on top of `store`.
- [reader.go](reader.go) — `Reader[T]`: all consume ops (Reserve/Take/Commit/
  Drain/Follow/Err); copies each record into its own scratch buffer.
- [prealloc_linux.go](prealloc_linux.go) / [prealloc_other.go](prealloc_other.go) — `preallocate`:
  `syscall.Fallocate` (stdlib, Linux) with an `ftruncate` fallback everywhere else and on
  filesystems that answer ENOTSUP/ENOSYS/EINVAL.
- [lock_flock.go](lock_flock.go) / [lock_other.go](lock_other.go) — `tryLockDir`: non-blocking
  `flock(LOCK_EX)` on the directory handle → `ErrLocked`; a no-op where stdlib has no flock
  (Windows, Solaris, AIX, plan9, js).
- [syncdir_unix.go](syncdir_unix.go) / [syncdir_other.go](syncdir_other.go) — `fsyncDir`: a
  directory fsync is POSIX-only; off Unix it must be a no-op or every cycle would fail.
- [store_test.go](store_test.go) — store-level unit tests (`TestStore*`).
- [diskqueue_test.go](diskqueue_test.go) — Queue-level tests and `BenchmarkAddTake`.
- [faults_test.go](faults_test.go) — `//go:build diskqueue_faults`. The tests that need a seam
  the default build does not have: `append`'s fsync ordering and each of its failure arms.
  Run with `go test -tags diskqueue_faults ./...` (`make faults`).
- [aliasing_test.go](aliasing_test.go) — why the Reader copies: `s.readBuf` is shared by every
  Reader, so the copy is about *sharing*, not file lifetime.
- [progress_test.go](progress_test.go) — no consume op may report damage it did not step past.
- [bench_test.go](bench_test.go) / [stats_test.go](stats_test.go) — the syscall-bound paths, and
  the counters-vs-gauges distinction.
- [recovery_test.go](recovery_test.go) — the recovery contract from the public API:
  never wedged, never delivered as data, every loss counted in `Stats`.
- [robust_test.go](robust_test.go) — live fault injection (`breakHandle`/`reopenReadOnly`/
  `repairHandle` swap a segment's descriptor behind the store's back), covering the I/O-error
  paths. [recovery_fault_test.go](recovery_fault_test.go) does the on-disk (between close and
  reopen) equivalent.
- `cover_*_test.go` — the error and edge arms of the six densest functions, which had 44
  never-executed coverage blocks between them (78%) and now have 4 (98%): `cover_committo`,
  `cover_load`, `cover_loadfile`, `cover_quarantine` (`skipCorruptSegment`),
  `cover_forcecommit` (`forceCommitAll`, plus `Reader.next`'s hard-error exit) and
  `cover_append`. Two techniques worth reusing live here: `quarantineSinkHandle` swaps in a
  `/dev/null` descriptor, the only shape where **pwrite lands and fsync fails** (neither
  `breakHandle` nor `reopenReadOnly` can make it — with those the write fails first and the
  flush is never reached), and forging `s.lastFrameAt`/`lastFrameEnd` reaches `commitTo`'s
  destination guard, which `recordLen` otherwise bounds out of reach.

  Each file opens with an `// UNCOVERED:` note stating what it could **not** reach and why.
  Keep those honest: the four remaining blocks are unreachable without a production seam, and
  three of them (`append`'s `writeHeader` and header-`fsync` arms, `load`'s repair `flushFile`)
  have their semantics covered by the faults build's adjacent `faultPoint` arms instead. The
  fourth, `Reader.next`'s `if !ok`, is excluded by its own `drained`/`empty` guards. The
  `loadFile` note also records a clamp whose *value* is pinned but whose *existence* no test can
  detect, which is the kind of thing worth knowing before trusting a green suite.

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
preallocated (real blocks via `fallocate`, see `preallocate`), capped at
`maxSegments` live files, with handles opened **on demand**
(LRU, capped by `maxOpenFiles`; the active file stays open). The directory itself is
opened once in `openStore` and held in `s.dirFile` for the session: it is both the
handle `syncDir` fsyncs and the thing the advisory `flock` hangs on, so closing the
store releases the lock. 64-byte LE header:
`[0:8]` magic, `[8:16]` commit cursor (next uncommitted record), `[16:24]` write
cursor (data end), `[24:32]` written count, `[32:40]` committed count, `[40]`
version, `[41]` flags (`flagOversized`), `[56:64]` xxhash64 of `[0:56]`; then records, each
`uvarint(len) || payload || xxhash64(payload)` (8-byte LE checksum trailer,
verified on read — mismatch → `ErrCorrupt`, and the record is dropped). Records
are written with one `WriteAt` (framed in the reused `s.writeBuf`) and read with
`ReadAt` into the reused `s.readBuf`. The header is the single source of truth —
recovery reads **no records at all**: `load` preads each 64-byte header, validates
the magic, the version (`knownVersion`) and the header checksum, and takes
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

- **Two classes of I/O failure, and they are not treated alike.** A *write* (pwrite,
  create, open, unlink) failure leaves the store consistent — nothing was published —
  so it is returned and the caller may retry; it must **not** latch. An *fsync*
  failure is unrecoverable in place: Linux reports a writeback error once and then
  drops the dirty pages, so a retry can report success over lost data. Every fsync
  site therefore goes through `s.failIO`, which latches `s.ioErr` (wrapping `ErrIO`
  *and* the errno, so `errors.Is` finds both) and returns it forever after.
  `append`/`commitTo`/`sync` check `s.ioErr` on entry; reads deliberately do not, so
  a poisoned queue can still be drained. Checking is a nil compare, so the hot path
  stays zero-alloc. `close` reports the latch and skips the pointless flush.
- **Every I/O call site returns its error.** `commitTo`, `recordOp`/`flushBatch` and
  `skipCorruptSegment` all return `error` (they used to swallow everything);
  `dropCommitted` is the one deliberate exception, and it compensates by keeping an
  un-unlinkable file in `s.files`. The `_ =` discards that remain are all annotated
  with why. Don't reintroduce a silent one.
- **Error classification is a three-way split, and it decides what may be deleted.**
  (1) *Corruption* — a bad checksum, an undecodable frame, a short read, a segment
  that has vanished: wrapped in `ErrCorrupt`, and only this class licenses
  the recovery paths to drop data. (2) *Retriable* — EACCES/EMFILE/EIO/ENOSPC from
  an open, write or unlink: returned as itself, store untouched, nothing deleted.
  (3) *Durability* — a failed fsync: latched as `ErrIO`. Laundering (2) into (1) is
  how an audit found a chmod blip unlinking a healthy segment, so `readHeader`
  returns open errors raw and only wraps a short `io.ReadFull`, and `loadFile`
  fails the open rather than dropping a segment it could not read.
- **All record-length arithmetic happens in uint64/int64 (`fitsInRecord`).** A
  corrupt prefix decodes to an arbitrary uint64; narrowing it first and summing
  `n+L+checksumSize` in `int` wraps *negative*, sails past a signed bounds check and
  panics reslicing `readBuf` in `growBuf` — a corrupt byte on disk killing the
  process, which is exactly what this library must never do. `commitTo` additionally
  refuses any decoded `next` that is not strictly forward and within `writeOff`.
- **The read cursor may never lag the commit cursor.** `commitTo` drags `headOff` up
  to `commitOff` before `dropCommitted`; otherwise a `Commit` past the read cursor
  reclaims the file `headOff` is standing in and every later read addresses a
  deleted file. `Reader.Commit` also rejects an offset beyond `headOffset()`.
- **Zero-alloc hot path.** `append` frames the record in the reused `s.writeBuf`
  and `WriteAt`s it; `Add` serializes via the reused `w.scratch` and the
  append-style `MarshalFunc`. `Reader.read` copies the payload (a slice of the
  reused `s.readBuf`, filled by `ReadAt`) into the reused `r.scratch` (one memcpy,
  no alloc once warm) before `unmarshal`. The `s.writeBuf`/`s.readBuf` grow via
  `growBuf` (allocates only when a bigger record appears). Don't add per-op heap
  allocations on Add / read / commitTo. The benchmark guards this.
- **Readers own the returned bytes, because `s.readBuf` is shared.** All consume
  ops live on `Reader[T]` ([reader.go](reader.go)); each copies the record into
  its own `r.scratch` *under the lock*. The reason is **sharing, not lifetime**:
  `s.readBuf` belongs to the store and *every* Reader on the queue reads through
  it, so without a per-Reader copy one consumer holding its value while another
  reads gets its bytes rewritten from a different goroutine — a data race, which
  `TestConcurrentReadersRace` reports under `-race`. (The bytes are ordinary Go
  memory filled by `ReadAt`; closing or unlinking the segment cannot invalidate
  them. That was mmap-era reasoning and it is no longer the operative reason,
  though the conclusion it reached is still true.) Valid until that Reader's next
  read. A `Reader` is single-goroutine; use one per consumer.
- **Reclamation is whole-file, on write *and* commit.** `dropCommitted(keep)`
  deletes files whose every record is committed (`base+size <= commitOff`), except
  `keep`. A file whose `os.Remove` fails stays in `s.files` (counts not subtracted,
  `s.unreclaimed++`), so `maxSegments` keeps describing what is on disk and the next
  drop retries it — dropping it from the slice would leak disk under a bound that
  claims otherwise. It runs from two places: `cycle` (from `append`) with `keep == nil` —
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
  `dropCommitted` can do the same. All store ops hold the Queue mutex.
- **Two independent caps, and they mean different things.** `maxSegments` bounds
  the *file count* (`cycle` drops committed files, then returns `ErrFull` if
  `len(files) >= maxSegments`; 0 = unbounded). `maxBytes` bounds the *uncommitted
  backlog in bytes*, checked in `append` before anything is written. Whichever
  binds first wins. `maxBytes` is an admission policy, not a geometry — it is
  deliberately **not** an `openStore` parameter and is set on the store after the
  open, so recovery never consults it and a store written under one cap reopens
  cleanly under another. A record longer than `maxBytes` returns
  `ErrRecordTooLarge` (permanent — no amount of draining admits it) rather than
  `ErrFull` (transient).
- **Records never span files — but `SegmentSize` is not a ceiling on record size.**
  `append` cycles when `size+recLen > df.capacity`, which is per-file: ordinary
  segments get `segmentSize`, and a record too large for that gets a segment sized
  to exactly its framed length. `cycle(need)` reserves `max(segmentSize, need)`.
  Such a segment sets `flagOversized` in header byte `[41]` (inside the checksummed
  `[0:56]`), and `loadFile` exempts it from the geometry check *by that flag only*.
  The flag cannot be inferred: "longer than `headerSize+segmentSize`" equally
  describes a store created with a **larger** `SegmentSize` and reopened with a
  smaller one, which must still be `ErrSegmentSizeMismatch` rather than silently
  half-read. Because its capacity equals its size, an oversized segment can never
  take a second record. `Stats().DiskBytes` sums per-file capacity (`diskBytes()`),
  not `len(files) × segmentSize`. `Add` drops an outsized `w.scratch` on **success**
  as well as failure, or one huge record pins its buffer for the queue's lifetime.
- **Recovery (`load`) reads no records — with exactly one licensed exception.** Reopen preads each file's 64-byte header
  (no mapping) in `loadFile`, validates it, and takes the data end from the write
  cursor and the `written` count from the header, with `commitOff` from the first
  file whose commit cursor is short of its end; `headOff = commitOff`. Only the
  active file is opened at the end of `load` (and its reservation re-asserted, best
  effort, so a segment from a sparse-era build gets its blocks). `dropCommitted`
  subtracts the dropped file's counts (it's fully committed, so
  `written == committed`, keeping `Count` exact). Fully-committed leading files are
  *not* dropped on open.

  The exception is `surviveCount`, and it is licensed by the segment's own header
  having proved the file lost bytes: that header's record count then describes a
  file which no longer exists, so believing it makes `Count()` promise a backlog no
  drain can deliver and the queue never reads empty again. An arithmetic bound
  (`size / minRecordSize`) is not enough — it is only tight for minimal records and
  otherwise sits above the header's count and never fires. So a *truncated* segment,
  and only a truncated one, gets a bounded frame walk over the bytes that survived.
  Every healthy open still costs one 64-byte pread per segment.
- **Committed counts come from the recovered cursor, not from each header.** The
  counts are per-segment while the cursor is global, and writeback across segments
  is not ordered — a header can claim "fully committed" for records the (rewound)
  cursor will replay. Counting those at load *and* again when they are re-consumed
  drove `Count()` negative. So `load` does a second pass: below `commitOff` →
  `committed = written`, above it → `0`, and the one segment straddling it keeps its
  header's figure. This is also what makes `dropCommitted`'s "written == committed
  here" true by construction.
- **Wrong file size: geometry vs. truncation.** Because segments are preallocated to
  exactly `headerSize+segmentSize`, any other size is either a store built with a
  different `SegmentSize` or a file that lost bytes. `loadFile` tells them apart with
  the file's own (validated) header: `writeCursor > fileSize` means the published
  bytes are gone, so it is corruption — clamp `size` to what is really there, count
  the cut tail in `discardedBytes`, and let the per-record checksum handle whatever
  the truncation ran into; otherwise it is a complete file of another geometry →
  `ErrSegmentSizeMismatch`. Checking a max over `os.Stat` sizes, as before,
  misreported a truncated single-segment store as a config mismatch and locked
  recovery out of it.
- **A zero-length segment is not corruption.** It is a create interrupted between
  the link and the header write, and can hold no record — `loadFile` removes it and
  raises no loss event, so nobody is paged for an aborted create. Together with
  `createFile` unlinking its own partial file on every error path, a failed segment
  create can never brick a later open.
- **Recovery is the behaviour, not an option** (ported from the sibling project
  [spool](https://github.com/JohanLindvall/spool), whose README states the two
  rules). **Corruption degrades to reported loss — never to corrupt output, never
  to a wedged queue** — and **every loss path is observable**. There is no strict
  mode: a queue that answers `ErrCorrupt` forever is unavailable *as well as*
  damaged, and it also stops all reclamation, so the disk fills behind the stuck
  cursor. Each event surfaces as exactly one `ErrCorrupt` and is counted.
- **Blast radius is decided by how much framing survives.** This is the rule to
  preserve through any change to `takeHead`:
  - the record's length framed it inside the segment but its checksum fails →
    drop **that record only** (`lostRecords`), advance `headOff` to `next`;
  - the length itself is unusable — undecodable, overrunning the segment, or the
    file is gone → the boundaries behind it are gone with it, so
    `skipCorruptSegment` abandons the **rest of the segment** (`lostSegments`);
  - a genuine I/O error is **not** damage: nothing is dropped and the cursor stays
    put, because the bytes may still be there next time.
  `shortReadIsCorrupt` and the `os.ErrNotExist` wrap in `read` are what route the
  second class in; without them a truncated or vanished segment arrives as a bare
  `io.EOF`/`ENOENT` that no recovery path recognises, and the reader wedges.
- **A segment counts as lost only if something in it was.** `skipCorruptSegment`
  books `lostSegments` inside the `lost := end - s.headOff; lost > 0` guard, not
  beside it. Reached from the commit path the read cursor is often already past the
  segment's end — every record in it was delivered — and a lost segment reported
  against zero lost bytes tells an operator data went missing when none did. The
  `corruptions` bump stays unconditional: the event did happen.
- **`pendingCorrupt` is the backlog of losses with no read of their own.** A
  segment dropped at open loses records that no `takeHead` will ever fail on, so
  the count is paid down one per `takeHead` call — otherwise the records are
  simply missing from the stream with nothing said. `empty()` accounts for it, so
  a blocking consumer wakes up to collect the reports.
- **`load` drops a damaged segment wherever it sits**, not only at the tail, and
  the open succeeds. `dropSegment` splits the two reasons apart: a *damaged*
  header (bad magic, bad checksum, short read) is data loss — counted in
  `lostSegments`/`lostBytes`, owed one `ErrCorrupt`; an *unknown version*
  (`knownVersion`) is a format change whose data the design accepts dropping —
  counted in `foreignSegments` and silent. A zero-length file is an aborted create
  and is removed with no event at all. Numbering always resumes past the highest
  seen, never at 1.
- **`skipCorruptSegment` applies the in-memory force-commit only *after* the
  header carrying it is durable** (and rolls the header bytes back if the write
  fails), so a failure there cannot leave the store believing it quarantined a
  segment the next open will replay. When the segment's file has vanished there is
  no header to write, and the in-memory advance is all there is to do.

  **This binds the `df == nil` arm too**, which is why it goes through
  `forceCommitAll` rather than squaring the counters itself. It used to do the
  latter — every `df.committed`, `nCommitted`, `nCommittedTotal` and `commitOff`
  moved with no header written anywhere — so the next open recovered the untouched
  cursors and replayed every record, while `nCommittedTotal` (a lifetime counter no
  reopen undoes) had already counted them and `Stats().Committed` counted them
  again on the way back out. Every in-process assertion passed; only a reopen
  caught it. `forceCommitAll` publishes and flushes each file's header before
  moving that file's counts, and moves the per-file and global counts *together*
  because `dropCommitted` subtracts `df.committed` from `nCommitted` as it
  reclaims. The arm is unreachable through the public API (both callers pass a
  cursor `commitTo` keeps inside a live segment) and cannot be deleted either —
  dropping the guard nil-derefs on the next line.
- **Blocking waiters.** `waitLocked`/`signal` use a lazily-created `notify`
  channel, nil when nobody waits, so `Add` stays allocation-free.
- **`Reader.Drain`/`Follow` consume** via the shared `headOff` (like iterator-
  shaped `Take`): read **and `commitTo` under the lock**, then release and yield —
  so they commit-on-read (at-most-once) and are safe for concurrent cooperating
  readers. `Drain` is bounded by a `writeOff` snapshot; `Follow` waits via
  `waitLocked` (which lives on Queue, called as `r.w.waitLocked`). The locked
  half lives in `Reader.next`, which takes the mutex with a **`defer` unlock** — the
  old hand-unlocked loop held `w.mu` across the user's `UnmarshalFunc`, so a panic
  there left the whole queue deadlocked. A failed read or commit ends the iteration
  and is stored in `r.err` for `Reader.Err` (an `iter.Seq` cannot carry it), and a
  failed commit stops *without* yielding, so the item replays instead of being
  handed out as consumed.
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
  instead. [faults_test.go](faults_test.go) pins the ordering itself — build with
  `-tags diskqueue_faults` — and `TestAppendOrdersDataBeforeHeader` fails if the
  data fsync is removed. The recovery-fault tests do NOT pin it: they forge
  on-disk residue and check what a reopen makes of it, which is a different (also
  valuable) property. Don't collapse it to one fsync on the per-op path.
- **`append` advances the cursors only once the record cannot be un-published.**
  Order: `writeRecord` → (per-op) data `fsync` → advance `size`/`written`/`writeOff`/
  `nWritten` + `header()` → `writeHeader` → (per-op) header `fsync`. A failure before
  the advance leaves the store untouched; a failed `writeHeader` rolls the advance
  (and the header bytes) back, because the record is invisible to a reopen. Only the
  *final* fsync failure leaves the record in place — the header is in the page cache,
  so it is real to everything short of a power loss — and that path poisons the
  store, which is what `Add`'s doc comment promises. Moving the advance back above
  the writes reintroduces "Add failed but the item is queued".
- **Dirty tracking.** Each `dataFile` has a single `dirty` bool meaning *has
  unsynced bytes*, **not** *is open*: `writeRecord`/`writeHeader` set it (page-cache
  writes not yet fsync'd); `flushFile` fsyncs and clears it, is a no-op for a clean
  file, and **reopens** a dirty file whose handle was evicted (`writeHeader` does the
  same). Eviction therefore leaves `dirty` set — clearing it, as the old `noSync`
  path did, made a later explicit `Sync` skip the file forever. Batched
  `append`/`commitTo` and `noSync` `writeHeader` the record/header into the page
  cache (so a reopen-via-`load` and `readFileHeader` see them) but defer the fsync;
  the per-op path fsyncs inline and clears `dirty`. `flushFile`/`flushBatch`/
  `sync`/`close`/`evictOpen` fsync only dirty files and skip clean ones — a file
  only read since its last flush is closed with no fsync. Under `noSync`, eviction
  just clears `dirty` and relies on kernel writeback (the page-cache bytes survive
  the handle being closed).
- **Lazy open.** Files open on demand via `ensureOpen` (`read`, `commitTo`, and
  `append` for the active file); `evictOpen` closes the LRU handle beyond
  `maxOpenFiles`, never the active or just-opened file, fsyncing a dirty victim first
  (a clean victim is closed without fsync). `df.f == nil` means closed —
  `flushFile`/`sync`/`flushBatch`/`close` skip such files; `df.hdr` stays resident
  so accessors still work and a later `ensureOpen` just reopens the handle.
  `createFile` writes (and, unless `noSync`, fsyncs) the fresh header so a
  cycled-but-empty segment is a valid file on disk.

## Gotchas

- `Take`/`Drain`/`Follow` advance `headOff` and commit; `Reserve` advances
  `headOff` without committing (so `Empty()` can be true while `Count() > 0`).
- `Reader.Requeue` moves the head record to the tail: it appends *first* and
  commits the head *second*, because the reverse loses the record outright if the
  append fails. The consequence is that a failed commit after a successful append
  leaves the record twice, which at-least-once already permits; a failed append
  moves nothing (`rewindHead`). It breaks FIFO for that record by design, and like
  `Skip` it acts on the **shared** head, not on a record the calling Reader holds.
- `Stats().UnsyncedBytes` counts record bytes appended on the *deferred* paths
  only (`noSync`, `batched`) — the per-op path fsyncs before `append` returns, so
  it stays 0 there. It is incremented after `writeHeader` succeeds, so a rolled-back
  append never contributes, and cleared only by a flush that covered every file
  (`flushBatch`, `sync`), so a partial failure over-reports rather than under-reports.
- `Take`/`TryTake` can return **a value and a non-nil error**: the read succeeded
  and the commit did not. Callers that check `err` first simply let the item replay,
  which is safe; don't "fix" it into dropping the value.
- `Reader.read` snapshots `headOffset()` and calls `rewindHead` when `unmarshal`
  fails, so a decode error leaves the record at the head instead of consuming it
  into thin air — the same stance strict mode takes for a corrupt record. This means
  a permanently-failing codec re-reads the same record; that is intentional, and
  `Reader.Skip` is the sanctioned way past it. Do *not* route decode failures into
  `skipCorruptSegment`: a codec bug is not disk corruption, quarantining a whole
  segment for it discards intact data, and it would make `Stats().Corruptions` — the
  metric operators watch for real data loss — lie.
- `commitTo` quarantines like the read path: a record it cannot frame moves the
  cursor past the whole segment instead of freezing it forever (which would also
  stop all reclamation, so the disk fills behind the stuck cursor).
- `commitTo` finalizes the outgoing file's header *before* `ensureOpen` opens the
  next one: with a small `maxOpenFiles`, opening the next segment can evict the one
  whose header is about to be written. `writeHeader` also self-heals a nil handle,
  so this is belt and braces — but the ordering avoids reopening a file just to
  write 64 bytes.
- Offsets are byte positions, monotonic within a session, not stable across a
  reopen (head resets to the recovered commit cursor).
- I/O is `os.File` `ReadAt`/`WriteAt`/`Sync` plus a directory `fsync` (`fsyncDir`)
  for durable creates/removes, `syscall.Fallocate` for preallocation and
  `syscall.Flock` for the directory lock — all stdlib, so still no
  `golang.org/x/sys` dependency. The platform-specific bits live in the three
  build-tagged file pairs; every `GOOS` in `go tool dist list` must keep compiling
  (`GOOS=windows go build ./...` etc.).
- The fault-injection helpers in `robust_test.go` work by swapping `df.f` behind the
  store's back, and rely on `ensureOpen` *not* reopening a file whose `f` is
  non-nil. If that ever changes, those tests silently stop injecting anything.
