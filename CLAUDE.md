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
  `startFresh`, `abortedCreate`, `salvageTornHeader`/`countVerified`, `dropSegment`,
  `removeFile`, `readHeader`.
- [store.go](store.go) — the `store`: the `[]byte`-only file backend (ReadAt/WriteAt). Keeps
  everything whose ordering is the invariant — the handle LRU, the segment lifecycle, the
  durability policy, `append`, the read/quarantine path, `commitTo` and the gauges. These are
  deliberately *not* split further: pulling `writeHeader`/`flushFile` away from `append` and
  `commitTo` would separate the calls whose order is the whole point (see the
  data-before-header invariant below).
- [diskqueue.go](diskqueue.go) — the generic `Queue[T]` writer/owner (Add/AddWait/AddBatch,
  Empty/Count/Size, Stats/Rewind/Sync/Err/Close, NewReader) on top of `store`,
  including the group-commit leader/follower machinery (`addDurableLocked`,
  `leadFlushLocked`, the quiesce/space signals).
- [reader.go](reader.go) — `Reader[T]`: all consume ops (TryPeek/Reserve/Take/Commit/
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
  Those notes name the block rather than numbering it: line numbers in them have gone stale
  before, which is worse than no reference at all.
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
go test -run=^$ -bench=BenchmarkAddTake -benchtime=1s ./...   # 0 allocs/op (NoSync path)
go test -run=^$ -bench='BenchmarkAddBatch|BenchmarkAddParallel' -benchmem ./...  # per-op path
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
  process, which is exactly what this library must never do. Bound the **whole frame**, not
  just the payload length: `avail` itself can exceed `maxInt` on a 32-bit build, so
  `v <= maxInt` is not enough and `n+int(v)+checksumSize` wrapped anyway (reproduced on
  `GOARCH=386` with a segment above 2 GiB). `commitTo` additionally
  refuses any decoded `next` that is not strictly forward and within `writeOff`.
- **The read cursor may never lag the commit cursor.** `commitTo` drags `headOff` up
  to `commitOff` before `dropCommitted`; otherwise a `Commit` past the read cursor
  reclaims the file `headOff` is standing in and every later read addresses a
  deleted file. `Reader.Commit` also rejects an offset beyond `headOffset()`.
- **Zero-alloc hot path.** `append` frames the record in the reused `s.writeBuf`
  and `WriteAt`s it; `Add`/`AddWait` serialize through the pooled `marshalBuf`
  (`w.bufs`, so codecs run concurrently across producers) and `AddBatch` through the
  reused `w.scratch`, both with the append-style `MarshalFunc`. Every reused buffer is
  released past `segmentSize` by `trimOver` at its own site — `putBuf` for the pool,
  `dropOversizedScratch` for the batch scratch, `Reader.trimScratch` for the reader copy,
  `dropBlock` for `readBuf`. `Reader.read` copies the payload (a slice of the
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
- **The per-record paths must not become O(segments).** Three were, each under the queue
  mutex: `dropCommitted` walked the whole live set per reclaim although reclaimable files are
  a strict *prefix* (it now breaks at the first survivor and splices the tail); `commitTo`
  rebuilt the header's 56-byte xxhash for every record crossed although the bytes are written
  once per file (`persistCommit` builds them now — that halved `BenchmarkBatchCommit`); and
  `Stats()` summed every segment's capacity, costing 391 µs at 60k segments, so `s.diskBytes`
  is maintained instead — update it at **every** `s.files` append (`cycle`, `startFresh`,
  `load`) and in `dropCommitted`. `flushBatch`/`sync` still walk the whole set, deliberately:
  a `df.dirty` test per file is cheaper than a second intrusive list.
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
- **Recovery (`load`) reads no records — with exactly two licensed exceptions,
  both confined to segments already proven damaged** (`surviveCount` for a
  truncated segment, `countVerified` for a torn header — see below), so every
  healthy open still costs one 64-byte pread per segment. Reopen preads each file's 64-byte header
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
- **Recovery is not finished until its conclusions are durable.** Both passes above
  correct a header that could not be believed — the truncation clamp rewrites the write
  cursor, the reconciliation rewrites the committed count — and a correction that lives
  only in memory is believed *again* by the next open. So `load` ends with a write-back
  loop that stamps `setWriteCursor`/`setWrittenCount`/`setCommitCursor`/`setCommittedCount`
  into every segment whose header does not already say them, with the commit cursor
  reconciled against the recovered *global* cursor exactly as the counts are. Two failures
  this closes, both of which lost acknowledged records with every loss counter reading zero:
  a truncated segment used to keep its old commit cursor, which later appends grew the
  segment past and the next open then believed; and a segment whose committed count the
  reconciliation overrode kept the stale figure, which became authoritative once the
  segments ahead of it were reclaimed and it became the leading one. On a healthy open every
  field already agrees, so the guard never fires and reopening still costs one 64-byte pread
  per segment and no writes.
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
- **An unfinished create is not corruption — but only at the tail.** BOTH the
  zero-length and the all-zero shape are gated on being the highest-numbered segment, for
  the reason `abortedCreate` gives: segments are created one at a time at the end of the
  sequence and the number advances only once the create has succeeded, so a zeroed file with
  a live sibling *above* it was never an unfinished create. It is a segment that lost every
  byte it had, and it takes the ordinary corruption path — counted, and owed one
  `ErrCorrupt`. The zero-length branch used to skip that gate, so a middle segment truncated
  to nothing was unlinked in silence. A create interrupted between the link
  and the header write leaves a zero-length file; one interrupted a step later,
  between the reservation and the header write, leaves a *full-length* file of
  zeros. Neither can hold a record, so `loadFile` removes both and raises no loss
  event — booking the second as damage reported a whole segment (8 MiB at the
  default geometry) of phantom loss after every power cut. The second case is not
  self-evident from the file, so `abortedCreate` proves it: the header is all zeros,
  the data region is all zeros (nothing acknowledged can hide there — a record's
  bytes and the header publishing them are fsync'd together, and an all-zero frame
  is an empty payload whose checksum word would have to be xxhash's nonzero digest
  of nothing), and the segment is the tail, the only position a create can occupy. A
  scan that cannot finish leaves the corruption verdict standing; the accounting may
  over-report a loss, never under-report one. Reordering `createFile` to write the
  header before reserving would not settle it — the reservation is metadata and can
  reach disk while the header bytes are still in the page cache. Together with
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
- **A record reported destroyed must also be retired.** `takeHead`'s checksum-mismatch arm
  drops one record and advances `headOff`, and nothing else would ever move the commit cursor
  past it: every consume op returns on the error without committing. Left alone, a damaged
  record at the tail kept `Count()` above zero for good, pinned its segment against
  reclamation, and was re-read and re-reported on every reopen — one `ErrCorrupt` and one
  `LostRecords` per restart, forever. So the arm commits past it, but *only* when the commit
  cursor stood exactly there; with records reserved behind it the cursor may not jump,
  because that would retire work no consumer has acknowledged. Those cases heal on their own,
  since the eventual `Commit` walks across the record like any other — its framing is intact,
  only the payload failed.
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
- **A torn header rewrite salvages; other damaged headers drop.** Every commit
  rewrites the 64-byte header in place, so a power cut can tear it — and that
  used to be the one way a routine, successful operation could destroy a whole
  segment of already-durable records. The torn-rewrite signature is *magic and
  version intact, checksum bad* (neither field changes between header images,
  so both survive any interleaving); on that signature `salvageTornHeader`
  rebuilds the header from `countVerified`, a bounded walk that trusts only
  frames whose payload checksum verifies — a frame that merely decodes is what
  zero fill does. The commit position is unrecoverable, so the salvaged
  segment's cursor resets to its start and everything from it forward replays
  (at-least-once); one corruption event is booked with **no** bytes (the bytes
  past the clamp are usually fill, and LostBytes is a lower bound). The rebuilt
  capacity comes from the file's own length floored at `segmentSize`, so the
  repair pass can only ever extend the file, and a walk that cannot run leaves
  the drop verdict standing.
- **`load` drops a damaged segment wherever it sits**, not only at the tail, and
  the open succeeds. `dropSegment` splits the two reasons apart: a *damaged*
  header (bad magic, short read, or a bad checksum the salvage above could not
  redeem) is data loss — counted in `lostSegments`/`lostBytes`, owed one
  `ErrCorrupt`; an *unknown version* (`knownVersion`) is a format change whose
  data the design accepts dropping — counted in `foreignSegments` and silent. A
  zero-length file — or a tail whose every byte is zero — is an aborted create
  and is removed with no event at all. Duplicate segment numbers from unpadded
  strays (`data.1` beside `data.00000001`) are collapsed to one load.
  Numbering always resumes past the highest seen, never at 1.
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
- **Staged records are invisible until their span publishes.** The per-op
  append path (Queue-level: `addDurableLocked`/`leadFlushLocked`/
  `publishBatchLocked` over the store's `stagePending`/`takeSpan`/
  `publishSpan`) writes record bytes past the published extent and tracks them
  only in `pendingBytes`/`inFlight`: `df.hdr`, `af.size`, `writeOff` and
  `nWritten` move at publication, never at staging. That single rule is what
  lets the flush leader release the queue mutex during its fsyncs — readers,
  `commitTo`, `Stats` and a crash all see the published extent only, so
  nothing can deliver, commit or recover a record whose bytes are not yet
  durable. A header write racing the data fsync is the torn state the solo
  append's ordering exists to prevent; staging keeps the header out of reach
  by construction.
- **`AddBatch` stages on EVERY policy, not just per-op.** The deferred policies ran the
  plain `append` loop, which writes a 64-byte header per record — so the method whose whole
  purpose is amortization cost two pwrites per record on exactly the policies chosen for
  throughput, against 1.01 on per-op. One staged path now serves all of them, and
  `publishBatchLocked` branches only on whether the span needs its two fsyncs. The span still
  counts every record through `recordOps`, because `SyncEvery` is documented as a count of
  *writes* and collapsing a batch into one tick would silently widen the power-loss window it
  exists to bound. Every early return in that loop either publishes what is staged or
  discards it — the `ioErr` arm used to do neither, leaving a span no leader would settle and
  wedging every later quiesce, `Close` included, forever.
- **Every staged record lives in the active file** — cycling, `AddBatch`,
  `Requeue`, `Sync` and `Close` quiesce first (`waitFlushQuiescedLocked`) — so
  a failed publication discards exactly one contiguous tail
  (`discardStaged`/`unpublishSpan`) and the in-memory view snaps back to the
  header that never changed. The span failure arms mirror the solo append's:
  data-fsync failure latches with nothing published, header-write failure
  discards with nothing latched, header-fsync failure latches with the span
  real in the page cache. `faults_test.go` pins the ordering for all three
  shapes (solo, group, batch) under the same injection-point names.
- **Group commit shares the fsyncs, not the contract.** A solo Add leads a span
  of one and allocates nothing (the `flushGroup` is created only by a follower
  that arrives mid-flush). Note which benchmark proves what: `BenchmarkAddTake` and
  `TestAddTakeZeroAlloc` both run under `NoSync`, so they guard the *plain* append path
  and never execute `addDurableLocked`/`stagePending`/`leadFlushLocked`/`publishSpan`.
  The per-op path is guarded by `BenchmarkAddBatch` and `BenchmarkAddParallel`, also
  0 allocs/op; measured directly, a solo per-op Add+Take is 0 allocs/op, a per-op
  `AddBatch` is 0, and 8 concurrent producers cost ~0.27 allocs/op — the one
  `flushGroup` and channel per flush window, which is the design. Followers wait on their group's `done` channel and take the
  span's verdict; the leader drains spans until nothing is pending, then
  clears `flushing` and wakes the quiesce waiters. After ANY wait that
  released the lock (quiesce, space, follower), re-check `closed` — staging
  onto a closed store would reopen handles Close just released. `Requeue`
  quiesces BEFORE its `takeHead`, because its rotation must be atomic under
  one continuous lock hold (its `rewindHead` on failure may not undo another
  reader's progress). The leader deliberately leaves `af.dirty` set after its
  header fsync — a commit may have rewritten the header while the lock was
  away, and clearing it would skip that write's flush.
- **Quiescing needs a barrier, and it must not be `waitFlushQuiescedLocked`.** The flush
  leader leaves its span loop only when no follower staged during the last span, so a steady
  producer stream keeps `flushing` true across hundreds of consecutive spans and everything
  that quiesces — `Sync`, `AddBatch`, `Requeue`, `Close` — waits behind the producers rather
  than behind the flush. Measured before the fix: a worst-case `Sync` of 36.9s at 8
  producers, one leader driving 703 spans; `AddBatch`'s tail at 1.92s. So
  `waitFlushQuiescedLocked` holds `quiesceWant` for the duration of its wait, and
  `addDurableLocked` parks arriving producers on `quiesceRelease` *before staging* — a
  staged record is what keeps the waiter's predicate true.

  The barrier has to be a SEPARATE channel. Sending producers into
  `waitFlushQuiescedLocked` instead livelocks: a producer whose predicate is already false
  returns immediately still holding the mutex, re-checks the barrier, re-enters, and spins on
  the lock — starving the very waiter it was meant to yield to. That was implemented and
  measured (30 Syncs failed to complete in 25s) before the current shape. Both fields are
  zero/nil when nobody quiesces, so the uncontended Add path is one int compare; a
  pathological tight `Sync` loop can hold `quiesceWant` above zero and invert the fairness,
  which is the deliberate trade.
- **Marshal runs outside the lock for Add/AddWait** (pooled `marshalBuf`, so
  codecs run concurrently across producers and a slow codec cannot stall
  consumers); `AddBatch` marshals under the lock through `w.scratch`. Both
  keep the no-callback rule: `MarshalFunc` must never call back into the
  queue.
- **`peekHead` previews, it never books.** `Reader.TryPeek` moves no cursor,
  pays down no `pendingCorrupt`, and its `ErrCorrupt` is uncounted — the
  consume op that steps past the damage books the event exactly once. Don't
  add accounting to the peek path; double-counting a loss is how operators
  stop trusting `Corruptions`.
- **Blocking waiters.** `waitLocked`/`signal` use a lazily-created `notify`
  channel, nil when nobody waits, so `Add` stays allocation-free. The same
  pattern runs the other two waits: `spaceNotify` (consume ops wake `AddWait`
  producers; every commit-capable Reader op signals it, spurious wakes are
  re-checked) and `flushDrained` (the flush leader wakes quiesce waiters).
  `Close` closes `notify` AND `spaceNotify`, then quiesces the flush before
  `st.close()` so a leader mid-fsync finishes on live handles.
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
  closes the handle with `dirty` still set (no fsync of the victim), so a later
  explicit `Sync` reopens and flushes it — which is what keeps
  `Stats().UnsyncedBytes` honest after `Sync` returns.
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
- I/O is `os.File` `ReadAt`/`WriteAt` for records and headers; every per-record and
  per-header flush goes through `datasync` — `syscall.Fdatasync` on Linux, `f.Sync()`
  elsewhere and for `createFile`, the one write that changes metadata. Plus a directory
  `fsync` (`fsyncDir`) for durable creates/removes, `syscall.Fallocate` for preallocation
  and `syscall.Flock` for the directory lock — all stdlib, so still no `golang.org/x/sys`
  dependency. The platform-specific bits live in the
  build-tagged file pairs, sharing the `fdControl` EINTR-retry helper
  ([fdcontrol_unix.go](fdcontrol_unix.go)); every `GOOS` in `go tool dist list`
  must keep compiling (`GOOS=windows go build ./...` etc.).
- The truncation repair in `load` re-extends the file **best-effort** (like the
  active file's reservation): the clamped cursor is already durable, so a later
  open re-detects the short file with nothing new to book and retries — making
  it fatal would lock a full disk out of the queue that must be drained to free
  it. The header republish above it stays fatal, or the same loss would be
  booked on every open.
- **Deliberately out of scope** (decided, not merely missing — don't add them):
  a dead-letter quarantine for corrupt segments (they are unlinked, not moved
  aside) and any offline inspection/repair tool. Recovery is in-process only.
- The fault-injection helpers in `robust_test.go` work by swapping `df.f` behind the
  store's back, and rely on `ensureOpen` *not* reopening a file whose `f` is
  non-nil. If that ever changes, those tests silently stop injecting anything.
