# Repository guidance

## Scope and workflow

`github.com/JohanLindvall/diskqueue` is a generic, durable FIFO queue for Go.
It uses local segment files, standard-library file I/O, and `cespare/xxhash/v2`
checksums. Keep the implementation pure Go with a small dependency footprint.

Read [README.md](README.md) for an overview, [docs/guide.md](docs/guide.md) for
public behavior, and [CONTRIBUTING.md](CONTRIBUTING.md) for development setup.
Keep changes focused and update the package comments and guide when public
behavior changes.

Recovery belongs in the running library. Do not add an offline inspection or
repair tool, or a dead-letter quarantine for corrupt segments.

## Source map

| File | Responsibility |
| --- | --- |
| [diskqueue.go](diskqueue.go) | Queue API, producers, options, statistics, group commit, sync, and close |
| [reader.go](reader.go) | Reading, acknowledgement, and consuming iterators |
| [reserve.go](reserve.go) | In-memory reservation ledger and contiguous acknowledgement |
| [store.go](store.go) | Segment lifecycle, handle LRU, append, commit, and durability ordering |
| [header.go](header.go) | On-disk header layout, accessors, and checksums |
| [record.go](record.go) | Record framing, checksums, buffer handling, and length bounds |
| [recovery.go](recovery.go) | Opening existing queues and repairing damaged storage |
| `datasync_*`, `prealloc_*`, `lock_*`, `syncdir_*`, `fdcontrol_*` | Platform-specific file operations and fallbacks |
| [example_test.go](example_test.go), [examples/](examples/README.md) | Executable usage examples |

Keep the operations whose ordering establishes durability close together.
Use the source files as the authority for byte offsets and internal constants.

## Checks and conventions

- Run `make check` for Go behavior changes. It checks formatting, vet, lint,
  normal and fault-injection tests with the race detector, and runnable examples.
- For documentation and comment-only changes, check links, references, and
  formatting. Run examples if their code changes.
- Add a focused regression test for a behavior change. Storage tests should
  verify state after reopening as well as state in memory.
- Use `make bench` for performance work and `make fuzz FUZZTIME=30s` for a
  bounded store fuzz run. Do not compare benchmarks with different sync policies.
- Preserve the Go version floor in `go.mod`. Keep the Makefile linter pin and
  CI version aligned. Platform-specific changes need their fallback builds.
- Format Go with `gofmt`. Start every Go file with
  `// SPDX-License-Identifier: MIT` and a blank line, above any build constraint.
- Keep the README quick start identical to `examples/basic/main.go`, apart
  from the SPDX comment. The Playground URL is a snapshot of
  `Example_recovery`; update the shared program if that example changes.

## Storage and cursors

Segments have a checksummed 64-byte header followed by records framed as
`uvarint(length) || payload || xxhash64(payload)`. Records do not span segments.
A record larger than `SegmentSize` gets its own segment, marked as oversized.
Read geometry from the validated header; file length alone cannot distinguish
oversized records, configuration mismatches, and truncation.

Preserve `commitOff <= headOff <= writeOff` for the published stream:

- `writeOff` is the published tail.
- `headOff` is the shared read cursor, reset to `commitOff` on open or rewind.
- `commitOff` is the persisted retirement position.

Mutate headers through `dataFile.header` and its modifiers so their checksums
stay valid. Keep per-file counts, global counts, and cached disk capacity
consistent when publishing, committing, recovering, or reclaiming segments.
Offsets belong to the current recovered log; callers must not persist them
across reopening or reuse reservations after a rewind.

## Durability and publication

- Under per-operation durability, persist record data before publishing its
  header: record write, data sync, header write, header sync. Group commit may
  share these syncs, but each successful `Add` must cover its own record.
- Staged records remain outside the published extent. Do not advance header
  cursors, visible sizes, or record counts until publication.
- A failed record or header write rolls back unpublished state. A failed data
  sync latches a durability error without publishing. A failed final header
  sync leaves the record published in the page cache and latches the error.
- `AddBatch` stages under every sync policy. Every exit must publish or discard
  staged data. If a codec panics, discard the staged tail and re-panic.
  Count each record toward `SyncEvery` and respect boundaries within a batch.
- Quiesce pending publication before cycling segments, batch operations,
  requeueing, explicit sync, or closing. Requeue must quiesce before reading
  the head so rotation and rollback retain one continuous lock hold.
- `NoSync` disables per-operation syncing; explicit `Sync` and `Close` still
  flush. `SyncInterval` is the timer backstop for batched durability.
- `dirty` means unsynced data, independent of whether the handle is open.
  Reopen dirty evicted files when flushing. Eviction under `NoSync` must
  preserve dirty state.

## Concurrency and resource ownership

- Queue state is protected by `w.mu`. A `Reader` belongs to one goroutine.
  Copy payloads from the store's shared read buffer into reader-owned scratch
  under the lock before decoding. Returned aliases remain valid until that
  reader's next read.
- `Add` and `AddWait` marshal outside the lock using pooled buffers;
  `AddBatch` marshals under it. Codecs must be safe for their calling context
  and must not call back into the queue or its readers.
- Use deferred unlocks around caller codecs and iterator operations so a panic
  cannot leave the queue locked. Release the mutex before yielding an item.
- After waits that release the mutex, re-check `closed` before accessing the
  store. Close must wake readers and producers and wait for active flushes.
- Preserve the separate `quiesceWant` / `quiesceRelease` producer barrier.
  Producers must wait before staging while a quiescer needs the flush to finish.
  `AddWait` cancellation covers waiting for capacity, not this barrier.
- `syncMu` serializes explicit flushes. Acquire it before `w.mu` when both are
  needed. Close stops the background syncer before acquiring `syncMu`.
- Off-lock sync opens and pins a bounded chunk of files at a time. Honor
  `flushChunkSize`, protect pinned files from eviction and reclamation, and
  skip files reclaimed between chunks. Reacquire `w.mu` through a defer.
- Clear a flushed file's dirty flag only if its `writeSeq` is unchanged.
  Reconcile unsynced gauges against the snapshot and `flushEpoch`, preserving
  writes made during the flush and any intervening flush.

## Consumption and acknowledgement

- `Reserve` advances the read cursor without committing. `Ack` marks one
  reservation and advances the durable cursor only across a contiguous
  acknowledged prefix. Acknowledgements behind a gap may replay after a crash.
- `Commit` retires the specified position and everything before it; it must
  reject offsets beyond the read cursor. Keep it distinct from per-record
  acknowledgement for competing workers.
- Reconcile the reservation ledger from `commitOff`, and clear it on rewind.
  Preserve acknowledgement idempotence and reject unknown outstanding offsets.
- `Take`, `TryTake`, `Drain`, and `Follow` commit before returning or yielding
  an item. `Take` can return both a value and a commit error. Iterators report
  errors through `Reader.Err`; a failed commit must not yield the item.
- `Drain` stops at a snapshot of the write cursor. `Follow` waits for new work.
  Both continue past reported corruption and retain an error for inspection.
- `TryPeek` moves no cursor, consumes no pending corruption report, and books
  no loss counters.
- Decode failures leave the record at the head and wrap `ErrCodec`. Check
  `ErrCodec` before `ErrCorrupt`, since a caller's decoder can wrap either.
- `Skip` and `Requeue` act on the unread shared head and retire it through the
  reservation ledger. Requeue appends before retiring the original, preserving
  the original timestamp. A failed append must restore the read cursor.

## Error handling and recovery

Keep these error classes separate:

| Condition | Required behavior |
| --- | --- |
| Corrupt framing, checksum failure, or missing segment | Advance past the damage, report `ErrCorrupt`, and account for loss |
| A file cannot be opened, read, or written | Return the underlying error without treating inaccessible bytes as corruption |
| A sync fails | Latch `ErrIO` through `failIO`, preserving the underlying error |
| A codec rejects a record | Return `ErrCodec` and leave the record available |

Propagate I/O errors. Further writes, commits, and syncs must report a latched
durability failure; reads remain available. Document any deliberate ignored
error and preserve its compensating accounting.

- Healthy recovery reads headers without scanning records. Restrict record
  walks to proven truncation (`surviveCount`) and torn headers (`countVerified`).
  Detecting an all-zero aborted create also requires checking its contents.
- A failed recovery read must fail the open, without deleting the segment or
  presenting an incomplete walk as a successful repair.
- A header with valid magic and version but a bad checksum is eligible for
  salvage from verified records. Its commit position cannot be trusted, so
  replay from that segment onward.
- Reconcile committed counts against the recovered global cursor. Persist
  corrected cursors and counts before returning from recovery. Re-extending
  truncated preallocation is best-effort; publishing a repair is not.
- Treat an empty or all-zero file as an aborted create only at the highest
  segment number. The same shape in the middle of the log is counted damage.
- Count unknown format versions separately in `ForeignSegments`. Resume
  numbering beyond the highest segment number observed.
- With intact framing, a bad payload checksum costs one record. Broken
  framing or a vanished file abandons the remaining segment. Retiring damage
  must not skip unacknowledged reservations.
- Report each corruption event once. Use `pendingCorrupt` for events detected
  during open, and make pending reports visible to blocking readers.
- Book lost segments only when bytes were lost. Persist quarantine headers
  before applying in-memory retirement, including the `forceCommitAll` path.
- Validate the whole record frame in `uint64` / `int64` before converting to
  `int` or slicing. Bounds must hold on 32-bit builds, and decoded positions
  must advance without crossing the published end.

## Capacity, reclamation, and performance

- `MaxSegments` limits file count; `MaxBytes` limits uncommitted record bytes.
  Preserve transient `ErrFull` versus permanent `ErrRecordTooLarge`.
  Backlog limits are admission policy and must not block recovery.
- Reclaim complete segments from the leading prefix on writes and commits.
  Keep the active file during commit reclamation and protect pinned files.
  If unlink fails, retain the file and its capacity in accounting and retry.
- Finalize a segment's commit header before opening the next segment, since
  opening can evict the previous handle.
- Keep per-record work independent of the total number of segments. Maintain
  cached disk capacity, build commit headers per segment, and bound handle use
  with the LRU.
- Reuse serialization and read buffers. Release oversized buffers through
  the existing trimming helpers on both success and failure.
- `Stats` distinguishes backlog gauges, in-flight and unsynced bytes, lifetime
  operation counters, and loss counters. Failed or partial flushes must not
  report unsynced data as durable.
- Preserve the CI allocation checks for `BenchmarkAddTake`,
  `BenchmarkAddTakeCommit`, `BenchmarkRead`, and `BenchmarkBatchCommit`.
  Inspect `B/op` as well as `allocs/op` for durable batch and parallel writes:
  amortized group allocations can round down in `allocs/op`.

## Test coverage to preserve

- [faults_test.go](faults_test.go) verifies write/sync ordering and injected
  failure paths. Run it with the `diskqueue_faults` build tag.
- Recovery tests verify on-disk outcomes after reopen; they complement the
  ordering tests.
- [aliasing_test.go](aliasing_test.go) checks reader isolation and value
  lifetime; run the race detector for concurrency changes.
- [progress_test.go](progress_test.go) checks that consuming operations
  advance past corruption they report.
- `cover_*_test.go` exercises internal error paths. Keep `UNCOVERED` comments
  specific to branches and limitations rather than line numbers or percentages.
- Descriptor-swapping fault helpers rely on `ensureOpen` preserving non-nil
  handles. Update those helpers if handle-management behavior changes.
