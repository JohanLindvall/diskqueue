# Disk buffering in the wild

How the observability agents implement their disk buffers, and where `diskqueue` sits
among them.

Every agent that ships logs or metrics somewhere eventually grows a disk buffer, because
the network goes away and memory does not scale. They have all solved the same problem,
and they have solved it *very* differently — including on questions you would expect to be
settled, like "is there a checksum" and "is it durable".

This is a comparison of **mechanism**, not of products. `diskqueue` is a ~1,400-line Go
library; Fluent Bit is a whole agent. Comparing them wholesale would be silly. What is
comparable is the layer underneath: how bytes reach the disk, what happens when they come
back wrong, and what you can see from outside.

> **Method.** Every claim below was read out of primary sources — the actual source files
> and official docs — and then independently fact-checked against those sources a second
> time. Where the two passes disagreed, the source won. Snapshot date **2026-07-29**;
> versions are named where they matter. Where something could not be confirmed from a
> source it is marked *unconfirmed* rather than guessed. These projects move; re-check
> before betting on a number.

---

## The cast

| Project | The disk buffer | Language | Status |
| --- | --- | --- | --- |
| **Fluent Bit** | `storage.type filesystem`, on the vendored `chunkio` library | C | Current, stable |
| **Fluentd** | `buf_file`, `buf_file_single` | Ruby | Current, stable |
| **Promtail** | WAL over Prometheus `tsdb/wlog` | Go | **Dead** — EOL 2026-03-02, source deleted from Loki |
| **Grafana Alloy** | `loki.write` WAL (same `wlog`) + `otelcol.storage.file` | Go | WAL **experimental**, default off; storage ext. public preview |
| **OpenTelemetry Collector** | exporterhelper `sending_queue` + `file_storage` extension (bbolt) | Go | Queue stable, extension **beta** |
| **Elastic Beats / Filebeat** | `queue.disk` (`libbeat/publisher/queue/diskqueue`) | Go | GA since 7.15 |
| **Elastic Beats** | `queue.spool` on `go-txfile` | Go | **Removed** (was beta 6.3–7.x) |
| **Logstash** | Persistent Queue, `queue.type: persisted` | Java | GA, but **off by default** |
| **Vector** | `buffer.type = "disk"` (disk buffer v2) | Rust | Stable since 0.22 |
| **Telegraf** | `buffer_strategy = "disk"` on `tidwall/wal` | Go | **Experimental**, since 1.32 (2024) |
| **NSQ** | `nsqio/go-diskqueue` | Go | Current, low activity |
| **rsyslog** | disk / disk-assisted queue (`queue.filename`, `.qi`) | C | Current |
| **rsyslog** | `segmentedDisk` store | C | **Experimental, unreleased** — `main` only |
| **syslog-ng** | `disk-buffer()` | C | Current |
| **diskqueue** | this library | Go | Unreleased |

---

## Question 1 — is there a checksum, and what does it cover?

| System | Algorithm | Covers | Per |
| --- | --- | --- | --- |
| **diskqueue** | xxhash64 ×2 | record payload; segment header `[0:56]` | record + segment header |
| Fluent Bit / chunkio | CRC-32 (IEEE) — **off by default** | chunk content section only | whole chunk |
| Fluentd `buf_file` | **none** | — | — |
| Fluentd `buf_file_single` | **none** | — | — |
| Prometheus WAL (Promtail, Alloy) | CRC32-**Castagnoli** | fragment payload — **not** the 7-byte record header | fragment |
| Alloy marker file | CRC32-**IEEE** | first 10 of 14 bytes | marker |
| OTel Collector queue | **none** | — | — |
| ├ `file_storage` / bbolt | FNV-1a 64 | **meta pages only** — data pages have none | 2 pages in the file |
| Filebeat `queue.disk` | CRC32-IEEE | frame length + payload; **not** the segment header | frame |
| Beats legacy spool (txfile) | FNV-1a 32 | **meta pages only** | 2 pages in the file |
| Logstash PQ | CRC32 | element **data only** — not the seqNum, not the length | element + checkpoint |
| Vector disk v2 | **CRC-32/IEEE** (`crc32fast`) | record id + payload | record |
| Telegraf (`tidwall/wal`) | **none** | — | — |
| NSQ `go-diskqueue` | **none** | — | — |
| rsyslog classic | **none** | — | — |
| rsyslog `segmentedDisk` | CRC-32C | segment header, record header, payload, footer, state slot | everything |
| syslog-ng | **none** | — | — |

Six of them — Fluentd, the OTel Collector, Telegraf, NSQ, rsyslog's classic queue and
syslog-ng — have **no integrity check at all** on the data they persist. Seven, if you
count the removed Beats spool, whose checksums cover only its two meta pages. Their only
defence against a flipped bit is that the payload happens to stop parsing — which msgpack,
protobuf and gob frequently do not, since a flipped bit inside a string field is still a
valid string.

Two things worth flagging:

- **Vector's docs and code disagree.** Every doc comment and the design-constraints list
  say the record checksum is CRC32**C** (Castagnoli); `record.rs` imports `crc32fast`,
  which is CRC-32/IEEE. Functionally equivalent for detection, but if you are writing a
  tool to read Vector's buffers, believe the code.
- **A checksum that skips the length field is weaker than it looks.** Prometheus's WAL
  covers the payload but not the 7-byte header that says how long the payload is;
  Logstash's element CRC covers the data but not the length prefix. `diskqueue` has the
  same property (the length prefix sits outside the hash) and gets away with it because a
  corrupted length moves the checksum window and the comparison fails anyway — but it does
  mean the *framing* is trusted, not verified. [spool](https://github.com/JohanLindvall/spool),
  a sibling project, folds the length bytes into the hash for exactly this reason.

---

## Question 2 — is it durable by default?

"Durable" here means one thing: **the write survives a power cut**, not just a process
crash. Every system in this table survives `kill -9`, because the page cache does that for
free. Surviving the machine going away requires `fsync`.

| System | fsync by default? | Cadence |
| --- | --- | --- |
| **diskqueue** | **Yes** | Two fsyncs per record (data, then the header that publishes it); `SyncEvery: N` to batch, `NoSync` to opt out |
| Telegraf | **Yes** | `buffer_disk_sync` defaults true, per write |
| Beats legacy spool | **Yes** | Per transaction, two-phase meta page |
| Filebeat `queue.disk` | **Yes** | Once per write batch, **not configurable** |
| Logstash PQ | Partly | `queue.checkpoint.writes` — msync + checkpoint every **1024** writes by default, no wall-clock backstop |
| Fluent Bit | **No** | `msync(MS_ASYNC)`; `storage.sync full` → `MS_SYNC`. No `fsync` anywhere in chunkio's Unix path; Windows `full` does call `FlushFileBuffers` |
| Fluentd | **No** | No fsync, and **no option to enable one** |
| Prometheus WAL / Promtail / Alloy | **No** | Page-cache writes; fsync only on 128 MiB segment rotation and on close |
| OTel `file_storage` | **No** | `fsync: false` is the default → bbolt `NoSync: true`, documented upstream as "THIS IS UNSAFE" |
| Vector | **No** | 256 KiB buffered writer → page cache; real fsync only on flush intervals/rotation |
| NSQ | **No** | `syncEvery` (2500 msgs) / `syncTimeout` (2s) |
| rsyslog classic + segmented | **No** | `queue.syncQueueFiles` defaults **off** |
| syslog-ng | **No** | No `fsync`, `fdatasync`, `msync`, `O_SYNC` or `O_DSYNC` anywhere in `modules/diskq` |

**Most "persistent" buffers in this space are not power-loss durable out of the box.** That
is a defensible choice — the fsync bill is brutal (see below), the buffer is usually a
transient hop, and many upstreams resend. But it is rarely the thing a user reads into the
word "persistent", and in one case the documentation actively says otherwise: syslog-ng's
admin guide states that with `reliable(yes)` messages "are persisted on the disk, and
survive syslog-ng OSE crash **or power failure**". There is no durability primitive in
`modules/diskq` that could implement the second half of that sentence.

For scale, `diskqueue` on one laptop NVMe (AMD Ryzen 7 8840HS, ext4), 256-byte records:

| Policy | ns/op | records/s |
| --- | ---: | ---: |
| `SyncEvery: 1` (fully durable per record) | 2 309 559 | ~430 |
| `SyncEvery: 10` | 119 069 | ~8 400 |
| `SyncEvery: 100` | 13 488 | ~74 000 |
| `SyncEvery: 1000` | 4 553 | ~220 000 |
| `NoSync` | 2 985 | ~335 000 |

Three orders of magnitude between "durable per record" and "durable per thousand". That
gap is the entire reason the table above looks the way it does. `diskqueue`'s per-record
figure is also about 2× worse than it needs to be, deliberately: it issues **two** fsyncs
per record — the payload, then the header that publishes it — so a power cut can only ever
truncate cleanly, never expose a published record whose bytes never landed.

---

## Question 3 — what does one bad byte cost?

This is the question nobody documents and everybody eventually meets. Sorted by blast
radius.

| System | One bad byte destroys | Then what |
| --- | --- | --- |
| **rsyslog `segmentedDisk`** | ~1 record | Skip-and-count, rescan from the next byte; `corruption.events/bytes/records/segments` counters |
| **diskqueue** | 1 record (payload damage) / 1 segment (framing damage) | Drop, count, return one `ErrCorrupt`, keep going |
| Logstash PQ (torn tail) | the tail after the tear | Truncate to the last good element, rebuild the checkpoint, continue |
| Vector | **the rest of that data file** | `roll_to_next_data_file()`; comment: "we're not sure the rest of the data file is even valid" |
| Fluent Bit | one chunk (~2 MB) | By default the corrupt file is **left on disk forever** (`storage.delete_irrecoverable_chunks` defaults Off); a DLQ arrived in v5.0.0 |
| Fluentd | one chunk (up to 256 MiB) | Copied to a backup dir, then **unlinked**. But a torn tail is *unchecksummed*, so it is accepted and **flushed downstream as data** |
| NSQ | **one whole data file** (100 MiB at nsqd defaults) | Renamed `*.dat.bad`, never re-delivered. Any read error qualifies — including `open` failing |
| Filebeat `queue.disk` | one segment | Skipped at load and logged; the file is never read, never deleted, and its bytes do not count against `max_size` |
| Prometheus WAL `Repair` | the corrupt segment **and every segment after it** | `os.Remove`s all newer segments, truncates the corrupt one, continues |
| syslog-ng | **the entire disk-buffer file** | Renamed `<file>.corrupted`, a fresh empty file is started, `queued_messages` reset to 0 |
| rsyslog classic | **the entire queue** | `onCorruption=safe` (default): move everything to `<prefix>.bad.<timestamp>/` and start empty |
| OTel Collector | **the entire queue, silently** | Unreadable metadata → `"Failed getting metadata, starting with new ones"`, indices reset to 0. Every queued item is orphaned on disk: never delivered, never deleted, and the collector starts up looking healthy |
| OTel `file_storage` (bbolt) | **the process** | A damaged non-meta page has no checksum; `Page.FastCheck` panics. Default `recreate: false` has no `recover()`, so the collector **crash-loops** until an operator deletes the file |
| Telegraf | **the process, until a human intervenes** | `"wal file is corrupt, you have to manually delete the wal at %q and restart Telegraf"`. A read error later on `panic`s |
| Promtail | nothing — and everything | Retry forever. No marker, so it re-opens the last segment and re-reads from offset 0; a persistently corrupt record **livelocks the watcher** |
| Beats legacy spool | the whole spool | Both meta pages bad → `txfile.Open` fails → **the Beat does not start** |

Three failure shapes recur, and they are worth naming:

1. **Fail-closed.** Telegraf, the OTel file storage extension, and the old Beats spool
   refuse to run. Safe for the data still on disk, but it converts a corrupt buffer into an
   outage, and in Telegraf's case the documented remedy is "delete everything".
2. **Silent total loss.** OTel's metadata reset is the sharpest example: nothing crashes,
   nothing is logged above `Error`, no metric moves, and the entire backlog stops existing.
   syslog-ng and rsyslog classic do the same thing loudly.
3. **Livelock.** Promtail's watcher never advances past a bad record — the one outcome that
   is worse than both dropping it and crashing, because the queue is neither working nor
   visibly broken.

**Alloy is asymmetric and it is worth knowing which half you are in.** Replaying a *closed*
segment, a read error is swallowed with `"ignoring error reading to end of segment, may
have dropped data"` and the watcher moves on. Tailing the *active* segment, it retries
forever, exactly like Promtail. Same code, opposite behaviour, decided by whether the
segment is the newest one.

---

## Question 4 — what does a restart cost?

| System | Startup work | Proportional to |
| --- | --- | --- |
| **diskqueue** | One 64-byte `pread` per segment; **no record is read** | segment count |
| rsyslog `segmentedDisk` | One 512-byte state file + `lstat` probes | O(1) |
| NSQ | `Fscanf` five integers from a metadata file | O(1) |
| syslog-ng | mmap the 4096-byte header | O(1) |
| rsyslog classic | Deserialize the `.qi` property bag | O(1) |
| OTel + bbolt | Validate 2 meta pages, then re-enqueue not-dispatched items | ~O(1) + in-flight |
| Beats legacy spool | Validate 2 meta pages | O(1) |
| Vector | mmap'd ledger + validate the last record only | O(1) |
| Filebeat `queue.disk` | Read each segment header, **walk its frames to count them** | frame count |
| Logstash PQ | Read every checkpoint; recover the head page element by element | pages + head page |
| Fluent Bit | mmap **every** chunk and msgpack-validate it; with checksums on, re-CRC everything | total backlog bytes |
| Fluentd | `glob` + open every chunk; `buf_file` also reads every `.meta` | chunk count |
| Prometheus WAL / Alloy | **Full sequential replay** of checkpoint + segments | total WAL bytes |
| Promtail | *None* — starts at the newest segment | O(1), by not replaying |

That last row is not a compliment. Promtail's watcher begins at `lastSegment` with no
checkpoint and no marker, so **a restart skips whatever was still unsent in the older
segments**. Alloy fixed this with the `segment_marker` file; it is the single biggest
functional difference between the two, and a good reason the migration was worth doing.

The header-only recoveries (top of the table) all pay for it the same way: they trust a
cursor written separately from the data, and need an ordering rule to keep the two honest.
`diskqueue` publishes the record bytes and only then the header that advertises them,
`fsync` in between, so the header can lag the data but never lead it.

---

## Question 5 — can you see the loss?

Everyone has queue-depth gauges. Almost nobody counts what corruption destroyed.

| System | A metric that increments when data is lost to corruption? |
| --- | --- |
| **rsyslog `segmentedDisk`** | Yes — `corruption.events`, `corruption.bytes`, `corruption.records`, `corruption.segments` |
| **diskqueue** | Yes — `LostBytes`, `LostRecords`, `LostSegments`, `DiscardedBytes`, `ForeignSegments`, `Corruptions` |
| Promtail / Alloy | Partly — `record_decode_failures_total` counts decode failures, not bytes |
| Vector | No (`buffer_discarded_events_total` counts `when_full` drops, not corruption) |
| Fluent Bit | No — and a chunk that fails to register is invisible to the chunk gauges entirely |
| Fluentd, Filebeat, Logstash, OTel, `file_storage`, NSQ, rsyslog classic, syslog-ng, Telegraf | **No** |

If your agent silently drops a segment, in most of these you find out from a log line, if
you are grepping for the right string, or from a gap in the data days later. This is the
axis where the field is weakest, and it is cheap to fix: the counter costs one `uint64`.

---

## Question 6 — capacity and backpressure

| System | Cap | Unit | When full |
| --- | --- | --- | --- |
| **diskqueue** | `MaxSegments` × `SegmentSize` (default 32 × 8 MiB) | segments | `Add` returns `ErrFull` |
| Fluent Bit | `storage.total_limit_size` per output; `max_chunks_up` is a *memory* bound | chunks / bytes | Oldest chunks dropped |
| Fluentd | `total_limit_size` 64 GiB, `chunk_limit_size` 256 MiB | bytes | `BufferOverflowError` |
| Promtail / Alloy | `max_segment_age` — **wall clock, not bytes** | time | Segments deleted regardless of whether they were sent |
| OTel | `sending_queue.queue_size` = 1000 | requests (or items/bytes via `sizer`) | Reject, or block with `block_on_overflow` |
| `file_storage` | `max_size` (default 0 = unlimited) | bytes per DB file | Write fails |
| Filebeat | `max_size` — **required in config**, no code default | bytes | Producers block |
| Logstash | `queue.max_bytes` 1 GiB, `queue.page_capacity` 64 MiB | bytes, per pipeline | Pipeline blocks |
| Vector | `max_size`, minimum ~256 MiB | bytes | `when_full`: block or `drop_newest` |
| Telegraf | **unbounded** — `metric_buffer_limit` is not enforced for the disk strategy | — | Grows until the disk does not |
| NSQ | **unbounded** | — | Caller's problem |
| rsyslog | `queue.maxDiskSpace` (0 = unlimited) | bytes | Queue-full handling |
| syslog-ng | `capacity-bytes()`, min 1 MiB | bytes | Flow control |

Two traps here. **Promtail and Alloy cap by age, not by consumption** — a segment older
than `max_segment_age` is deleted whether or not anyone read it, so a slow or down remote
loses data on a timer rather than on a byte budget. And **Telegraf's disk buffer is
unbounded**: `metric_buffer_limit` is passed only to the memory arm.

`diskqueue`'s cap is in *segments*, not bytes, which is the coarsest unit in this table —
see below.

---

## Where `diskqueue` lands

Nothing here is novel in isolation. Segment files, a commit cursor and per-record
checksums are the standard vocabulary. Where it differs is in which trade-offs it takes:

- **Durable by default, per record.** Only four systems here fsync by default, and
  `diskqueue` is the strictest of them — two fsyncs per record so the failure mode is a
  clean truncation. It is also therefore the slowest at `SyncEvery: 1`, by design, with
  `SyncEvery`/`SyncInterval` as the escape hatch.
- **Recovery reads no records.** Everything needed to reopen lives in each segment's
  64-byte header, so startup is one `pread` per segment. Fluent Bit, Prometheus's WAL and
  Filebeat all pay for their backlog at every restart.
- **Corruption is dropped-and-counted, never wedging.** The blast radius scales with how
  much framing survived — one record if only a payload is damaged, one segment if the
  length prefix is. Of the fifteen, only rsyslog's unreleased `segmentedDisk` takes the
  same line; the rest lose a whole file, the whole queue, the process, or (Promtail) make
  no progress at all.
- **Every loss path has a counter.** Two systems in this survey can tell you how many bytes
  corruption destroyed. This one and an experimental branch of rsyslog.
- **Damage never becomes data.** A record that fails its checksum is dropped, never
  returned. Fluentd's unchecksummed torn tail is the counterexample: it flushes downstream
  as if it were real.
- **Zero allocations on the hot path**, and one dependency (`cespare/xxhash/v2`).

The closest design in the survey is **rsyslog's `segmentedDisk`** — CRC-32C over every
structure, O(1) startup from a small state file, skip-and-count corruption handling, rich
corruption counters. It is also experimental and not in any release. Convergent evolution
is a decent sign the shape is right.

---

## Where `diskqueue` is worse

An honest accounting, because most of this table is not a scoreboard:

- **It is unreleased and unproven.** Fluent Bit's storage layer has run on millions of
  hosts for years. This has a test suite and a fuzzer.
- **It is a library, not a buffer for an agent.** No inputs, no outputs, no retry policy,
  no compression, no encryption, no TLS, no backpressure protocol, no multi-pipeline
  routing. Vector, Fluent Bit and the Collector are solving a much larger problem, and
  their buffer designs answer to constraints this library does not have.
- ~~**The cap is in segments, not bytes.**~~ No longer true: `MaxBytes` caps the
  uncommitted backlog in bytes, composing with `MaxSegments` (whichever binds first
  returns `ErrFull`) — the same shape as Logstash's `queue.max_bytes` and syslog-ng's
  `capacity-bytes()`. Preallocation still makes the disk-footprint number honest.
- **One process, one writer.** An advisory `flock` on the directory; no shared access, no
  multi-process fan-in. Fluent Bit's per-input chunk directories and Logstash's per-
  pipeline queues are more flexible.
- **No compression.** Filebeat and Fluentd both compress buffered data; on log workloads
  that is often a 5–10× disk saving, which dwarfs any framing-overhead difference.
- **Framing overhead is fine but not free**: 9–11 bytes per record (uvarint length +
  xxhash64). 3.9% on a 256-byte record, 9% on a 100-byte one. Vector and Filebeat are
  comparable at 12 bytes; Fluent Bit amortizes to nearly zero by checksumming a whole
  2 MB chunk — and pays for it by losing the whole chunk.
- **At-most-once is easy to pick by accident.** `Take`, `Drain` and `Follow` commit as they
  read. That is a deliberate API, but the safe path (`Reserve`/`Commit`) is the longer one
  to write, and several systems here make the acked path the default.
- **No ordering across a reopen.** Offsets are not stable across restarts — fine for a
  queue, a problem if you wanted a log you can seek into.

---

## Things worth stealing

- **rsyslog `segmentedDisk`'s counters.** `corruption.records` *and* `corruption.bytes` *and*
  `corruption.segments` — three different questions an operator asks. Already adopted here.
- **Logstash's `pqcheck` / `pqrepair`.** An offline tool that inspects and repairs the
  queue, so the answer to corruption is not always "delete the directory". syslog-ng's
  `dqtool` and rsyslog's recovery tooling are the same idea. `diskqueue` has none.
- **Fluent Bit's dead-letter queue** (v5.0.0): quarantine the unreadable chunk somewhere an
  operator can find it, rather than unlinking it. Recovery is lossy either way, but one of
  those is forensically useful.
- **Vector's ledger.** Reader and writer positions in one mmap'd file rather than in each
  segment's header — cheaper to update than `diskqueue`'s per-segment commit cursor,
  at the cost of a second thing that can be torn.
- **Fluentd's `.meta` sidecars.** Chunk metadata separate from chunk data means the payload
  format is entirely the user's business. `diskqueue` bakes its framing in.

---

## Sources

Everything above was read from source at the versions named, on 2026-07-29:

- Fluent Bit `master` (5.1.0-dev), `src/flb_storage.c`, `src/flb_input_chunk.c`,
  `src/flb_mp.c`, `plugins/in_storage_backlog/sb.c`, vendored `lib/chunkio` (CIO 1.5.4)
- Fluentd `master` (v1.19.3), `lib/fluent/plugin/buf_file.rb`, `buf_file_single.rb`,
  `lib/fluent/plugin/buffer/file_chunk.rb`, `buffer.rb`
- Prometheus `main`, `tsdb/wlog/{wlog,reader,live_reader,watcher,checkpoint}.go`
- Loki v3.7.4 `clients/pkg/promtail/wal` (deleted from `main`); Grafana Alloy
  `internal/component/common/loki/wal`, `otelcol.storage.file`
- OpenTelemetry Collector `main` (v1.63.0/v0.157.0),
  `exporter/exporterhelper/internal/queue/persistent_queue.go`; contrib
  `extension/storage/filestorage`; `go.etcd.io/bbolt` v1.5.0
- Elastic Beats `main`, `libbeat/publisher/queue/diskqueue/*`; `elastic/go-txfile` (`pq`,
  `layout.go`); Logstash `main` (v9.4.4), `logstash-core/src/main/java/org/logstash/ackedqueue/*`
- Vector v0.57.0, `lib/vector-buffers/src/variants/disk_v2/*`
- Telegraf `master` (≥1.32), `models/buffer_disk.go`, `github.com/tidwall/wal` v1.2.1
- NSQ `nsqio/go-diskqueue` (master + v1.1.0), `nsqd/topic.go`, `nsqd/channel.go`
- rsyslog `main` (v8.2606.0 released), `runtime/queue.c`, `runtime/stream.c`,
  `runtime/segdisk_store.c` (unreleased)
- syslog-ng `master` (4.12.0), `modules/diskq/{qdisk.c,logqueue-disk*.c,dqtool.c}`

`diskqueue` figures are from this repository; the throughput table is
`BenchmarkAddDurability` (256-byte records, ext4 on NVMe, AMD Ryzen 7 8840HS) and the
framing overhead is `uvarintLen(n) + n + 8`.
