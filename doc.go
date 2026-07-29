/*
Package diskqueue implements a generic, durable, FIFO disk-backed queue — a
persistent work queue that doubles as a write-ahead log.

Items are appended with [Queue.Add] and consumed through a [Reader]. The queue
survives process crashes and, configurably, power loss; on reopen it resumes from
the last committed record. It is backed by its own file store using plain
pread/pwrite/fsync (no mmap), and its only dependency is
[github.com/cespare/xxhash/v2] for per-record checksums.

# Getting started

A queue lives in a directory and is parameterized by the element type plus a
codec pair:

	q, err := diskqueue.New[uint64](dir, marshal, unmarshal)
	if err != nil {
		return err
	}

	if err := q.Add(42); err != nil {
		return err
	}

	r := q.NewReader()
	v, ok, err := r.TryTake()

	// Close flushes and reports a latched durability failure, so its error is
	// worth checking rather than deferring into the void.
	if err := q.Close(); err != nil {
		return err
	}

[MarshalFunc] appends to a caller-supplied buffer rather than allocating, which
is what keeps [Queue.Add] allocation-free. Both codec funcs run under the queue's
lock and must not call back into the queue or its readers.

# Consuming

There are three consumption patterns, and the choice between them is a delivery
guarantee, not a matter of taste.

At-most-once, one call. [Reader.Take] (blocking), [Reader.TryTake], and the
[Reader.Drain] / [Reader.Follow] iterators read and commit under a single lock.
If the process dies after the commit and before the work is done, that item is
gone. Use these when the work is cheap to lose or is itself idempotent and
externally recorded.

At-least-once, two calls. [Reader.Reserve] / [Reader.TryReserve] hand back an
item and its offset without committing; [Reader.Commit] retires it once the work
is durably done. A crash in between replays the item. This is the right default
for a work queue, and it is what makes the package usable as a write-ahead log.

Iteration. [Reader.Drain] yields the items present when iteration begins;
[Reader.Follow] continues indefinitely, waiting for new ones until the context is
cancelled or the queue is closed. Both are [iter.Seq], so an error cannot travel
with the values — check [Reader.Err] after the loop, or an I/O failure is
indistinguishable from an empty queue.

[Reader.Skip] discards the record at the head without decoding it. It is the
sanctioned way past a record the codec will never accept, because a decode
failure deliberately leaves the record in place (see [ErrCodec]).

# Durability

Every [Queue.Add] writes the record and then, separately, the header that
publishes it — data before header, each with its own fsync. A power loss can
therefore truncate the log cleanly but can never expose a record whose payload
never landed.

How often that fsync happens is the main throughput knob:

  - [Options].SyncEvery of 0 or 1 (the default) syncs every write and commit.
    Safe against power loss, and roughly two orders of magnitude slower than the
    alternatives.
  - [Options].SyncEvery greater than 1 syncs once every N operations. Up to N
    operations are exposed to power loss; a torn tail is caught by the per-record
    checksum on read.
  - [Options].SyncInterval adds a wall-clock backstop, so an idle queue's last
    writes become durable on a timer rather than waiting for N more operations.
  - [Options].NoSync never syncs. Data still survives a process crash through the
    page cache, but not a power loss.

[Queue.Sync] flushes on demand and [Queue.Close] always flushes.

A failed fsync is not retriable and is not treated as one. Linux reports a
writeback error exactly once and then drops the dirty pages, so a second fsync
can report success over data that is already gone. Rather than claim a durability
it does not have, the queue latches the failure: every subsequent Add, commit and
Sync returns [ErrIO] wrapping the original errno. Reads keep working, so a
poisoned queue can still be drained. Close and reopen to continue.

# Crash recovery and corruption

Two rules govern everything the recovery path does:

Corruption degrades to reported loss — never to corrupt output, and never to a
wedged queue. There is no strict mode. A queue that answers [ErrCorrupt] forever
is unavailable as well as damaged, and because a stuck cursor also stops
reclamation, the disk fills up behind it.

Every loss path is observable. Each event surfaces as exactly one [ErrCorrupt]
from a read and is counted in [Stats].

How much is lost depends on how much framing survives. A record whose checksum
fails but whose length still frames it inside its segment costs that record
alone. A length that is undecodable or overruns the segment takes the rest of
that segment with it, because the record boundaries behind it are gone too. A
genuine I/O error is not damage at all: nothing is dropped and the cursor stays
put, since the bytes may still be there next time.

Reopening reads no records — one 64-byte header pread per segment, and the header
is the single source of truth for the write cursor, the resume point and the
record count. The one exception is a segment whose header proves it lost bytes to
truncation; that one gets a bounded frame walk, because its recorded count
describes records that no longer exist and believing it would promise a backlog
no drain could deliver.

On reopen the read cursor resets to the persisted commit cursor, so uncommitted
items replay: the crash guarantee is at-least-once. [Queue.Rewind] does the same
thing without reopening, returning in-flight (reserved but uncommitted) records
to the queue — a bulk nack for a consumer that is shutting down or has failed.

# Observability

[Queue.Stats] returns a plain struct — no metrics registry is imposed on the
caller, and no callback of theirs runs under the queue's lock. It carries gauges
(backlog, in-flight bytes, segment count, disk footprint), lifetime counters
(added, delivered, committed), and the loss counters.

[Stats].Corruptions is the field to alert on: it counts events, each of which was
or will be surfaced as one [ErrCorrupt]. [Stats].LostBytes, LostRecords and
LostSegments say what those events cost. [Stats].Unreclaimed climbing
means fully-committed segments will not unlink and disk is not being freed.

Note that [Reader.Err] and a read's error tell you an event happened; only Stats
carries its magnitude.

# Sizing and limits

Storage is a directory of numbered, preallocated segment files.
[Options].SegmentSize sets each one (8 MiB by default) and [Options].MaxSegments
caps how many exist at once (32 by default), so the footprint is bounded at about
their product. Records never span segments, so a record larger than one segment
is rejected with [ErrRecordTooLarge]. When the cap is reached, [Queue.Add]
returns [ErrFull] until a segment is fully committed and reclaimed.

Segments are preallocated with fallocate where available, so a full filesystem is
discovered at segment creation rather than mid-record. [Options].MaxOpenFiles
bounds open descriptors for deep backlogs by closing least-recently-used handles;
it only matters when MaxSegments is unbounded, since the segment cap already
bounds the descriptor count.

Reclamation is whole-segment: a file is unlinked once every record in it is
committed. Committing is therefore what frees disk, and a consumer that reserves
without committing will hit [ErrFull] with the disk full of retained work.

# Concurrency

A [Queue] is safe for concurrent use. A single [Reader] is not — create one per
consuming goroutine with [Queue.NewReader].

Readers share one read/commit cursor and cooperate: each item is delivered to
exactly one of them. Take, TryTake, Drain and Follow commit under the lock as
they read and are safe for concurrent cooperating readers. Reserve/Commit is the
only deferred path, and its commits must be issued in offset order — with several
readers reserving concurrently, one reader's commit retires another's in-flight
record. Use a single consumer for that pattern, or coordinate.

[Reader.Skip] acts on the shared head rather than on a record the calling Reader
holds, so with cooperating readers it may discard one another reader would have
handled.

The blocking methods honour their context.

# Value lifetime

The slice passed to [UnmarshalFunc] — and anything in T that aliases it — is
owned by that Reader and is valid only until the Reader's next read. Copy out of
it if you need it longer.

Each Reader copies its record into a private buffer before decoding, which is
what makes concurrent readers safe: the store's read buffer is shared by every
Reader on the queue, so without the copy one consumer holding its value while
another reads would have its bytes rewritten from a different goroutine.

# Performance

The hot paths are allocation-free once warm. Add serializes through a reused
buffer and writes each record with a single pwrite; reads go through a shared
block buffer that holds a run of a segment rather than a single record, so
consuming a backlog costs roughly one pread per block instead of two per record.

# On-disk format

Each segment is a 64-byte little-endian header followed by records. The header
holds a magic number, the commit cursor, the write cursor, written and committed
counts, a format version, and an xxhash64 over its own first 56 bytes. Each
record is a uvarint length, the payload, and an 8-byte xxhash64 of the payload,
verified on every read.

A directory holds exactly one queue: [New] takes a non-blocking advisory lock on
it and returns [ErrLocked] if another Queue, in this process or another, already
holds it.

Segments written by a future format version are dropped on open and counted in
[Stats].ForeignSegments rather than failing the open. Reopening with a different
[Options].SegmentSize is refused with [ErrSegmentSizeMismatch], since honouring
it would discard data.

# Platform support

Every GOOS compiles. Preallocation uses fallocate on Linux and falls back to
ftruncate elsewhere and on filesystems that reject it. The directory lock uses
flock where the standard library exposes it and is a no-op on Windows, Solaris,
AIX, plan9 and js — on those platforms nothing prevents two processes from
opening the same directory. Directory fsync is POSIX-only and is likewise a no-op
off Unix. All of this uses only the standard library; there is no
golang.org/x/sys dependency.
*/
package diskqueue
