# diskqueue

[![CI](https://github.com/JohanLindvall/diskqueue/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/JohanLindvall/diskqueue/actions/workflows/ci.yml?query=branch%3Amain)
[![Go Reference](https://pkg.go.dev/badge/github.com/JohanLindvall/diskqueue.svg)](https://pkg.go.dev/github.com/JohanLindvall/diskqueue)
[![Version](https://img.shields.io/github/v/tag/JohanLindvall/diskqueue?sort=semver&label=version)](https://pkg.go.dev/github.com/JohanLindvall/diskqueue?tab=versions)
[![License](https://img.shields.io/github/license/JohanLindvall/diskqueue)](LICENSE)

**A durable, generic FIFO queue for Go, backed by local disk.**

Keep background jobs, events, or outgoing requests across application restarts.
Embed the queue in your Go process, choose your own serialization, and
acknowledge work after it succeeds. No separate server to operate.

[Quick start](#usage) · [Examples](examples/README.md) · [User guide](docs/guide.md) ·
[API reference](https://pkg.go.dev/github.com/JohanLindvall/diskqueue) ·
[Contributing](CONTRIBUTING.md)

## Why diskqueue?

- **Durable by default.** `Add` flushes the record and then its header before
  returning. Concurrent producers share flushes through group commit.
- **Explicit acknowledgement.** `Reserve` + `Ack` lets workers finish processing
  before retiring an item. Uncommitted work replays after reopening.
- **Observable recovery.** Checksums catch accidental damage. Corrupt data is
  skipped and reported through `ErrCorrupt` and loss counters in `Stats()`.
- **Bounded buffering.** Set a backlog byte budget, limit segment files, and use
  `AddWait(ctx, value)` to wait for capacity.
- **Small dependency footprint.** Pure Go, no mmap or cgo, and one dependency:
  `cespare/xxhash/v2`. Healthy restarts read segment headers, not every record.

A good fit for local work queues, buffering during a downstream outage, and
replaying unacknowledged events. Each directory has one owning queue instance;
multiple goroutines can produce and consume through it. For replication,
network clients, or independent subscriber histories, use a broker or log
service with those features.

## Install

Requires **Go 1.23 or newer**. From an existing Go module:

```sh
go get github.com/JohanLindvall/diskqueue
```

Starting from an empty directory? Run `go mod init example.com/queue-demo` first.
The project is pre-1.0; review changes before upgrading and keep the module
version pinned in your application's `go.mod`.

## Usage

Save this as `main.go` and run `go run .`. It writes an item, reads it, acknowledges
it, and exits. The temporary directory is removed on exit; use a persistent
application data directory when you want the queue to survive restarts.

```go
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/JohanLindvall/diskqueue"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	dir, err := os.MkdirTemp("", "diskqueue-basic-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	q, err := diskqueue.New[string](dir,
		func(dst []byte, s string) ([]byte, error) { return append(dst, s...), nil },
		func(data []byte) (string, error) { return string(data), nil },
	)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	if err := q.Add("hello, diskqueue"); err != nil {
		return err
	}

	r := q.NewReader()
	value, ok, offset, err := r.TryReserve()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("expected the item we just added")
	}
	fmt.Println(value) // Do your work before acknowledging.
	return r.Ack(offset)
}
```

Output: `hello, diskqueue`

The same program lives in [examples/basic](examples/basic/main.go). To see
unfinished JSON work survive a close and reopen, run
[`go run ./examples/replay`](examples/replay/main.go) from a checkout.
For an interactive corruption demo,
[try the Go Playground](https://go.dev/play/p/IerWmkg2R-x).

### Choose how to consume

| Need | Use | When is the item committed? |
| --- | --- | --- |
| Process, then acknowledge | `Reserve(ctx)` + `Ack(offset)` | After your handler succeeds and you acknowledge |
| Check for work without waiting | `TryReserve()` + `Ack(offset)` | After your handler succeeds and you acknowledge |
| Consume in one call | `Take(ctx)` or `TryTake()` | Before the call returns successfully |
| Iterate over available or arriving items | `Drain(ctx)` or `Follow(ctx)` | Before the loop body runs; check `Reader.Err()` afterward |

Give each consuming goroutine its own `Reader`. Readers share a single stream;
they divide work rather than each receiving a copy. Use `Ack` with multiple
workers: `Commit` also retires earlier records that another worker may still
be processing.

Make handlers **idempotent**. A crash after processing but before a durable
acknowledgement can replay the item. There is no automatic retry timer for a
failed handler: unacknowledged reservations replay after reopening or a
coordinated `Rewind` once workers have stopped. Acknowledgements behind an
unfinished reservation become durable when the contiguous prefix is complete.
See [delivery semantics](docs/guide.md#semantics).

## Configuration

The zero-value `Options` uses an 8 MiB segment size, up to 32 segment files,
and syncs every write and commit. Start with these defaults; tune after measuring.

```go
q, err := diskqueue.New[string](path, marshal, unmarshal, diskqueue.Options{
    MaxBytes:    64 << 20, // 64 MiB of uncommitted records
    MaxSegments: 32,       // cap segment files independently
})
```

`MaxBytes` limits backlog bytes, not total disk use: allow room for segment
headers and preallocated slack. `Add` returns `ErrFull` at capacity;
`AddWait(ctx, value)` waits for room. An item larger than the byte budget returns
`ErrRecordTooLarge` immediately. See [sizing and options](docs/guide.md#options).

`NoSync` and `SyncEvery > 1` trade power-loss durability for throughput.
`SyncInterval` adds a flush timer to `SyncEvery`; `Sync` and `Close` flush on
demand. Always handle `Close` errors. Filesystem and platform support affect
durability and directory locking; see [storage layout](docs/guide.md#storage-layout).

## Performance

The reusable-buffer paths have benchmarks that CI checks for **0 allocs/op**.
Your codec and sync policy affect allocations: the string and JSON examples
allocate, and the default group-commit path has per-flush bookkeeping.

Run the benchmarks on your own storage:

```sh
# Unsynced add/read/commit path: throughput and allocation cost.
go test -run='^$' -bench='^BenchmarkAddTake$' -benchmem .

# Durable writes, batching, and concurrent producers.
go test -run='^$' -bench='^Benchmark(AddDurable|AddBatch|AddParallel)$' -benchmem .
```

Report the Go version, CPU, filesystem, storage device, record size, and sync
policy with results. Unsynced throughput does not describe durable write latency.

## Documentation

<!-- Keep older README section links useful after moving the detailed reference. -->
<a id="api"></a>
<a id="semantics"></a>
<a id="recovery"></a>
<a id="failure-handling"></a>
<a id="options"></a>
<a id="storage-layout"></a>
<a id="reserve--commit-at-least-once-with-explicit-acknowledgement"></a>
<a id="iterating-consuming"></a>

| Looking for | Read |
| --- | --- |
| Complete programs you can run | [Examples](examples/README.md) |
| Queue and reader methods | [API overview](docs/guide.md#api) and [Go reference](https://pkg.go.dev/github.com/JohanLindvall/diskqueue) |
| Acknowledgement, retries, concurrency, and value lifetime | [Semantics](docs/guide.md#semantics) and [failure handling](docs/guide.md#failure-handling) |
| Capacity, durability, and timestamps | [Options](docs/guide.md#options) |
| Corruption, recovery, and loss metrics | [Recovery](docs/guide.md#recovery) |
| Segment format and platform limitations | [Storage layout](docs/guide.md#storage-layout) |
| Design tradeoffs against other disk buffers | [Comparison](comparison.md) |

## Help and contributing

[Ask a question](https://github.com/JohanLindvall/diskqueue/issues/new?template=question.yml),
[report a bug](https://github.com/JohanLindvall/diskqueue/issues/new?template=bug_report.yml),
or [suggest an improvement](https://github.com/JohanLindvall/diskqueue/issues/new?template=feature_request.yml).
Small documentation fixes, reproducible bug reports, examples, and benchmark
results are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, checks,
and a map of the code, and follow our [code of conduct](CODE_OF_CONDUCT.md).

```sh
git clone https://github.com/JohanLindvall/diskqueue.git
cd diskqueue
make help       # available development commands
make check      # formatting, vet, pinned lint, race/fault tests, and examples
```

If diskqueue is useful to you, a star helps others discover it. Sharing a
concrete use case or a reproducible benchmark helps people decide whether it
fits their application.

## Security

Report vulnerabilities privately using [SECURITY.md](SECURITY.md).
Use the issue tracker for ordinary bugs and durability problems.

## Citing

[CITATION.cff](CITATION.cff) provides GitHub's **Cite this repository** metadata.

## License

[MIT](LICENSE). Every Go source file carries an `SPDX-License-Identifier: MIT`
identifier.
