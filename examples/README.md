# Runnable examples

From a checkout with Go 1.23 or newer:

```sh
go run ./examples/basic
go run ./examples/replay
```

Both programs use a temporary queue directory, close it, remove it, and exit.
They keep the default durability policy and report write, acknowledgement, and
close errors. In an application, use a persistent directory instead and keep
the same codec, `SegmentSize`, and `StampRecords` setting when reopening it.

| Example | What it shows | Expected output |
| --- | --- | --- |
| [basic](basic/main.go) | String codecs, `Add`, `TryReserve`, and `Ack` | `hello, diskqueue` |
| [replay](replay/main.go) | JSON jobs and replay after reopening without acknowledging | See below |

```text
reserved: job-1
closing without acknowledgement
replayed: job-1
remaining: 0
```

The replay example demonstrates the persisted commit cursor using a clean
close and reopen. It is not a simulation of a power failure. It also shows why
handlers should be idempotent: a job may have been processed before a crash
without its acknowledgement reaching disk.

The string and JSON codecs allocate while decoding. For a codec that reuses
buffers, see the `uint64` examples in [example_test.go](../example_test.go).
That file also includes executable examples for corruption handling and
iterators; run them with:

```sh
go test -run '^Example' .
```

Use [`Reserve(ctx)`](../docs/guide.md#reserve-and-acknowledge)
to wait for work and `TryReserve` to return immediately when the queue is empty.
For several workers, give each goroutine its own `Reader` and use `Ack` after
processing. A failed handler leaves a reservation in flight; it replays after
reopening, or after a coordinated `Rewind` with all workers stopped.

[Back to the README](../README.md) · [Full guide](../docs/guide.md)
