# Contributing to diskqueue

Thanks for helping improve diskqueue. Documentation fixes, questions, examples,
bug reports, and code contributions are all welcome. You do not need to be a
storage expert to contribute.

Please follow the [code of conduct](CODE_OF_CONDUCT.md). Report security
vulnerabilities through [SECURITY.md](SECURITY.md), rather than a public issue.

## Find a place to start

- Try the [examples](examples/README.md) and improve a step that was confusing.
- Look for [good first issues](https://github.com/JohanLindvall/diskqueue/labels/good%20first%20issue)
  or [help wanted](https://github.com/JohanLindvall/diskqueue/labels/help%20wanted).
  These lists may be empty; a small documentation PR is always welcome.
- Share a minimal reproducer for a bug or a benchmark with its environment.

For a small fix, open a pull request directly. For a new API, dependency,
on-disk format change, or large refactor, open an issue describing the problem
and proposed behavior first so we can agree on scope.

## Set up

Install Go 1.23 or newer, Git, and optionally GNU Make. Race tests need a working
C compiler and a platform supported by Go's race detector. Linux is the main
test environment; CI also builds all Go targets.

Fork the repository on GitHub, then clone your fork:

```sh
git clone https://github.com/YOUR-USERNAME/diskqueue.git
cd diskqueue
git remote add upstream https://github.com/JohanLindvall/diskqueue.git
git switch -c describe-your-change
go mod download
go run ./examples/basic
```

## Check a change

```sh
make help
make check
```

`make check` checks formatting, runs vet for both builds, runs the pinned
golangci-lint version used in CI, runs the normal and fault-injection suites
with the race detector, and executes both standalone examples. The first lint
run installs the tool into the ignored `.bin/` directory. Checks do not
rewrite source or module files.

| Command | Purpose |
| --- | --- |
| `make test` | Normal tests with race detection and coverage |
| `make faults` | Full suite with the `diskqueue_faults` build tag and race detection |
| `make examples` | Run the standalone examples |
| `make fmt` | Format Go files |
| `make fix` | Format Go files and run `go mod tidy` |
| `make lint` | Run the pinned linter |
| `make bench` | Run all benchmarks with allocation statistics |
| `make fuzz FUZZTIME=30s` | Fuzz the file store for a bounded duration |

Without Make, the main checks are:

```sh
gofmt -l .
go vet ./...
go vet -tags diskqueue_faults ./...
go test -race -cover ./...
go test -race -tags diskqueue_faults ./...
go run ./examples/basic
go run ./examples/replay
```

`gofmt -l .` should print nothing. CI runs the pinned linter as well as native
amd64/arm64 tests, the minimum Go version, 32-bit tests, cross-compilation,
fuzzing, and allocation benchmarks. A local test run covers only your current
platform.

For documentation-only changes, check links and run any changed examples.
For a behavior change, add a focused regression test that fails without the
fix. Do not add tests just to restate implementation details.

## Find your way around

| File | Responsibility |
| --- | --- |
| [diskqueue.go](diskqueue.go) | Public queue API, options, stats, producers, and group commit |
| [reader.go](reader.go) | Readers, acknowledgement, and iterators |
| [reserve.go](reserve.go) | Reservation tracking and ordered retirement |
| [store.go](store.go) | Segment lifecycle, file handles, writes, commits, and durability |
| [header.go](header.go), [record.go](record.go) | On-disk headers, framing, and checksums |
| [recovery.go](recovery.go) | Opening and recovering existing queues |
| [example_test.go](example_test.go), [examples/](examples/README.md) | Executable API examples and standalone programs |
| [doc.go](doc.go), [docs/guide.md](docs/guide.md) | Package documentation and detailed user guidance |
| [CLAUDE.md](CLAUDE.md) | Detailed storage invariants and implementation notes |

Keep these properties intact when changing storage code:

- Payloads reach stable storage before the headers that publish them.
- Corruption is skipped, reported, and counted; genuine I/O errors must not
  cause readable data to be discarded.
- An acknowledgement must not retire another worker's unfinished reservation.
- Healthy opens read headers without scanning every record.
- Reused buffers must preserve documented value lifetimes and concurrency rules.
- Platform-specific implementations need matching fallbacks.

Use `gofmt`, keep dependencies small, and put
`// SPDX-License-Identifier: MIT` at the start of new Go files. Update package
comments and the user guide when public behavior changes. Keep the README
quick start in sync with `examples/basic/main.go`.

## Open a pull request

Explain the problem, the resulting behavior, and how you checked the change.
Link the related issue if there is one. Include compatibility implications
for stored data or callers when relevant. A draft PR is useful for an early
design discussion.

Keep unrelated cleanup separate. If a check could not run, say which one and
why; you do not need to claim every platform was tested. Contributions are
covered by the repository's [MIT license](LICENSE).

## Report a bug or request a feature

Use the [issue chooser](https://github.com/JohanLindvall/diskqueue/issues/new/choose).
For storage bugs, include the module version, Go version, operating system,
filesystem, queue options, expected behavior, and a small reproducer.
Synthetic data is best; queue files may contain application secrets.

For benchmark reports, include the command, payload size, sync policy, producer
and consumer counts, CPU, filesystem, and storage device. Compare equivalent
durability policies. See the [performance examples](README.md#performance).

## Releases

The project is pre-1.0. Successful CI builds of `main` create a new patch tag;
existing tags are not rewritten. See the
[maintainer guide](docs/maintaining.md) for preparing user-facing release notes.
