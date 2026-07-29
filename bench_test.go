package diskqueue

import (
	"strconv"
	"testing"
)

// Benchmarks for the paths whose cost is dominated by syscalls rather than by
// the codec: a read, a commit, and the durable write policies. BenchmarkAddTake
// in diskqueue_test.go guards the zero-allocation property; these measure the
// syscall count, which is what actually moves.

var benchPayload = make([]byte, 256)

// BenchmarkRead isolates the read path: one Add per iteration is amortized away
// by pre-filling, so what is measured is takeHead + the reader's copy.
func BenchmarkRead(b *testing.B) {
	m, u := bytesCodec()
	w, err := New[[]byte](b.TempDir(), m, u, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	for i := 0; i < b.N; i++ {
		if err := w.Add(benchPayload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, _, err := r.TryReserve(); !ok || err != nil {
			b.Fatalf("reserve: ok=%v err=%v", ok, err)
		}
	}
}

// BenchmarkAddTakeCommit is the full round trip a queue consumer actually runs:
// append, read, commit — the path where a redundant pread costs most.
func BenchmarkAddTakeCommit(b *testing.B) {
	m, u := bytesCodec()
	w, err := New[[]byte](b.TempDir(), m, u, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	r := w.NewReader()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Add(benchPayload); err != nil {
			b.Fatal(err)
		}
		if _, ok, err := r.TryTake(); !ok || err != nil {
			b.Fatalf("take: ok=%v err=%v", ok, err)
		}
	}
}

// BenchmarkAddDurable measures each sync policy. The gap between SyncEvery 1 and
// the batched values is the whole reason the option exists.
func BenchmarkAddDurable(b *testing.B) {
	for _, every := range []int{1, 10, 100, 1000} {
		b.Run("SyncEvery"+strconv.Itoa(every), func(b *testing.B) {
			m, u := bytesCodec()
			w, err := New[[]byte](b.TempDir(), m, u, Options{SyncEvery: every, MaxSegments: -1})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Add(benchPayload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDeepBacklog reads from a queue spanning many segments with the open
// handle cap engaged, so segment lookup and handle churn are both in the path.
func BenchmarkDeepBacklog(b *testing.B) {
	for _, maxOpen := range []int{0, 3} {
		b.Run("MaxOpenFiles"+strconv.Itoa(maxOpen), func(b *testing.B) {
			m, u := bytesCodec()
			w, err := New[[]byte](b.TempDir(), m, u, Options{
				NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: maxOpen,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			r := w.NewReader()
			for i := 0; i < b.N; i++ {
				if err := w.Add(benchPayload); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok, err := r.TryTake(); !ok || err != nil {
					b.Fatalf("take: ok=%v err=%v", ok, err)
				}
			}
		})
	}
}

// BenchmarkBatchCommit is the Reserve/Commit deferred-ack path: reserve N
// records, then retire them with one Commit. That commit walks every record's
// frame to find each boundary, with the queue mutex held the whole time — so its
// cost is what every producer and every other reader waits behind.
func BenchmarkBatchCommit(b *testing.B) {
	const batch = 1000
	m, u := bytesCodec()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		w, err := New[[]byte](b.TempDir(), m, u, Options{NoSync: true, MaxSegments: -1})
		if err != nil {
			b.Fatal(err)
		}
		r := w.NewReader()
		for j := 0; j < batch; j++ {
			if err := w.Add(benchPayload); err != nil {
				b.Fatal(err)
			}
		}
		var last int64
		for j := 0; j < batch; j++ {
			_, ok, off, err := r.TryReserve()
			if !ok || err != nil {
				b.Fatalf("reserve: ok=%v err=%v", ok, err)
			}
			last = off
		}
		b.StartTimer()

		if err := r.Commit(last); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = w.Close()
		b.StartTimer()
	}
}
