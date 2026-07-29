package diskqueue

import (
	"bytes"
	"sync"
	"testing"
)

// Why the Reader copies each record into its own scratch buffer.
//
// The reason is NOT file lifetime. `payload` is a slice of s.readBuf — ordinary
// Go memory filled by ReadAt — so closing or unlinking the segment cannot
// invalidate it; that rationale is mmap-era reasoning left behind when the store
// moved to plain file I/O. The operative reason is sharing: s.readBuf belongs to
// the *store*, and every Reader on the queue reads through it. Without a
// per-Reader copy, one consumer holding its value while another performs the next
// read gets its bytes rewritten underneath it, from another goroutine — a data
// race, not merely a lifetime surprise.
//
// These tests use the aliasing codec the docs warn about (T = []byte, identity
// unmarshal), which is the shape that makes the hazard reachable at all.

func bytesCodec() (MarshalFunc[[]byte], UnmarshalFunc[[]byte]) {
	return func(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil },
		func(d []byte) ([]byte, error) { return d, nil } // deliberately aliases
}

func distinctPayload(i int) []byte { return bytes.Repeat([]byte{byte('A' + i%26)}, 64) }

// TestReadersDoNotShareABuffer: the value one Reader is holding must survive
// another Reader's read. Without the per-Reader copy, the second read overwrites
// the store's buffer and the first consumer's slice changes under it.
func TestReadersDoNotShareABuffer(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	for i := 0; i < 2; i++ {
		if err := w.Add(distinctPayload(i)); err != nil {
			t.Fatal(err)
		}
	}
	r1, r2 := w.NewReader(), w.NewReader()

	held, ok, err := r1.TryTake()
	if !ok || err != nil {
		t.Fatalf("first take: ok=%v err=%v", ok, err)
	}
	// r2 now reads through the same store buffer while r1 still holds its value.
	if _, ok, err := r2.TryTake(); !ok || err != nil {
		t.Fatalf("second take: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(held, distinctPayload(0)) {
		t.Fatalf("another Reader's read rewrote a held value: got %q..., want %q...",
			held[:8], distinctPayload(0)[:8])
	}
}

// TestConcurrentReadersRace is the assertion that cannot be argued with: run the
// same shape from two goroutines under -race. Without the per-Reader copy this is
// a reported data race on the store's shared read buffer.
func TestConcurrentReadersRace(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	const n = 400
	for i := 0; i < n; i++ {
		if err := w.Add(distinctPayload(i)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := w.NewReader() // one Reader per goroutine, as documented
			for {
				v, ok, err := r.TryTake()
				if err != nil || !ok {
					return
				}
				// Touch every byte of the value while the other goroutines read.
				// The race detector reports the conflict if these bytes are the
				// store's buffer rather than this Reader's copy.
				want := v[0]
				for _, b := range v {
					if b != want {
						t.Errorf("value not self-consistent: %q", v[:8])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestValueSurvivesSegmentReclamation pins "immediate close is safe" as
// behaviour. Taking the last record of a segment commits it, which reclaims and
// unlinks that segment inside the same call — the value handed back must still be
// intact afterwards.
//
// It holds for a different reason than CLAUDE.md used to give (the bytes were
// never the file's to begin with), but it is the property callers depend on, so
// it is worth a test that does not care why.
func TestValueSurvivesSegmentReclamation(t *testing.T) {
	m, u := bytesCodec()
	dir := t.TempDir()
	w, err := New[[]byte](dir, m, u, Options{NoSync: true, SegmentSize: 4096, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	// 64-byte payloads in 4 KiB segments: comfortably several segments.
	const n = 300
	for i := 0; i < n; i++ {
		if err := w.Add(distinctPayload(i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.Stats().Segments; got < 3 {
		t.Fatalf("need several segments, got %d", got)
	}

	r := w.NewReader()
	for i := 0; i < n; i++ {
		before := countDataFiles(t, dir)
		v, ok, err := r.TryTake()
		if !ok || err != nil {
			t.Fatalf("take %d: ok=%v err=%v", i, ok, err)
		}
		if after := countDataFiles(t, dir); after < before {
			// This take's commit reclaimed a segment. The value must be unharmed.
			if !bytes.Equal(v, distinctPayload(i)) {
				t.Fatalf("value corrupted by the reclamation its own commit triggered: "+
					"got %q..., want %q...", v[:8], distinctPayload(i)[:8])
			}
		}
		if !bytes.Equal(v, distinctPayload(i)) {
			t.Fatalf("take %d returned the wrong record", i)
		}
	}
}
