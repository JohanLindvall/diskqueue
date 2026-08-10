package diskqueue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestSyncKeepsOpenFilesUnderCap: an off-lock flush pins the whole dirty set,
// and a pinned file is exactly the one evictOpen may not close — so opening
// them all at once made MaxOpenFiles unenforceable for the duration of the
// flush, and (nothing re-evicts on the way out) after it as well. NoSync is the
// unbounded shape: nothing is flushed until an explicit Sync, so every segment
// written since the last one is dirty and the descriptor count tracked the
// backlog rather than the cap. Sixteen dirty segments against a floor cap of 3
// used to leave 16 handles open; EMFILE is what it looks like at scale.
func TestSyncKeepsOpenFilesUnderCap(t *testing.T) {
	const openCap = 3
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{
		NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: openCap,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	payload := make([]byte, 1024)
	const records = 48
	for i := 0; i < records; i++ {
		if err := w.Add(payload); err != nil {
			t.Fatal(err)
		}
	}

	w.mu.Lock()
	dirty := 0
	for _, df := range w.st.files {
		if df.dirty {
			dirty++
		}
	}
	w.mu.Unlock()
	if dirty <= openCap {
		t.Fatalf("only %d dirty segments against a cap of %d: the flush never has to chunk, so this test proves nothing", dirty, openCap)
	}

	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	open, stillDirty := w.st.nOpen, 0
	for _, df := range w.st.files {
		if df.dirty {
			stillDirty++
		}
	}
	w.mu.Unlock()
	if open > openCap {
		t.Errorf("%d segment handles open after a Sync over %d dirty segments, cap is %d", open, dirty, openCap)
	}
	// The cap must not have been bought by flushing less: everything dirty at
	// the call is durable when it returns, which is the whole promise.
	if stillDirty != 0 {
		t.Errorf("%d segments still dirty after Sync, want 0", stillDirty)
	}
	if got := w.Stats().UnsyncedBytes; got != 0 {
		t.Errorf("UnsyncedBytes=%d after Sync, want 0", got)
	}
	r := w.NewReader()
	for i := 0; i < records; i++ {
		v, ok, err := r.TryTake()
		if err != nil || !ok {
			t.Fatalf("record %d: ok=%v err=%v", i, ok, err)
		}
		if len(v) != len(payload) {
			t.Fatalf("record %d: %d bytes, want %d", i, len(v), len(payload))
		}
	}
}

// The deterministic proofs for the off-lock flush live in
// offlocksync_faults_test.go, behind the injection seam. This file is the
// default build's share: a short hammer that runs Sync — foreground and the
// SyncInterval backstop both — against producers, a committing consumer and
// Close, under small segments and a floor MaxOpenFiles so eviction and
// reclamation keep crossing the pinned files. It asserts the invariants that
// survive scheduling (Close returns clean, a closed queue refuses Sync) and
// leaves the rest to the race detector, which is what `make test` runs it
// under.
func TestSyncCloseAddRace(t *testing.T) {
	m, u := bytesCodec()
	payload := make([]byte, 512)
	for it := 0; it < 25; it++ {
		w, err := New[[]byte](t.TempDir(), m, u, Options{
			SyncEvery:    1 << 30,
			SyncInterval: time.Millisecond,
			SegmentSize:  4096,
			MaxSegments:  -1,
			MaxOpenFiles: 3,
		})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for p := 0; p < 2; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := w.Add(payload); errors.Is(err, ErrClosed) {
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := w.Sync(); errors.Is(err, ErrClosed) {
					return
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := w.NewReader()
			for {
				if _, _, err := r.TryTake(); errors.Is(err, ErrClosed) {
					return
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		time.Sleep(4 * time.Millisecond) // let the interval backstop fire mid-traffic
		if err := w.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", it, err)
		}
		close(stop)
		wg.Wait()
		if err := w.Sync(); !errors.Is(err, ErrClosed) {
			t.Fatalf("iteration %d: Sync after Close: %v, want ErrClosed", it, err)
		}
	}
}
