package diskqueue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

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
