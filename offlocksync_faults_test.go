//go:build diskqueue_faults

package diskqueue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// The off-lock flush (Queue.syncOffLock) makes three promises the default build
// cannot observe: the fdatasyncs really run with the queue mutex RELEASED, Close
// really waits for an in-flight flush's pins to drain before the store's
// handles are closed, and a failure at the off-lock fsync latches exactly as
// the in-lock one did. The "sync.file" injection point — crossed once per
// pinned file, between the pin and its fdatasync — is the seam that makes all
// three deterministic: a hook that parks there holds the flush mid-span, off
// the lock, for exactly as long as the test needs. No descriptor trick can do
// that; every other way of slowing an fsync is a race with the scheduler.

// parkAtSyncFile installs a hook that parks the flush at its first "sync.file"
// crossing: entered is closed when the flush arrives there, and the flush (all
// of its file crossings) proceeds once release is closed. Every other injection
// point reports no fault. The hook is called from several goroutines — the
// parked flush, concurrent appends hitting the append.* points — so it
// synchronizes only through its channels and the sync.Once.
func parkAtSyncFile(t *testing.T) (entered, release chan struct{}) {
	t.Helper()
	entered, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	faultHook = func(name string) error {
		if name == "sync.file" {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	t.Cleanup(func() { faultHook = nil })
	return entered, release
}

// TestSyncHoldsNoLockAcrossItsFdatasyncs is the off-lock property itself: with
// a Sync parked mid-fdatasync, producers and consumers keep moving. Under the
// old in-lock flush every operation below blocks on the mutex the flush holds,
// so the watchdog turns what used to be a stall into a failure. The tail then
// pins the writeSeq/snapshot semantics: a record added while the flush was in
// flight was not covered by it, must still be reported as exposure afterwards,
// and is flushed by the next Sync.
func TestSyncHoldsNoLockAcrossItsFdatasyncs(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{
		SyncEvery: 1 << 30, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	payload := make([]byte, 1024)
	for i := 0; i < 12; i++ { // ~4 dirty segments, so the flush pins several files
		if err := w.Add(payload); err != nil {
			t.Fatal(err)
		}
	}

	entered, release := parkAtSyncFile(t)
	syncErr := make(chan error, 1)
	go func() { syncErr <- w.Sync() }()
	<-entered

	// The flush is parked between its pins and its first fdatasync, with the
	// queue mutex released. Everything here needs that mutex — the takes also
	// commit, driving reclamation over the pinned files, and the adds drive
	// eviction over them (MaxOpenFiles is at its floor).
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Add(payload); err != nil {
			t.Errorf("Add during a parked Sync: %v", err)
		}
		r := w.NewReader()
		for i := 0; i < 3; i++ {
			if _, ok, err := r.TryTake(); !ok || err != nil {
				t.Errorf("TryTake during a parked Sync: ok=%v err=%v", ok, err)
			}
		}
		_ = w.Stats()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("queue operations blocked behind Sync's fdatasyncs: the flush is holding the mutex")
	}

	close(release)
	if err := <-syncErr; err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// The mid-flight Add landed after the flush's snapshot; booking it as
	// durable would be claiming an fsync that never covered it.
	if got := w.Stats().UnsyncedBytes; got == 0 {
		t.Fatal("UnsyncedBytes=0 after a Sync that raced a concurrent Add: the mid-flight record was booked as durable")
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := w.Stats().UnsyncedBytes; got != 0 {
		t.Fatalf("UnsyncedBytes=%d after the follow-up Sync, want 0", got)
	}
}

// TestCloseWaitsForInFlightSyncPins: Close's st.close() closes every handle,
// and the parked flush is holding some of them for its fdatasyncs. Close must
// park behind the flush (it acquires syncMu, which the flush holds for its
// whole span) rather than pulling the descriptors out from under it.
func TestCloseWaitsForInFlightSyncPins(t *testing.T) {
	dir := t.TempDir()
	m, u := bytesCodec()
	opt := Options{SyncEvery: 1 << 30, SegmentSize: 4096, MaxSegments: -1}
	w, err := New[[]byte](dir, m, u, opt)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 1024)
	for i := 0; i < 6; i++ {
		if err := w.Add(payload); err != nil {
			t.Fatal(err)
		}
	}

	entered, release := parkAtSyncFile(t)
	syncErr := make(chan error, 1)
	go func() { syncErr <- w.Sync() }()
	<-entered

	closeErr := make(chan error, 1)
	go func() { closeErr <- w.Close() }()
	// A short delay cannot prove Close waits, but a Close that returns during
	// it has certainly not waited: the flush is still parked on its handles.
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned (%v) while a Sync held pinned handles", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	// The Sync was admitted while the queue was open, so it completes with its
	// own verdict — not ErrClosed — and only then does Close take its turn.
	if err := <-syncErr; err != nil {
		t.Fatalf("Sync concurrent with Close: %v", err)
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("Close after the flush drained: %v", err)
	}
	// Nothing was fsync'd over a closed handle and nothing latched: the store
	// reopens clean with every record intact.
	w2, err := New[[]byte](dir, m, u, opt)
	if err != nil {
		t.Fatal(err)
	}
	if got := w2.Count(); got != 6 {
		t.Fatalf("reopen Count=%d, want 6", got)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
}

// parkAtSyncFileCrossing is parkAtSyncFile aimed at a chosen crossing rather
// than the first: the flush parks at its nth "sync.file" (1-based) and every
// other crossing passes straight through, so a test can observe the flush deep
// into a chunked run instead of at its very start.
func parkAtSyncFileCrossing(t *testing.T, n int) (entered, release chan struct{}) {
	t.Helper()
	entered, release = make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	crossings := 0
	faultHook = func(name string) error {
		if name != "sync.file" {
			return nil
		}
		mu.Lock()
		crossings++
		parkHere := crossings == n
		mu.Unlock()
		if parkHere {
			close(entered)
			<-release
		}
		return nil
	}
	t.Cleanup(func() { faultHook = nil })
	return entered, release
}

// TestSyncKeepsOpenFilesUnderCapMidFlush is the peak that
// TestSyncKeepsOpenFilesUnderCap can only measure after the fact: MaxOpenFiles
// has to hold WHILE the flush runs, not just once it has finished. A pinned
// file is exactly the one evictOpen may not close, so a flush that pinned its
// whole dirty set held a descriptor per dirty segment for its whole span —
// unbounded under NoSync, where nothing is flushed until an explicit Sync.
// Parking deep into the run is what makes the distinction: at that point every
// earlier chunk's handle must already have been released.
func TestSyncKeepsOpenFilesUnderCapMidFlush(t *testing.T) {
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
	for i := 0; i < 48; i++ { // 16 dirty segments against a cap of 3
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
	if dirty <= 2*openCap {
		t.Fatalf("only %d dirty segments against a cap of %d: too few to tell a chunked flush from an all-at-once one", dirty, openCap)
	}

	// Two thirds of the way in, so several chunks have come and gone.
	entered, release := parkAtSyncFileCrossing(t, 2*dirty/3)
	syncErr := make(chan error, 1)
	go func() { syncErr <- w.Sync() }()
	<-entered

	// The flush is parked mid-fdatasync with the queue mutex released, holding
	// the handles of the chunk in flight. That is the peak.
	w.mu.Lock()
	open := w.st.nOpen
	w.mu.Unlock()
	if open > openCap {
		t.Errorf("%d segment handles open mid-flush over %d dirty segments, cap is %d", open, dirty, openCap)
	}

	close(release)
	if err := <-syncErr; err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := w.Stats().UnsyncedBytes; got != 0 {
		t.Errorf("UnsyncedBytes=%d after Sync, want 0", got)
	}
}

// TestOffLockSyncFailureLatches: moving the fdatasync off the lock must not
// soften its verdict. A failure there latches ErrIO exactly as the in-lock
// flush's did, the dirty state stays standing (exposure keeps over-reporting),
// and every later Sync repeats the latched answer.
func TestOffLockSyncFailureLatches(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{SyncEvery: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}

	injectAt(t, "sync.file", errInjected)
	err = w.Sync()
	if !errors.Is(err, ErrIO) {
		t.Fatalf("Sync with a failing off-lock fdatasync: %v, want ErrIO", err)
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("Sync: %v does not wrap the original failure", err)
	}
	if !errors.Is(w.Err(), ErrIO) {
		t.Fatalf("Err=%v, want the latched ErrIO", w.Err())
	}
	if got := w.Stats().UnsyncedBytes; got == 0 {
		t.Fatal("UnsyncedBytes cleared by a failed Sync: exposure must keep over-reporting")
	}
	if err := w.Sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("Sync after the latch: %v, want the latch to hold", err)
	}
}
