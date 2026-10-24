// SPDX-License-Identifier: MIT

//go:build diskqueue_faults

package diskqueue

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExplicitFlushPersistsNoSyncDirectory(t *testing.T) {
	for _, closeQueue := range []bool{false, true} {
		w, dir := openAckQueue(t, ackOpts())
		addN(t, w, 1)
		injectAt(t, "sync.dir", errInjected)
		flush := w.Sync
		if closeQueue {
			flush = w.Close
		}
		if err := flush(); !errors.Is(err, errInjected) || !errors.Is(err, ErrIO) {
			t.Errorf("Close=%v: directory flush failure was not reported: %v", closeQueue, err)
		}
		if w.Stats().UnsyncedBytes == 0 {
			t.Error("failed directory flush reported all bytes durable")
		}
		faultHook = nil
		_ = w.Close()
		reopened, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
		if err != nil {
			t.Fatal(err)
		}
		if v, ok, err := reopened.NewReader().TryTake(); err != nil || !ok || v != 0 {
			t.Fatalf("record after reopen: %d, %v, %v", v, ok, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestZeroScanReadErrorPreservesSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.00000001")
	if err := os.WriteFile(path, make([]byte, 4096+headerSize), 0o644); err != nil {
		t.Fatal(err)
	}
	injectAt(t, "recovery.readZero", errInjected)
	w, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err == nil {
		_ = w.Close()
		t.Error("recovery succeeded after failing to read the zero-filled segment")
	} else if !errors.Is(err, errInjected) {
		t.Errorf("recovery error lost its cause: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed read removed the segment: %v", err)
	}
}

func TestFailedTruncationRecoveryClosesHandles(t *testing.T) {
	dir := t.TempDir()
	opts := manyRecords(t, dir, 700)
	path := filepath.Join(dir, "data.00000001")
	if err := os.Truncate(path, headerSize+70003); err != nil {
		t.Fatal(err)
	}
	failReadsAfter(t, 0, errInjected)
	before := openFDs(t)
	for i := 0; i < 5; i++ {
		w, err := New[uint64](dir, bigM(), bigU(), opts)
		if err == nil {
			_ = w.Close()
			t.Fatal("recovery unexpectedly succeeded")
		}
	}
	if after := openFDs(t); after > before {
		t.Fatalf("failed recovery leaked %d descriptors", after-before)
	}
}

func TestGroupFlushReportsInterveningDurabilityFailure(t *testing.T) {
	for _, point := range []string{"append.syncData", "append.syncHeader"} {
		w, _ := openAckQueue(t, Options{SegmentSize: 4096})
		faultHook = func(name string) error {
			if name == point {
				// Model a failure latched by a commit while the group flush has
				// released the mutex. Its own fdatasync can still succeed.
				w.st.failIO(errInjected)
			}
			return nil
		}
		err := w.Add(1)
		faultHook = nil
		if !errors.Is(err, errInjected) || !errors.Is(err, ErrIO) {
			t.Errorf("%s: Add reported %v after an intervening durability failure", point, err)
		}
		_ = w.Close()
	}
}

func TestSyncKeepsDescriptorCapDuringConcurrentReads(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{
		NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	for i := 0; i < 8; i++ {
		if err := w.Add(make([]byte, 3000)); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := parkAtSyncFile(t)
	done := make(chan error, 1)
	go func() { done <- w.Sync() }()
	<-entered
	// The first chunk is pinned; read a segment outside that chunk while the
	// active file also occupies a descriptor.
	w.mu.Lock()
	_, _, _, _, readErr := w.st.read(w.st.files[4].base)
	open := w.st.nOpen
	w.mu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	if open > 3 {
		t.Fatalf("concurrent read opened %d descriptors under a cap of 3", open)
	}
}

func TestInterruptedRepairKeepsReplayDecision(t *testing.T) {
	dir := emptyTornStore(t)
	repairs := 0
	faultHook = func(name string) error {
		if name == "recovery.writeHeader" {
			repairs++
			if repairs == 2 {
				return errInjected
			}
		}
		return nil
	}
	s, err := openStore(dir, 32, 0, false, 0, 0)
	faultHook = nil
	if err == nil {
		_ = s.close()
		t.Fatal("open ignored a failed repair")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("open: %v", err)
	}
	s, err = openStore(dir, 32, 0, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	if s.count() != 2 {
		t.Fatalf("interrupted repair lost its replay decision: count=%d", s.count())
	}
}

func TestRecoveryFlushesRepairsUnderNoSync(t *testing.T) {
	dir := emptyTornStore(t)
	injectAt(t, "recovery.syncHeader", errInjected)
	s, err := openStore(dir, 32, 0, true, 0, 0)
	faultHook = nil
	if err == nil {
		_ = s.close()
		t.Fatal("NoSync let recovery publish repairs without flushing them")
	}
	if !errors.Is(err, ErrIO) || !errors.Is(err, errInjected) {
		t.Fatalf("repair flush failure: %v", err)
	}
	s, err = openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	if s.count() != 2 {
		t.Fatalf("retry lost records after the failed repair flush: %d", s.count())
	}
}

func TestFailedGroupPublicationWakesCapacityWaiters(t *testing.T) {
	w, _ := openAckQueue(t, Options{SegmentSize: 4096, MaxBytes: 17})
	// While this span is in flight, its bytes exhaust the budget and AddWait
	// can park on spaceNotify even though the visible queue is still empty.
	if err := w.st.stagePending(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	waiter := make(chan struct{})
	w.spaceNotify = waiter
	injectAt(t, "append.writeHeader", errInjected)
	w.mu.Lock()
	err := w.leadFlushLocked()
	w.mu.Unlock()
	if !errors.Is(err, errInjected) {
		t.Fatalf("flush: %v", err)
	}
	select {
	case <-waiter:
	default:
		t.Fatal("discarding unpublished bytes left capacity waiters asleep")
	}
	faultHook = nil
	if err := w.Add(2); err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
}

func TestSyncFailureWakesCapacityWaiters(t *testing.T) {
	w, _ := openAckQueue(t, Options{NoSync: true, MaxBytes: 17})
	addN(t, w, 1)
	waiter := make(chan struct{})
	w.spaceNotify = waiter
	injectAt(t, "sync.file", errInjected)
	if err := w.Sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("Sync: %v", err)
	}
	select {
	case <-waiter:
	default:
		t.Fatal("a latched sync failure left capacity waiters asleep")
	}
	faultHook = nil
}

func TestSyncSkipsFilesReclaimedBehindPinnedPrefix(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, Options{
		NoSync: true, SegmentSize: 4096, MaxSegments: -1, MaxOpenFiles: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	for i := 0; i < 8; i++ {
		if err := w.Add(make([]byte, 3000)); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := parkAtSyncFile(t)
	done := make(chan error, 1)
	go func() { done <- w.Sync() }()
	<-entered
	r := w.NewReader()
	for i := 0; i < 7; i++ {
		if _, ok, err := r.TryTake(); !ok || err != nil {
			close(release)
			_ = <-done
			t.Fatalf("read %d: %v, %v", i, ok, err)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Sync tried to reopen a reclaimed file behind the pinned prefix: %v", err)
	}
	if w.Count() != 1 {
		t.Fatalf("concurrent reclamation changed the unread record: %+v", w.Stats())
	}
}
