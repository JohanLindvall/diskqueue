// SPDX-License-Identifier: MIT

//go:build unix

package diskqueue

import (
	"bytes"
	"os"
	"testing"
)

func TestRecoveryRequiresCommitReconciliationToPersist(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	s, dir := newTestStore(t, 32, 0)
	for i := 0; i < 6; i++ {
		mustAppend(t, s, idxRec(i))
	}
	middle := s.files[1]
	middle.header(setCommitCursor(headerSize+middle.size), setCommittedCount(middle.written))
	if err := s.writeHeader(middle); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(middle.path, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(middle.path, 0o644) })
	reopened, err := openStore(dir, 32, 0, true, 0, 0)
	if err == nil {
		_ = reopened.close()
		t.Fatal("open ignored a failed repair of stale commit metadata")
	}
	if err := os.Chmod(middle.path, 0o644); err != nil {
		t.Fatal(err)
	}
	reopened, err = openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.count() != 6 {
		t.Fatalf("recovered count=%d, want 6", reopened.count())
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialHeaderWriteRestoresPublishedImage(t *testing.T) {
	if !loadIsolate(t) {
		return
	}
	s, dir := newTestStore(t, 4096, 0)
	mustAppend(t, s, idxRec(1))
	before := append([]byte(nil), s.active().hdr...)
	// Stage the record before lowering the limit: only its publishing header
	// write should be short. This is the same publication used by Add/AddBatch.
	if err := s.stagePending(idxRec(2)); err != nil {
		t.Fatal(err)
	}
	s.takeSpan()
	restore := loadCapFileSize(t, 32)
	err := s.publishSpan()
	restore()
	if err == nil {
		t.Fatal("expected the publishing header write to be short")
	}
	if s.count() != 1 || s.stagedBytes() != 0 {
		t.Fatalf("failed publication left in-memory state: %+v", s.stats())
	}
	h, err := s.readHeader(1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(h, before) {
		t.Error("short write left a torn header instead of the previous published image")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, off, ok, err := reopened.takeHead()
	if err != nil || !ok {
		t.Fatalf("read after failed publication: %v, %v", ok, err)
	}
	// Exercise a partial commit write too, using a header loaded during open
	// rather than one created in this session.
	restore = loadCapFileSize(t, 32)
	err = reopened.commitTo(off)
	restore()
	if err == nil || reopened.count() != 1 || reopened.commitOff != 0 {
		t.Fatalf("short commit write advanced retirement: err=%v stats=%+v", err, reopened.stats())
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
	again, err := openStore(dir, 4096, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.close() }()
	got, reports := drainRecovering(t, again)
	if reports != 0 || len(got) != 1 || got[0] != 1 {
		t.Fatalf("recovery resurrected a rejected append: %v, reports=%d", got, reports)
	}
}
