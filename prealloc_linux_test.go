//go:build linux

package diskqueue

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestSegmentBlocksArePreallocated: a segment must be backed by real blocks
// rather than a sparse hole, so a full filesystem is reported when the segment is
// created — where the store is still clean — instead of arriving as an ENOSPC in
// the middle of an append.
func TestSegmentBlocksArePreallocated(t *testing.T) {
	dir := t.TempDir()
	if !fallocateSupported(t, dir) {
		t.Skip("filesystem does not implement fallocate")
	}
	const segSize = 1 << 20
	s, err := openStore(dir, segSize, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()

	fi, err := os.Stat(filepath.Join(dir, "data.00000001"))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no stat_t")
	}
	if want, got := int64(headerSize+segSize), int64(st.Blocks)*512; got < want {
		t.Fatalf("segment holds %d bytes of allocated blocks for a %d-byte file: still sparse", got, want)
	}
}

func fallocateSupported(t *testing.T, dir string) bool {
	t.Helper()
	f, err := os.CreateTemp(dir, "probe")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	return preallocate(f, 4096) == nil && !isSparse(t, f)
}

func isSparse(t *testing.T, f *os.File) bool {
	t.Helper()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return !ok || int64(st.Blocks)*512 < 4096
}
