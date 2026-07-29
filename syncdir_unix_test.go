//go:build unix

package diskqueue

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

// TestDirSyncUnsupportedClassifier: a filesystem with no directory-fsync
// primitive must not poison the store or fail the open — syncDir runs on the
// open path, so misclassifying "this mount cannot do that" as a durability
// failure would make the queue unopenable over such a mount.
//
// The classifier is pure, so it can be tested exhaustively without needing one
// of those filesystems to hand.
func TestDirSyncUnsupportedClassifier(t *testing.T) {
	unsupported := []error{syscall.EINVAL, syscall.ENOTSUP, syscall.ENOSYS}
	for _, err := range unsupported {
		if !dirSyncUnsupported(err) {
			t.Errorf("dirSyncUnsupported(%v) = false, want true", err)
		}
		// It must survive wrapping: syncDir sees whatever os.File.Sync returns,
		// which is a *PathError, not a bare errno.
		wrapped := &os.PathError{Op: "sync", Path: "/tmp/x", Err: err}
		if !dirSyncUnsupported(wrapped) {
			t.Errorf("dirSyncUnsupported(%v) = false, want true through a PathError", wrapped)
		}
		if !dirSyncUnsupported(fmt.Errorf("wrapped: %w", err)) {
			t.Errorf("dirSyncUnsupported did not unwrap %v", err)
		}
	}

	// A real failure must NOT be classified away, or a genuine durability
	// problem is silently swallowed on every segment create.
	supported := []error{
		syscall.EIO, syscall.ENOSPC, syscall.EACCES, syscall.EBADF,
		errors.New("some other failure"),
	}
	for _, err := range supported {
		if dirSyncUnsupported(err) {
			t.Errorf("dirSyncUnsupported(%v) = true, want false: a real failure was swallowed", err)
		}
	}
	if dirSyncUnsupported(nil) {
		t.Error("dirSyncUnsupported(nil) = true")
	}
}
