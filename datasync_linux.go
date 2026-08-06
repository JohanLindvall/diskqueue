//go:build linux

package diskqueue

import (
	"errors"
	"os"
	"syscall"
)

// datasync flushes f's data without forcing a separate inode transaction for the
// metadata.
//
// fsync(2) writes the file's data *and* its inode; fdatasync(2) skips the inode
// unless a metadata change is needed to read the data back. Segments are
// preallocated to their full length at creation, so an append never grows the
// file and never changes any metadata a reader would need — only the timestamps,
// which nothing here depends on. That makes fdatasync the same crash guarantee
// for a materially smaller cost on the default per-record policy.
//
// A filesystem without fdatasync support answers ENOSYS; fall back rather than
// treating that as a durability failure.
func datasync(f *os.File) error {
	ctlErr, serr := fdControl(f, func(fd uintptr) error {
		return syscall.Fdatasync(int(fd))
	})
	if ctlErr != nil || errors.Is(serr, syscall.ENOSYS) {
		return f.Sync()
	}
	return serr
}
