//go:build unix

package diskqueue

import (
	"errors"
	"os"
	"syscall"
)

// fsyncDir flushes the directory itself, making segment creations and removals
// durable: fsync of a file flushes its data and inode but never the directory
// entry that names it, which a power loss would otherwise drop — stranding
// records that were already fsync'd.
func fsyncDir(d *os.File) error { return d.Sync() }

// dirSyncUnsupported reports whether err means "this filesystem has no directory
// fsync" rather than "the flush failed". Several kernels and FUSE filesystems
// answer EINVAL for fsync on a directory handle; treating that as a durability
// failure would poison a queue over such a mount, or refuse to open it at all.
//
// The cases are separate errors.Is calls on purpose: on Linux ENOTSUP and
// EOPNOTSUPP are the same constant, so listing them in one expression switch
// would be a duplicate-case compile error.
func dirSyncUnsupported(err error) bool {
	switch {
	case errors.Is(err, syscall.EINVAL):
		return true
	case errors.Is(err, syscall.ENOTSUP):
		return true
	case errors.Is(err, syscall.ENOSYS):
		return true
	default:
		return false
	}
}
