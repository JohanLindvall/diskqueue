//go:build linux

package diskqueue

import (
	"errors"
	"os"
	"syscall"
)

// preallocate reserves size bytes of blocks for f and extends it to that length.
//
// Reserving up front is what keeps a full filesystem from turning into a
// half-written record: ENOSPC is reported here, while the segment is still empty
// and nothing has been published, instead of arriving in the middle of an append
// (or, worse, from the fsync that follows it). It also keeps the segment out of
// the sparse-file allocator, so the queue's footprint is what MaxSegments ×
// SegmentSize says it is.
//
// On Linux this is fallocate(2) with mode 0. Filesystems that do not implement it
// (NFS, 9p, parts of FUSE/overlayfs, pre-2.6.31 kernels) answer ENOTSUP /
// EOPNOTSUPP / ENOSYS / EINVAL; there we fall back to ftruncate and accept a
// sparse segment. A real out-of-space answer (ENOSPC, EDQUOT) is returned to the
// caller.
func preallocate(f *os.File, size int64) error {
	ctlErr, ferr := fdControl(f, func(fd uintptr) error {
		return syscall.Fallocate(int(fd), 0, 0, size)
	})
	if ctlErr != nil {
		return ctlErr
	}
	switch {
	case ferr == nil:
		return nil
	case errors.Is(ferr, syscall.ENOTSUP), // == EOPNOTSUPP on Linux: filesystem says no
		errors.Is(ferr, syscall.ENOSYS), // pre-2.6.31, or stubbed out by a sandbox
		errors.Is(ferr, syscall.EINVAL), // arguments are fixed here, so: the fs rejects the shape
		errors.Is(ferr, syscall.EPERM),  // seccomp filters commonly answer EPERM
		errors.Is(ferr, syscall.ENODEV): // not a regular file
		return f.Truncate(size) // no fallocate support here; sparse it is
	default:
		// Name the file, and keep the errno reachable through Unwrap so callers can
		// still test for syscall.ENOSPC.
		return &os.PathError{Op: "fallocate", Path: f.Name(), Err: ferr}
	}
}
