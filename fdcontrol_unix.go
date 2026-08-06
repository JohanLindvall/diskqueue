//go:build linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd

package diskqueue

import (
	"errors"
	"os"
	"syscall"
)

// fdControl runs op on f's raw descriptor, retrying while it answers EINTR —
// the shared shape of every raw syscall this package makes (fdatasync,
// fallocate, flock). ctlErr reports a failure to reach the descriptor at all
// (SyscallConn or Control); opErr is op's own answer. They stay separate
// because the callers' fallbacks differ: datasync degrades a ctlErr to a full
// Sync, while preallocate and tryLockDir report it as the operation's failure.
func fdControl(f *os.File, op func(fd uintptr) error) (ctlErr, opErr error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return err, nil
	}
	cerr := rc.Control(func(fd uintptr) {
		for {
			opErr = op(fd)
			if !errors.Is(opErr, syscall.EINTR) {
				return
			}
		}
	})
	return cerr, opErr
}
