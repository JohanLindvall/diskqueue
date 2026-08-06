//go:build linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd

package diskqueue

import (
	"errors"
	"os"
	"syscall"
)

// tryLockDir takes a non-blocking exclusive advisory lock on the open queue
// directory, so a second Queue — in this process or another — cannot open the
// same directory and interleave writes into it. It returns ErrLocked when someone
// else already holds the lock. The lock is released when the handle is closed
// (store.close), including when the process dies.
//
// The lock is advisory (flock(2)); a process that never asks is not stopped.
// Filesystems without lock support answer ENOTSUP/ENOLCK/EINVAL — there we run
// unlocked rather than refusing to open.
func tryLockDir(d *os.File) error {
	ctlErr, lerr := fdControl(d, func(fd uintptr) error {
		return syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB)
	})
	if ctlErr != nil {
		return ctlErr
	}
	switch {
	case lerr == nil:
		return nil
	case errors.Is(lerr, syscall.EWOULDBLOCK): // EAGAIN: held by someone else
		return ErrLocked
	case errors.Is(lerr, syscall.ENOTSUP), errors.Is(lerr, syscall.ENOLCK),
		errors.Is(lerr, syscall.EINVAL), errors.Is(lerr, syscall.ENOSYS):
		return nil // no lock support on this filesystem; proceed unlocked
	default:
		return lerr
	}
}
