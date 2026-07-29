//go:build !(linux || darwin || dragonfly || freebsd || illumos || netbsd || openbsd)

package diskqueue

import "os"

// tryLockDir is a no-op on platforms where the standard library exposes no
// advisory whole-file lock (Windows, Solaris, AIX, plan9, js/wasm). Opening the
// same queue directory twice is then the caller's responsibility.
func tryLockDir(*os.File) error { return nil }
