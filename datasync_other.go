//go:build !linux

package diskqueue

import "os"

// datasync flushes f to stable storage. Only Linux exposes fdatasync(2) through
// the standard library, so everywhere else this is a full fsync — correct, just
// more than is strictly needed for a preallocated segment.
func datasync(f *os.File) error { return f.Sync() }
