//go:build !linux

package diskqueue

import "os"

// preallocate extends f to size. Only Linux exposes a block-reserving
// fallocate(2) through the standard library, so everywhere else this is an
// ftruncate: the segment reads back as size bytes of zeros, but its blocks are
// not reserved, so a full filesystem surfaces as a write error during an append
// rather than at segment creation.
func preallocate(f *os.File, size int64) error { return f.Truncate(size) }
