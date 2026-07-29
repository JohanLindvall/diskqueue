//go:build !unix

package diskqueue

import "os"

// fsyncDir is a no-op off Unix. Flushing a directory handle is a POSIX notion:
// on Windows FlushFileBuffers rejects a directory handle outright, and plan9 and
// js/wasm have no equivalent — so attempting it would fail every segment cycle
// rather than making anything durable. Directory-entry durability there is
// whatever the platform provides.
func fsyncDir(*os.File) error { return nil }

// dirSyncUnsupported is never consulted here — fsyncDir cannot fail.
func dirSyncUnsupported(error) bool { return false }
