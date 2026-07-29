//go:build diskqueue_faults

package diskqueue

// Built with `-tags diskqueue_faults`, the store gains named injection points at
// the boundaries whose *ordering* is the durability invariant. There is no other
// way to reach them: the failure a test needs is "this fsync fails while the
// write before it succeeded", and no descriptor trick produces that.
//
// The seam is a build tag rather than a package-level function variable on
// purpose. A mutable global would make every fsync call indirect in the shipped
// library, force these tests to run non-parallel, and put a test hook in the
// public binary. This way the default build has no seam at all.
//
// Tests set faultHook; it is not safe for concurrent use, which is fine because
// the tests that use it drive one queue at a time.
var faultHook func(name string) error

// faultPoint reports the injected failure for name, if a test asked for one.
func faultPoint(name string) error {
	if faultHook == nil {
		return nil
	}
	return faultHook(name)
}
