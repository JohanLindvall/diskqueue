//go:build !diskqueue_faults

package diskqueue

// faultsEnabled is an untyped constant false in the default build, so every
// `if faultsEnabled { ... }` block below is eliminated at compile time. The
// production binary is byte-identical to one with no seam at all — which is why
// the injection points can sit on the hot path without a benchmark to defend
// them.
const faultsEnabled = false

// faultPoint is never called in the default build.
func faultPoint(string) error { return nil }
