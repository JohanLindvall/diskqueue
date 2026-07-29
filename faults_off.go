//go:build !diskqueue_faults

package diskqueue

// faultPoint is the no-op the default build compiles. It inlines to a nil
// constant, so every `if err := faultPoint(...); err != nil` in the hot path
// folds away entirely — the shipped binary is byte-identical to one with no seam
// at all, which is why the injection points can sit in append without a
// benchmark to defend them.
func faultPoint(string) error { return nil }
