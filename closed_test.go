package diskqueue

import (
	"context"
	"errors"
	"testing"
)

// Close is a contract, not just a cleanup: "Further use returns ErrClosed."
// Every entry point has that guard and none of them were asserted, so a method
// added later — or one whose guard is dropped in a refactor — would silently
// operate on a closed store, reaching a nil directory handle and a files slice
// whose descriptors are gone.

// TestEveryMethodRejectsUseAfterClose walks the whole public surface.
func TestEveryMethodRejectsUseAfterClose(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true, MaxSegments: -1})
	if err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	if err := w.Add(1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Every op that mutates or reads the store must refuse.
	refusing := []struct {
		name string
		call func() error
	}{
		{"Add", func() error { return w.Add(2) }},
		{"Sync", func() error { return w.Sync() }},
		{"Close", func() error { return w.Close() }},
		{"Rewind", func() error { _, err := w.Rewind(); return err }},
		{"Reader.TryReserve", func() error { _, _, _, err := r.TryReserve(); return err }},
		{"Reader.TryTake", func() error { _, _, err := r.TryTake(); return err }},
		{"Reader.Reserve", func() error { _, _, _, err := r.Reserve(ctx); return err }},
		{"Reader.Take", func() error { _, _, err := r.Take(ctx); return err }},
		{"Reader.Commit", func() error { return r.Commit(0) }},
		{"Reader.Skip", func() error { _, err := r.Skip(); return err }},
	}
	for _, tc := range refusing {
		if err := tc.call(); !errors.Is(err, ErrClosed) {
			t.Errorf("%s after Close: %v, want ErrClosed", tc.name, err)
		}
	}

	// The iterators cannot return an error, so they end immediately and leave
	// Err nil — the queue was closed, which is an ordinary reason to stop.
	for range r.Drain(ctx) {
		t.Error("Drain yielded an item after Close")
	}
	if err := r.Err(); err != nil {
		t.Errorf("Drain.Err after Close: %v, want nil", err)
	}
	for range r.Follow(ctx) {
		t.Error("Follow yielded an item after Close")
	}
	if err := r.Err(); err != nil {
		t.Errorf("Follow.Err after Close: %v, want nil", err)
	}

	// The accessors keep working: a shutdown scrape wants the final state, and
	// these read scalars that cannot fault.
	if got := w.Stats(); got.Added != 1 {
		t.Errorf("Stats().Added after Close = %d, want 1", got.Added)
	}
	// The record was added and never read, so it is still available: Close does
	// not consume, and the gauges keep describing the queue as it was left.
	if w.Empty() || w.Count() != 1 {
		t.Errorf("after Close: Empty=%v Count=%d, want false and 1", w.Empty(), w.Count())
	}
	_ = w.Size()
	_ = w.Err()

	// And a Reader made after Close is just as refused.
	if _, _, err := w.NewReader().TryTake(); !errors.Is(err, ErrClosed) {
		t.Errorf("a Reader created after Close: %v, want ErrClosed", err)
	}
}

// TestNewReaderAfterCloseDoesNotPanic: NewReader takes no lock and checks
// nothing, so it is worth stating that the Reader it returns is inert rather
// than dangerous.
func TestNewReaderAfterCloseDoesNotPanic(t *testing.T) {
	w, err := New[uint64](t.TempDir(), marshalU64, unmarshalU64, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	if _, err := r.Skip(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Skip: %v, want ErrClosed", err)
	}
	if r.Err() != nil {
		t.Fatalf("Err on a fresh Reader: %v, want nil", r.Err())
	}
}
