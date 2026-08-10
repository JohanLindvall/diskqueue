package diskqueue

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// Options.StampRecords moves the "when was this enqueued" envelope callers
// were wrapping around every record into the store: an 8-byte unix-nano stamp
// prepended INSIDE the payload — checksummed, counted and framed like any
// other payload byte — laid down at serialization and stripped back off before
// the codec, with the wait reported as Reader.LastAge. These tests pin the
// contract's four corners: the age is sane and survives a reopen, the framed
// sizes on both ends count the stamp (so the MaxBytes accounting unit is
// unchanged), Requeue preserves the original stamp, and the option off is
// byte-for-byte the old behavior — including that flipping it over an existing
// backlog is codec-level garbage reported as ErrCodec, never a panic and never
// a quarantine.

// stampOpts is the smallest stamping configuration; NoSync because durability
// is not what any of these tests observe.
var stampOpts = Options{NoSync: true, StampRecords: true}

// TestStampReportsAge: an added record's LastAge covers at least the wall time
// it actually waited, on both the reserve and the take path, and ages
// monotonically with the wait.
func TestStampReportsAge(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("second")); err != nil {
		t.Fatal(err)
	}

	const wait = 30 * time.Millisecond
	time.Sleep(wait)
	r := w.NewReader()
	v, ok, off, err := r.TryReserve()
	if err != nil || !ok || !bytes.Equal(v, []byte("first")) {
		t.Fatalf("TryReserve: v=%q ok=%v err=%v", v, ok, err)
	}
	age1 := r.LastAge()
	if age1 < wait {
		t.Fatalf("LastAge=%v after waiting %v: the stamp under-reports the wait", age1, wait)
	}
	if age1 > time.Minute {
		t.Fatalf("LastAge=%v: not a sane wait for a record stamped this test run", age1)
	}
	if err := r.Ack(off); err != nil {
		t.Fatal(err)
	}

	// The second record was enqueued no earlier and is read later, after another
	// wait: its age still covers its own wait, and the total wall time only grew.
	time.Sleep(wait)
	v, ok, err2 := r.TryTake()
	if err2 != nil || !ok || !bytes.Equal(v, []byte("second")) {
		t.Fatalf("TryTake: v=%q ok=%v err=%v", v, ok, err2)
	}
	if age2 := r.LastAge(); age2 < 2*wait {
		t.Fatalf("LastAge=%v after two %v waits: the age is not accumulating", age2, wait)
	}
}

// TestStampCountsInFramedSize: the stamp is payload, so the framed size grows
// by exactly stampLen and every reporter of that unit — AddSized, LastBytes,
// the backlog gauge — agrees. This is the assertion that keeps a MaxBytes
// budget meaning the same thing with stamping on.
func TestStampCountsInFramedSize(t *testing.T) {
	m, u := bytesCodec()
	payload := []byte("twelve bytes")

	off, err := New[[]byte](t.TempDir(), m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = off.Close() }()
	on, err := New[[]byte](t.TempDir(), m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = on.Close() }()

	nOff, err := off.AddSized(payload)
	if err != nil {
		t.Fatal(err)
	}
	nOn, err := on.AddSized(payload)
	if err != nil {
		t.Fatal(err)
	}
	if nOn != nOff+stampLen {
		t.Fatalf("AddSized with stamps = %d, without = %d: want exactly +%d for the stamp", nOn, nOff, stampLen)
	}
	if got := on.Stats().BacklogBytes; got != nOn {
		t.Fatalf("BacklogBytes=%d, want the framed size %d: the gauges must count the same unit", got, nOn)
	}
	r := on.NewReader()
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("TryTake: ok=%v err=%v", ok, err)
	}
	if got := r.LastBytes(); got != nOn {
		t.Fatalf("LastBytes=%d, want %d: the consuming side must report the framed size stamp included", got, nOn)
	}
}

// TestStampSurvivesReopen: the stamp is payload, so it is as durable as the
// record — a reopen reports the age since the original enqueue, not since the
// restart.
func TestStampSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	m, u := bytesCodec()
	w, err := New[[]byte](dir, m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("persistent")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	const wait = 30 * time.Millisecond
	time.Sleep(wait)
	w2, err := New[[]byte](dir, m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()
	v, ok, err := r.TryTake()
	if err != nil || !ok || !bytes.Equal(v, []byte("persistent")) {
		t.Fatalf("TryTake after reopen: v=%q ok=%v err=%v", v, ok, err)
	}
	if age := r.LastAge(); age < wait {
		t.Fatalf("LastAge=%v across a reopen that took at least %v: the stamp did not survive", age, wait)
	}
}

// TestRequeuePreservesStamp: a rotation moves the raw payload, stamp inside,
// so the record's age keeps accumulating — it answers "how long has this item
// been waiting", and a rotation is not an answer to that.
func TestRequeuePreservesStamp(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := w.Add([]byte("poison")); err != nil {
		t.Fatal(err)
	}

	const wait = 30 * time.Millisecond
	time.Sleep(wait)
	r := w.NewReader()
	if ok, err := r.Requeue(); !ok || err != nil {
		t.Fatalf("Requeue: ok=%v err=%v", ok, err)
	}
	v, ok, err := r.TryTake()
	if err != nil || !ok || !bytes.Equal(v, []byte("poison")) {
		t.Fatalf("TryTake after Requeue: v=%q ok=%v err=%v", v, ok, err)
	}
	if age := r.LastAge(); age < wait {
		t.Fatalf("LastAge=%v after a %v wait and a Requeue: the rotation reset the stamp", age, wait)
	}
}

// TestStampOffUnchanged: with the option off nothing changes — a store written
// and read without stamps round-trips byte-for-byte, and LastAge is defined as
// 0 (there is no stamp to age). This is the arm that keeps every existing
// caller's bytes exactly as they were.
func TestStampOffUnchanged(t *testing.T) {
	dir := t.TempDir()
	m, u := bytesCodec()
	w, err := New[[]byte](dir, m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{0x00, 0x01, 0x02} // shorter than a stamp, and binary
	if err := w.Add(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New[[]byte](dir, m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()
	v, ok, err := r.TryTake()
	if err != nil || !ok || !bytes.Equal(v, payload) {
		t.Fatalf("round-trip with stamps off: v=%v ok=%v err=%v", v, ok, err)
	}
	if age := r.LastAge(); age != 0 {
		t.Fatalf("LastAge=%v with StampRecords off, want 0", age)
	}
}

// TestStampFlipIsCodecError: reopening a backlog written WITHOUT stamps with
// stamping ON is the caller error Options.StampRecords documents, and this is
// what it costs — each record too short to carry a stamp surfaces as ErrCodec
// (the same class as any undecodable record: left at the head, nothing
// counted as corruption, Skip the way past), never a panic. TryPeek previews
// the same verdict without consuming anything.
func TestStampFlipIsCodecError(t *testing.T) {
	dir := t.TempDir()
	m, u := bytesCodec()
	w, err := New[[]byte](dir, m, u, Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte{0xEE}); err != nil { // 1 byte: shorter than any stamp
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := New[[]byte](dir, m, u, stampOpts) // the documented caller error
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	r := w2.NewReader()

	if _, _, err := r.TryPeek(); !errors.Is(err, ErrCodec) {
		t.Fatalf("TryPeek of a short stamped record: %v, want ErrCodec", err)
	}
	_, ok, _, rerr := r.TryReserve()
	if ok || !errors.Is(rerr, ErrCodec) {
		t.Fatalf("TryReserve of a short stamped record: ok=%v err=%v, want ErrCodec", ok, rerr)
	}
	if errors.Is(rerr, ErrCorrupt) {
		t.Fatalf("TryReserve: %v claims disk corruption for a caller-side codec mismatch", rerr)
	}
	// The record is left at the head — a codec failure must never consume data
	// silently — so it is still there to be skipped.
	if got := w2.Count(); got != 1 {
		t.Fatalf("Count=%d after the codec error, want 1: the record must stay queued", got)
	}
	if st := w2.Stats(); st.Corruptions != 0 || st.LostRecords != 0 {
		t.Fatalf("Corruptions=%d LostRecords=%d: a codec mismatch booked as disk damage", st.Corruptions, st.LostRecords)
	}
	if ok, err := r.Skip(); !ok || err != nil {
		t.Fatalf("Skip past the short record: ok=%v err=%v", ok, err)
	}
	if !w2.Empty() {
		t.Fatal("queue not empty after skipping the only record")
	}
}

// TestStampAddBatch: AddBatch marshals under the lock through a different
// buffer than Add, so the stamping there is a separate code path — each item
// of a batch gets its own stamp and round-trips with a sane age.
func TestStampAddBatch(t *testing.T) {
	m, u := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, u, stampOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	items := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}
	if n, err := w.AddBatch(items); n != len(items) || err != nil {
		t.Fatalf("AddBatch: n=%d err=%v", n, err)
	}

	const wait = 20 * time.Millisecond
	time.Sleep(wait)
	r := w.NewReader()
	for _, want := range items {
		v, ok, err := r.TryTake()
		if err != nil || !ok || !bytes.Equal(v, want) {
			t.Fatalf("TryTake: v=%q ok=%v err=%v, want %q", v, ok, err, want)
		}
		if age := r.LastAge(); age < wait {
			t.Fatalf("LastAge=%v for a batch item after a %v wait", age, wait)
		}
		if want := framedLen(len(want) + stampLen); r.LastBytes() != want {
			t.Fatalf("LastBytes=%d, want %d (stamp included)", r.LastBytes(), want)
		}
	}
}
