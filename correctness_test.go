// SPDX-License-Identifier: MIT

package diskqueue

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestCorruptionRetirementWaitsForReservations(t *testing.T) {
	for _, framing := range []bool{false, true} {
		name := "checksum"
		if framing {
			name = "framing"
		}
		t.Run(name, func(t *testing.T) {
			w, dir := openAckQueue(t, ackOpts())
			addN(t, w, 3)
			at, damage := 17+1, []byte{0xff}
			if framing {
				at, damage = 17, unframeable
			}
			corruptData(t, w.st, 0, at, damage)
			r := w.NewReader()
			off := reserveN(t, r, 1)[0]
			if _, _, _, err := r.TryReserve(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("damaged record: %v", err)
			}
			if w.st.commitOff != 0 {
				t.Fatalf("corruption retired an outstanding reservation: commit=%d", w.st.commitOff)
			}
			if !framing {
				tail := reserveN(t, r, 1)[0]
				if err := r.Ack(tail); err != nil {
					t.Fatal(err)
				}
			}
			// A new append can share the damaged segment, including publication
			// by a group flush already in flight when the damage was detected.
			if err := w.Add(99); err != nil {
				t.Fatal(err)
			}
			v, ok, later, err := r.TryReserve()
			if err != nil || !ok || v != 99 {
				t.Fatalf("new record behind damage: %d, %v, %v", v, ok, err)
			}
			if err := r.Ack(off); err != nil {
				t.Fatal(err)
			}
			if w.Count() != 1 {
				t.Fatalf("retiring damage also retired the later reservation: %+v", w.Stats())
			}
			if err := r.Ack(later); err != nil {
				t.Fatal(err)
			}
			if w.Count() != 0 || w.Size() != 0 || !w.Empty() {
				t.Fatalf("ack left damaged backlog stranded: %+v", w.Stats())
			}
			if got := w.Stats().Corruptions; got != 1 {
				t.Fatalf("one damaged region reported %d times", got)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			if reopened.Count() != 0 || !reopened.Empty() {
				t.Fatalf("retired damage replayed: %+v", reopened.Stats())
			}
		})
	}
}

func TestAcknowledgementAfterSegmentDisappears(t *testing.T) {
	w, dir := openAckQueue(t, ackOpts())
	addN(t, w, 245)
	r := w.NewReader()
	first := reserveN(t, r, 1)[0]
	gone := w.st.files[0]
	quarantineEvictHandle(t, w.st, 0)
	if err := os.Remove(gone.path); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.TryReserve(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing segment: %v", err)
	}
	if w.st.commitOff != 0 {
		t.Fatal("missing segment retired an outstanding reservation")
	}
	if err := r.Ack(first); err != nil {
		t.Fatalf("acknowledging the delivered record after its file vanished: %v", err)
	}
	if got := w.Count(); got != 5 {
		t.Fatalf("remaining count=%d, want 5", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, reports := drainQueue(t, reopened.NewReader())
	if len(got) != 5 || got[0] != 240 || reports != 0 {
		t.Fatalf("surviving segment: %v, reports=%d", got, reports)
	}
}

func emptyTornStore(t *testing.T) string {
	t.Helper()
	s, dir := newTestStore(t, 32, 0)
	for i := 0; i < 4; i++ {
		mustAppend(t, s, idxRec(i))
	}
	last := s.active()
	last.header(setCommitCursor(headerSize+last.size), setCommittedCount(last.written))
	if err := s.writeHeader(last); err != nil {
		t.Fatal(err)
	}
	corruptData(t, s, 0, 0, unframeable)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	tearHeader(t, dir, 1)
	return dir
}

func TestEmptyTornSegmentReplaysFollowingCommits(t *testing.T) {
	dir := emptyTornStore(t)
	for attempt := 0; attempt < 2; attempt++ {
		reopened, err := openStore(dir, 32, 0, true, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		count := reopened.count()
		if err := reopened.close(); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("open %d: recovered %d records, want the following 2 to replay", attempt, count)
		}
	}
}

func TestPartialForceCommitKeepsCountsConsistent(t *testing.T) {
	s, dir := newTestStore(t, 32, 0)
	for i := 0; i < 6; i++ {
		mustAppend(t, s, idxRec(i))
	}
	reopenReadOnly(t, s, 1)
	if err := s.forceCommitAll(); err == nil {
		t.Fatal("force commit ignored a header write failure")
	}
	if s.count() != 4 || s.commitOff != s.files[1].base || s.headOff != s.commitOff {
		t.Fatalf("partial commit left inconsistent state: count=%d commit=%d head=%d",
			s.count(), s.commitOff, s.headOff)
	}
	repairHandle(t, s, 1)
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.close() }()
	if reopened.count() != 4 {
		t.Fatalf("reopen disagrees with partial commit: count=%d", reopened.count())
	}
}

func TestLatchedFailureRejectsAcknowledgementsBehindGaps(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)
	if err := r.Ack(offs[0]); err != nil {
		t.Fatal(err)
	}
	breakHandle(t, w.st, 0)
	if err := w.Sync(); !errors.Is(err, ErrIO) {
		t.Fatalf("expected a durability failure, got %v", err)
	}
	repairHandle(t, w.st, 0)
	for _, off := range []int64{offs[0], offs[2]} {
		if err := r.Ack(off); !errors.Is(err, ErrIO) {
			t.Errorf("Ack(%d) ignored the latched failure: %v", off, err)
		}
		if err := r.AckBatch(off); !errors.Is(err, ErrIO) {
			t.Errorf("AckBatch(%d) ignored the latched failure: %v", off, err)
		}
	}
	if _, err := w.AddBatch(nil); !errors.Is(err, ErrIO) {
		t.Errorf("AddBatch ignored the latched failure: %v", err)
	}
}

func TestAppendReportsEvictionDurabilityFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, 32, 0, false, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	for i := 0; i < 6; i++ {
		mustAppend(t, s, idxRec(i))
	}
	breakHandle(t, s, 0)
	if err := s.append(idxRec(6)); !errors.Is(err, ErrIO) {
		t.Fatalf("append ignored an eviction flush failure: %v", err)
	}
	if s.count() != 6 {
		t.Fatalf("failed append published a record: count=%d", s.count())
	}
	_ = s.close()
	reopened, err := openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.close() }()
	if reopened.count() != 6 {
		t.Fatalf("reopened count=%d, want 6", reopened.count())
	}
}

func TestCommitReportsEvictionDurabilityFailure(t *testing.T) {
	s, err := openStore(t.TempDir(), 32, 0, false, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.close() }()
	for i := 0; i < 8; i++ {
		mustAppend(t, s, idxRec(i))
	}
	// Reopening the first segment for this commit evicts the second, which is
	// still dirty. Its failed flush must reach this operation's caller.
	breakHandle(t, s, 1)
	if err := s.commitTo(idxRecLen); !errors.Is(err, ErrIO) {
		t.Fatalf("commit ignored an eviction flush failure: %v", err)
	}
	if s.commitOff != 0 {
		t.Fatal("commit published after the durability failure")
	}
}

func TestSegmentNumberExhaustionDoesNotReorderRecords(t *testing.T) {
	s, dir := newTestStore(t, 32, 0)
	mustAppend(t, s, idxRec(1))
	mustAppend(t, s, idxRec(2))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(s.filePath(1), s.filePath(^uint64(0))); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.append(idxRec(3)); err == nil {
		t.Error("segment numbering wrapped around and inserted a new segment before the backlog")
	}
	if err := reopened.close(); err != nil {
		t.Fatal(err)
	}
	again, err := openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.close() }()
	got, _ := drainRecovering(t, again)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("numbering overflow changed the persisted backlog: %v", got)
	}
}

func TestCreateDoesNotOverwriteAnExistingSegment(t *testing.T) {
	s, _ := newTestStore(t, 32, 0)
	mustAppend(t, s, idxRec(1))
	mustAppend(t, s, idxRec(2))
	path := s.filePath(s.nextNum)
	original := []byte("an existing file must not be truncated by create")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := s.append(idxRec(3)); !errors.Is(err, os.ErrExist) {
		t.Errorf("append overwrote an existing path: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("existing file changed: %q, %v", got, err)
	}
}

func TestBatchPanicWakesReadersForPublishedPrefix(t *testing.T) {
	for _, opts := range []Options{{}, {NoSync: true}, {SyncEvery: 2}} {
		opts.SegmentSize = 4096
		w, err := New[int](t.TempDir(), func(dst []byte, v int) ([]byte, error) {
			if v == 3 {
				panic("codec")
			}
			return append(dst, make([]byte, 3000)...), nil
		}, func([]byte) (int, error) { return 1, nil }, opts)
		if err != nil {
			t.Fatal(err)
		}
		// This is the same channel a blocked Reserve/Take/Follow waits on.
		waiter := make(chan struct{})
		w.notify = waiter
		func() {
			defer func() {
				if recover() == nil {
					t.Error("codec did not panic")
				}
			}()
			_, _ = w.AddBatch([]int{1, 2, 3})
		}()
		select {
		case <-waiter:
		default:
			t.Errorf("%+v: a published prefix left blocked readers asleep", opts)
		}
		want := 1
		if opts.SyncEvery == 2 {
			want = 2
		}
		if got := w.Count(); got != want {
			t.Errorf("published %d records, want %d", got, want)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDecodeFailurePreservesLastDeliveryMetrics(t *testing.T) {
	m, _ := bytesCodec()
	w, err := New[[]byte](t.TempDir(), m, func(b []byte) ([]byte, error) {
		if len(b) > 1 {
			return nil, errors.New("decode")
		}
		return b, nil
	}, Options{NoSync: true, StampRecords: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.AddBatch([][]byte{{1}, {2, 3}}); err != nil {
		t.Fatal(err)
	}
	r := w.NewReader()
	if _, ok, err := r.TryTake(); !ok || err != nil {
		t.Fatalf("read: %v, %v", ok, err)
	}
	bytes, age := r.LastBytes(), r.LastAge()
	if _, _, err := r.TryTake(); !errors.Is(err, ErrCodec) {
		t.Fatal(err)
	}
	if r.LastBytes() != bytes || r.LastAge() != age {
		t.Fatalf("failed decode replaced last delivery metrics: bytes %d -> %d, age %v -> %v",
			bytes, r.LastBytes(), age, r.LastAge())
	}
}

func TestFailedCommitCanBeRetried(t *testing.T) {
	w, dir := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)
	reopenReadOnly(t, w.st, 0)
	for i := 0; i < 2; i++ {
		if err := r.AckBatch(offs...); err == nil {
			t.Fatalf("attempt %d reported success without writing a header", i)
		}
		if w.Count() != 3 || w.st.commitOff != 0 || w.Stats().Committed != 0 {
			t.Fatalf("failed commit changed retirement: %+v", w.Stats())
		}
	}
	repairHandle(t, w.st, 0)
	if err := r.AckBatch(offs...); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New[uint64](dir, marshalU64, unmarshalU64, ackOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if reopened.Count() != 0 || !reopened.Empty() {
		t.Fatalf("successful retry was not persisted: %+v", reopened.Stats())
	}
}

func TestCommitRetainsOnlyPublishedPrefixAfterFailure(t *testing.T) {
	s, dir := newTestStore(t, 32, 0)
	for i := 0; i < 6; i++ {
		mustAppend(t, s, idxRec(i))
	}
	middle := s.files[1]
	reopenReadOnly(t, s, 1)
	if err := s.commitTo(s.writeOff); err == nil {
		t.Fatal("commit ignored an unwritable middle header")
	}
	if s.commitOff != middle.base || s.count() != 4 || s.nCommittedTotal != 2 {
		t.Fatalf("failed commit retired an unpublished suffix: %+v", s.stats())
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(dir, 32, 0, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.close() }()
	got, reports := drainRecovering(t, reopened)
	if reports != 0 || len(got) != 4 || got[0] != 2 || got[3] != 5 {
		t.Fatalf("failed commit changed the replayable suffix: %v, reports=%d", got, reports)
	}
}

func TestCommitCorruptionDoesNotPassItsTarget(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)
	corruptData(t, w.st, 0, 0, unframeable)
	_ = r.Commit(offs[0])
	if w.st.commitOff > offs[0] || w.Count() < 2 {
		t.Fatalf("damage in a prefix commit retired later reservations: %+v", w.Stats())
	}
}

func TestCommitCorruptionWakesBlockedReaders(t *testing.T) {
	w, _ := openAckQueue(t, ackOpts())
	addN(t, w, 3)
	r := w.NewReader()
	offs := reserveN(t, r, 3)
	waiter := make(chan struct{})
	w.notify = waiter // a reader waiting after every record was reserved
	corruptData(t, w.st, 0, 0, unframeable)
	if err := r.Commit(offs[2]); err != nil {
		t.Fatal(err)
	}
	if w.st.pendingCorrupt != 1 {
		t.Fatalf("expected a commit-time corruption report, got %d", w.st.pendingCorrupt)
	}
	select {
	case <-waiter:
	default:
		t.Fatal("a corruption report was queued without waking blocked readers")
	}
	if _, _, _, err := r.TryReserve(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("pending report: %v", err)
	}
	if !w.Empty() {
		t.Fatal("one corruption report should leave the queue empty")
	}
}

func TestReclaimedFileBehindPinnedPrefixIsNotLive(t *testing.T) {
	s, _ := newTestStore(t, 32, 0)
	for i := 0; i < 8; i++ {
		mustAppend(t, s, idxRec(i))
	}
	pinned, removed := s.files[0], s.files[1]
	pinned.pins++
	if err := s.commitTo(s.writeOff); err != nil {
		t.Fatal(err)
	}
	pinned.pins--
	if _, err := os.Stat(removed.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected reclaimed file: %v", err)
	}
	if s.live(removed) {
		t.Fatal("reclaimed file is still considered live behind a pinned prefix")
	}
}
