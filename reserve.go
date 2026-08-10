package diskqueue

// The reservation ledger: what makes Reader.Ack safe to call out of order.
//
// Commit is a prefix operation — it retires the record at an offset and every
// record before it — which is what makes a batch retire cheap (reserve N, commit
// the last offset once) but also means one worker's commit retires another
// worker's in-flight record. Competing consumers finish out of order by nature,
// so that pattern needs a per-record acknowledgement, and a per-record
// acknowledgement needs somewhere to remember the records that are outstanding.
//
// That is this: a small in-memory ledger of the records Reserve has handed out
// and Ack has not yet retired, kept in read order. Ack marks one entry, then the
// commit cursor advances across the longest unbroken run of acknowledged entries
// starting at the cursor. A gap — a record another worker reserved and has not
// acknowledged — stops the run, so an out-of-order acknowledgement never retires
// work nobody said was done.
//
// The ledger is session state and is deliberately not persisted. An entry that
// is acknowledged but still behind a gap when the process dies is simply not
// durable, so its record replays: exactly the at-least-once contract Reserve and
// Commit already promise. Nothing here can retire a record on its own.
//
// Its size is the consumer's outstanding reservations — the records it has taken
// and not yet acknowledged — not the backlog. A consumer that reserves without
// acknowledging grows it, which is the same unbounded in-flight set that already
// holds Count() above zero; Stats().InFlightBytes is the number to watch.

// reservation is one record Reserve handed out that no commit has retired yet.
// Both boundaries are kept: end is what the consumer acknowledges with, and start
// is what proves the entry is contiguous with the record before it, so the drain
// below can never step across a record that reached the consumer some other way.
type reservation struct {
	start int64
	end   int64
	acked bool
}

// trackReserve records the boundary of the record a Reserve just handed over.
// It reads the frame the take established rather than taking it as an argument,
// because takeHead may quarantine a damaged segment on the way and the record's
// real start is then not where the cursor stood when the read began.
//
// Entries are appended in read order and the read cursor only moves forward
// between rewinds, so the ledger is sorted by end and findReserved can bisect it.
//
// The retired prefix is dropped here as well as in ackTo, and that is what bounds
// the ledger for a consumer that never acks at all: a Reserve/Commit loop retires
// through commitTo, which knows nothing about the ledger, so without this the
// entries would accumulate for the lifetime of the queue. Amortized O(1) — each
// entry is walked once on its way out.
func (s *store) trackReserve() {
	s.dropRetired()
	s.reserved = append(s.reserved, reservation{start: s.lastFrameAt, end: s.lastFrameEnd})
}

// dropRetired discards the leading entries the commit cursor has already passed.
// It runs at the top and the bottom of every ack, which is what keeps the ledger
// honest without hooking every path that can move the cursor: Commit, Take, Skip,
// Requeue and a quarantine all advance it, and all of them simply leave entries
// behind that this drops on the next ack.
func (s *store) dropRetired() {
	i := 0
	for i < len(s.reserved) && s.reserved[i].end <= s.commitOff {
		i++
	}
	if i == 0 {
		return
	}
	s.reserved = append(s.reserved[:0], s.reserved[i:]...)
	if len(s.reserved) == 0 && cap(s.reserved) > maxIdleReservations {
		// A burst of reservations must not pin its ledger for the queue's
		// lifetime, the same reason trimOver releases the oversized I/O buffers.
		s.reserved = nil
	}
}

// maxIdleReservations is how large an empty ledger may stay before it is released
// rather than kept for reuse. Sized so an ordinary consumer's steady-state
// in-flight set is never reallocated.
const maxIdleReservations = 1024

// findReserved bisects the ledger for the entry ending at end. The slice is
// sorted by end (see trackReserve), so this stays O(log n) for a consumer holding
// a deep in-flight set.
func (s *store) findReserved(end int64) (int, bool) {
	lo, hi := 0, len(s.reserved)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.reserved[mid].end < end {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(s.reserved) && s.reserved[lo].end == end {
		return lo, true
	}
	return 0, false
}

// ackedRunEnd reports how far the commit cursor may advance: the end of the
// longest run of acknowledged entries that starts at the cursor and is contiguous
// record by record. It returns the cursor itself when the run is empty.
//
// The contiguity test is not redundant with the acked flags. A Take whose commit
// failed, or a Skip by another Reader, moves the read cursor across a record that
// never entered the ledger; without the test an ack of the reservation behind
// that hole would commit across it and retire a record its own consumer was told
// to expect back.
func (s *store) ackedRunEnd() int64 {
	at := s.commitOff
	for i := range s.reserved {
		if !s.reserved[i].acked || s.reserved[i].start != at {
			break
		}
		at = s.reserved[i].end
	}
	return at
}

// ackTo marks the reservation ending at end acknowledged and commits across
// whatever contiguous run of acknowledgements that completes. found is false when
// end names no outstanding reservation, which the caller reports as
// ErrInvalidOffset.
//
// Acknowledging is idempotent from both directions: an end the cursor has already
// passed is accepted and does nothing, and a second ack of a still-outstanding
// entry re-marks a flag that is already set. The ledger is reconciled from the
// commit cursor afterwards rather than assumed to match, so a commit that failed
// part way leaves the ledger describing what actually happened and the next ack
// picks up from there.
func (s *store) ackTo(end int64) (bool, error) {
	s.dropRetired()
	if end <= s.commitOff {
		return true, nil // already retired, here or by another consume path
	}
	i, ok := s.findReserved(end)
	if !ok {
		return false, nil
	}
	s.reserved[i].acked = true
	return true, s.commitAckedRun()
}

// commitAckedRun advances the durable cursor across the contiguous acknowledged
// run at the ledger's head, if any, and reconciles the ledger with however far
// the commit actually got. A run still blocked behind an unacknowledged
// reservation commits nothing and is not an error. Shared by ackTo, AckBatch and
// retireFrame.
func (s *store) commitAckedRun() error {
	target := s.ackedRunEnd()
	if target <= s.commitOff {
		return nil // acknowledged, but a gap ahead still blocks the cursor
	}
	err := s.commitTo(target)
	s.dropRetired() // keep the ledger in step with however far the commit actually got
	return err
}

// retireFrame retires the record spanning [start, end) — one a consume op just
// took off the head — WITHOUT a consumer to acknowledge it: the record enters
// the ledger already acknowledged, so the durable cursor advances only across
// the contiguous acknowledged run and can never retire a reservation another
// consumer still holds. With nothing outstanding before it the record commits
// immediately; behind an outstanding reservation the retirement waits for that
// ack, and a crash meanwhile replays the record — the same at-least-once answer
// every other unflushed commit gives. Skip and Requeue retire their head this
// way; a prefix commitTo here would silently retire other consumers' in-flight
// records (the exact hazard the ledger exists to prevent).
//
// Appending keeps the ledger sorted: the frame was the read head, so its end is
// past every outstanding reservation's.
func (s *store) retireFrame(start, end int64) error {
	s.dropRetired()
	s.reserved = append(s.reserved, reservation{start: start, end: end, acked: true})
	return s.commitAckedRun()
}
