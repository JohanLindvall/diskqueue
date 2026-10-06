// SPDX-License-Identifier: MIT

package diskqueue_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/JohanLindvall/diskqueue"
)

// tempDir gives each example its own queue directory. A directory holds one
// queue: New takes an advisory lock on it.
func tempDir() string {
	d, err := os.MkdirTemp("", "diskqueue-example")
	if err != nil {
		log.Fatal(err)
	}
	return d
}

// A zero-allocation codec. MarshalFunc must APPEND to dst and return the
// extended slice — returning a fresh slice instead works, but costs the
// allocation the reused buffer exists to avoid.
func marshal(dst []byte, v uint64) ([]byte, error) {
	return binary.LittleEndian.AppendUint64(dst, v), nil
}

func unmarshal(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.New("bad length")
	}
	return binary.LittleEndian.Uint64(data), nil
}

func Example() {
	q, err := diskqueue.New[uint64](tempDir(), marshal, unmarshal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()

	for i := uint64(1); i <= 3; i++ {
		if err := q.Add(i); err != nil {
			log.Fatal(err)
		}
	}
	r := q.NewReader()
	for {
		v, ok, err := r.TryTake() // read and commit in one step
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			break
		}
		fmt.Println(v)
	}
	// Output:
	// 1
	// 2
	// 3
}

// Damage on disk is dropped and counted, never delivered as data and never left
// blocking the queue: flip one byte of a record in its segment file, as a bad
// sector would, and a reopened queue reports exactly that record lost and hands
// over the rest. The README's Go Playground link runs this program, so a change
// here wants a fresh share there.
func Example_recovery() {
	dir := tempDir()
	text := func(dst []byte, s string) ([]byte, error) { return append(dst, s...), nil }
	untext := func(b []byte) (string, error) { return string(b), nil }

	q, err := diskqueue.New[string](dir, text, untext)
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range []string{"alpha", "bravo", "charlie"} {
		if err := q.Add(s); err != nil {
			log.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		log.Fatal(err)
	}

	// Find "alpha" in the segment file and flip one of its bytes.
	segs, err := filepath.Glob(filepath.Join(dir, "data.*"))
	if err != nil || len(segs) != 1 {
		log.Fatal("expected one segment: ", segs, err)
	}
	b, err := os.ReadFile(segs[0])
	if err != nil {
		log.Fatal(err)
	}
	i := bytes.Index(b, []byte("alpha"))
	if i < 0 {
		log.Fatal("record not found on disk")
	}
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{b[i] ^ 0xff}, int64(i)); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	q, err = diskqueue.New[string](dir, text, untext)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	r := q.NewReader()
	for {
		v, ok, err := r.TryTake()
		switch {
		case errors.Is(err, diskqueue.ErrCorrupt):
			fmt.Println("dropped:", err)
			continue
		case err != nil:
			log.Fatal(err)
		case !ok:
			fmt.Println("lost records:", q.Stats().LostRecords, "- still queued:", q.Count())
			return
		}
		fmt.Println(v)
	}
	// Output:
	// dropped: diskqueue: corrupt: record checksum
	// bravo
	// charlie
	// lost records: 1 - still queued: 0
}

// Reserve/Commit is the at-least-once path: the record is not retired until you
// say so, so a crash between the two replays it.
func ExampleReader_Reserve() {
	q, err := diskqueue.New[uint64](tempDir(), marshal, unmarshal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	if err := q.Add(42); err != nil {
		log.Fatal(err)
	}

	r := q.NewReader()
	v, ok, offset, err := r.Reserve(context.Background())
	if err != nil || !ok {
		log.Fatal(err)
	}
	// ... process v; only acknowledge once it is safely handled.
	if err := r.Commit(offset); err != nil {
		log.Fatal(err)
	}
	fmt.Println(v, q.Count())
	// Output: 42 0
}

// Corruption never stops the queue: damage is dropped, counted and reported as
// one ErrCorrupt, and the next call makes progress. This is the loop to write.
func ExampleReader_TryTake_corruption() {
	q, err := diskqueue.New[uint64](tempDir(), marshal, unmarshal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	for i := uint64(1); i <= 2; i++ {
		if err := q.Add(i); err != nil {
			log.Fatal(err)
		}
	}

	rd := q.NewReader()
	var lost int
	for {
		v, ok, err := rd.TryTake()
		switch {
		case errors.Is(err, diskqueue.ErrCorrupt):
			// Already dropped and stepped past; count it and go round again.
			lost++
			continue
		case err != nil:
			log.Fatal(err)
		case !ok:
			fmt.Println("drained, lost", lost, "of", q.Stats().Added)
			return
		}
		_ = v
	}
	// Output: drained, lost 0 of 2
}

// An iterator cannot carry an error per item, so check Err after the loop: a nil
// Err means it ended because the queue ran out, not because something failed.
func ExampleReader_Drain() {
	q, err := diskqueue.New[uint64](tempDir(), marshal, unmarshal)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	for i := uint64(1); i <= 3; i++ {
		if err := q.Add(i); err != nil {
			log.Fatal(err)
		}
	}

	rd := q.NewReader()
	sum := uint64(0)
	for v := range rd.Drain(context.Background()) {
		sum += v
	}
	if err := rd.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(sum)
	// Output: 6
}
