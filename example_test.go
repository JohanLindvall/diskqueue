package diskqueue_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"

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
