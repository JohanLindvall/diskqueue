// SPDX-License-Identifier: MIT

// This example leaves a JSON job unacknowledged, closes the queue, and opens
// it again to show how a worker resumes unfinished work.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/JohanLindvall/diskqueue"
)

type Job struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// JSON is convenient for application data, but this codec allocates. The
// uint64 codec in example_test.go demonstrates reusing the provided buffer.
func marshal(dst []byte, job Job) ([]byte, error) {
	data, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return append(dst, data...), nil
}

func unmarshal(data []byte) (Job, error) {
	var job Job
	err := json.Unmarshal(data, &job)
	return job, err
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	dir, err := os.MkdirTemp("", "diskqueue-replay-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	if err := leaveUnfinished(dir); err != nil {
		return err
	}
	return resume(dir)
}

func leaveUnfinished(dir string) (err error) {
	q, err := diskqueue.New[Job](dir, marshal, unmarshal)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	if err := q.Add(Job{ID: "job-1", Message: "send a welcome email"}); err != nil {
		return err
	}
	r := q.NewReader()
	job, ok, _, err := r.TryReserve()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("expected an unfinished job")
	}
	fmt.Println("reserved:", job.ID)
	// Closing flushes writes but does not acknowledge this reservation.
	fmt.Println("closing without acknowledgement")
	return nil
}

func resume(dir string) (err error) {
	q, err := diskqueue.New[Job](dir, marshal, unmarshal)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	r := q.NewReader()
	job, ok, offset, err := r.TryReserve()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("expected the unacknowledged job to replay")
	}
	fmt.Println("replayed:", job.ID)
	// Do idempotent work here, using job.ID to recognize a previous attempt.
	// A crash after processing but before Ack can deliver the job again.
	if err := r.Ack(offset); err != nil {
		return err
	}
	fmt.Println("remaining:", q.Count())
	return nil
}
