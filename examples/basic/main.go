// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/JohanLindvall/diskqueue"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	dir, err := os.MkdirTemp("", "diskqueue-basic-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	q, err := diskqueue.New[string](dir,
		func(dst []byte, s string) ([]byte, error) { return append(dst, s...), nil },
		func(data []byte) (string, error) { return string(data), nil },
	)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	if err := q.Add("hello, diskqueue"); err != nil {
		return err
	}

	r := q.NewReader()
	value, ok, offset, err := r.TryReserve()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("expected the item we just added")
	}
	fmt.Println(value) // Do your work before acknowledging.
	return r.Ack(offset)
}
