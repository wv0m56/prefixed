// Package fake is a fake implementation of the Origin plugin for tests.
package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"time"
)

// For tests. It implements the Origin interface.
type DelayedOrigin struct{}

// Fetch fetches dummy data. "error" as key simulates a network error should
// the returned io.ReadCloser is read. Else returns &bytes.Reader([]byte(key))
// implementing a no-op Close() method with 100ms delay. If timeout has
// elapsed and Fetch has not finished fetching data, it terminates and returns
// (err, nil).
func (do *DelayedOrigin) Fetch(key string, timeout time.Duration) (io.ReadCloser, *time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &delayedReadCloser{
		bytes.NewReader([]byte(key)),
		key,
		false,
		ctx,
		cancel,
	}, nil
}

type delayedReadCloser struct {
	br          *bytes.Reader
	key         string
	hasBeenRead bool
	ctx         context.Context
	cancel      context.CancelFunc
}

func (drc *delayedReadCloser) Close() error {
	drc.cancel()
	return drc.ctx.Err()
}

func (drc *delayedReadCloser) Read(p []byte) (int, error) {

	if !drc.hasBeenRead {
		// first time Read is called for key
		select {

		case <-drc.ctx.Done():
			return 0, drc.ctx.Err()

		case <-time.After(100 * time.Millisecond):
			if drc.key == "error" {
				return 0, errors.New("fake error")
			}
			drc.hasBeenRead = true
			return drc.br.Read(p)
		}

	} else {

		select {

		case <-drc.ctx.Done():
			return 0, drc.ctx.Err()

		default:
			return drc.br.Read(p)
		}
	}
}

type NoDelayOrigin struct{}

func (_ *NoDelayOrigin) Fetch(key string, _ time.Duration) (io.ReadCloser, *time.Time) {
	return &nodelayReadCloser{bytes.NewReader([]byte(key)), key}, nil
}

type nodelayReadCloser struct {
	br  *bytes.Reader
	key string
}

func (_ *nodelayReadCloser) Close() error {
	return nil
}

func (brc *nodelayReadCloser) Read(p []byte) (int, error) {
	if brc.key == "bench error" {
		return 0, errors.New("fake bench error")
	}
	return brc.br.Read(p)
}

type ExpiringOrigin struct{}

func (_ *ExpiringOrigin) Fetch(key string, _ time.Duration) (io.ReadCloser, *time.Time) {
	t := time.Now().Add(24 * time.Hour)
	return &nodelayReadCloser{bytes.NewReader([]byte(key)), key}, &t
}

// Origin whose value/payload is always a 10000 bytes long dummy content for
// all keys.
type ZeroesPayloadOrigin struct{}

func (_ *ZeroesPayloadOrigin) Fetch(_ string, _ time.Duration) (io.ReadCloser, *time.Time) {
	return &zeroesPayloadReadCloser{bytes.NewReader(make([]byte, 10000))}, nil
}

type zeroesPayloadReadCloser struct{ *bytes.Reader }

func (_ *zeroesPayloadReadCloser) Close() error {
	return nil
}

func (tbrc *zeroesPayloadReadCloser) Read(p []byte) (int, error) {
	return tbrc.Read(p)
}

// An origin which returns data with random expiry and random
// 1000-2000 bytes long payload.
type RandomOrigin struct{}

func (_ *RandomOrigin) Fetch(_ string, _ time.Duration) (io.ReadCloser, *time.Time) {
	t := time.Now().Add(time.Duration(rand.Int63n(30 * int64(time.Millisecond))))
	b := make([]byte, 1000+rand.Intn(1000))
	return &randomReadCloser{bytes.NewReader(b)}, &t
}

type randomReadCloser struct{ *bytes.Reader }

func (_ *randomReadCloser) Close() error {
	return nil
}

func (rrc *randomReadCloser) Read(p []byte) (int, error) {
	return rrc.Read(p)
}
