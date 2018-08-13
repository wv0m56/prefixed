// Package fake is a fake implementation of the Origin plugin for tests.
package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"
)

// For tests. It implements the Origin interface.
type DelayedOrigin struct{}

// Fetch fetches dummy data. "error" as key simulates a network error should
// the returned io.ReadCloser is read. Else returns &bytes.Reader([]byte(key))
// implementing a no-op Close() method with timeout delay.
func (do *DelayedOrigin) Fetch(key string, timeout time.Duration) io.ReadCloser {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &delayedReadCloser{bytes.NewReader([]byte(key)), key, ctx, cancel}
}

type delayedReadCloser struct {
	br     *bytes.Reader
	key    string
	ctx    context.Context
	cancel context.CancelFunc
}

func (drc *delayedReadCloser) Close() error {
	drc.cancel()
	return drc.ctx.Err()
}

func (drc *delayedReadCloser) Read(p []byte) (int, error) {

	select {

	case <-drc.ctx.Done():
		return 0, drc.ctx.Err()

	case <-time.After(100 * time.Millisecond):
		if drc.key == "error" {
			return 0, errors.New("fake error")
		}
		return drc.br.Read(p)
	}
}

type NoDelayOrigin struct{}

func (_ *NoDelayOrigin) Fetch(key string, _ time.Duration) io.ReadCloser {
	return &nodelayReadCloser{bytes.NewReader([]byte(key)), key}
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
