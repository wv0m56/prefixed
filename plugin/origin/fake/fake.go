// Package fake is a fake implementation of the Origin plugin for tests.
package fake

import (
	"bytes"
	"errors"
	"io"
	. "time"
)

// For tests. It implements the Origin interface.
type DelayedOrigin struct{}

// Fetch fetches dummy data. "error" as key simulates a network error should
// the returned io.ReadCloser is read. Else returns &bytes.Reader([]byte(key))
// implementing a no-op Close() method with a 100ms delay.
func (_ *DelayedOrigin) Fetch(key string) io.ReadCloser {
	return &fakeReadCloser{bytes.NewReader([]byte(key)), key}
}

type fakeReadCloser struct {
	br  *bytes.Reader
	key string
}

func (_ *fakeReadCloser) Close() error {
	return nil
}

func (frc *fakeReadCloser) Read(p []byte) (int, error) {
	if frc.key == "error" {
		Sleep(100 * Millisecond)
		return 0, errors.New("fake error")
	}
	Sleep(100 * Millisecond)
	return frc.br.Read(p)
}

type NoDelayOrigin struct{}

// No delay.
func (_ *NoDelayOrigin) Fetch(key string) io.ReadCloser {
	return &benchReadCloser{bytes.NewReader([]byte(key)), key}
}

type benchReadCloser struct {
	br  *bytes.Reader
	key string
}

func (_ *benchReadCloser) Close() error {
	return nil
}

func (brc *benchReadCloser) Read(p []byte) (int, error) {
	if brc.key == "bench error" {
		return 0, errors.New("fake bench error")
	}
	return brc.br.Read(p)
}
