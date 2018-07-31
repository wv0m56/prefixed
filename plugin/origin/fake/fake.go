package fake

import (
	"bytes"
	"errors"
	"io"
	. "time"
)

// For tests. It implements the Origin interface.
type Impl struct{}

// Fetch fetches dummy data. "error" as key simulates a network error should
// the returned io.ReadCloser is read. Else returns &bytes.Reader([]byte(key))
// implementing a no-op Close() method with a 100ms delay.
func (fo *Impl) Fetch(key string) io.ReadCloser {
	return &fakeReadeCloser{bytes.NewReader([]byte(key)), key}
}

type fakeReadeCloser struct {
	br  *bytes.Reader
	key string
}

func (_ *fakeReadeCloser) Close() error {
	return nil
}

func (frc *fakeReadeCloser) Read(p []byte) (int, error) {
	if frc.key == "error" {
		Sleep(100 * Millisecond)
		return 0, errors.New("fake error")
	}
	Sleep(100 * Millisecond)
	return frc.br.Read(p)
}
