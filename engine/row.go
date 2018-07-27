package engine

import (
	"bytes"
	"errors"
)

// RowWriter implements io.Writer. Origin plugin authors should create a RowWriter
// object by calling RowWriter() method from the engine object, and never directly.
type RowWriter struct {
	key string
	b   *bytes.Buffer
	e   *Engine
}

// Write satisfies io.Writer. When there's an error, n is negative.
func (rw *RowWriter) Write(p []byte) (n int, err error) {
	if rw.b == nil {
		return -1, errors.New("can't use directly created RowWriter")
	}
	return rw.b.Write(p)
}

// Commit upserts the written (key, []byte) pair into the engine.
func (rw *RowWriter) Commit() {
	if rw.e == nil {
		return
	}
	rw.e.Lock()
	rw.e.s.Upsert(rw.key, rw.b.Bytes())
	rw.e.Unlock()
}

func (rw *RowWriter) Key() string {
	return rw.key
}
