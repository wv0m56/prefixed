package engine

import "bytes"

// RowWriter implements io.Writer. Origin plugin authors should create a RowWriter
// object by calling RowWriter() method from the engine object, and never directly.
type RowWriter struct {
	key string
	b   *bytes.Buffer
	e   *Engine
}

// Write satisfies io.Writer, and will never return an error (although it could
// panic if the underlying buffer grows too large).
func (rw *RowWriter) Write(p []byte) (n int, err error) {
	return rw.b.Write(p)
}

// Commit upserts the written (key, []byte) pair into the engine.
func (rw *RowWriter) Commit() {
	rw.e.Lock()
	rw.e.s.Upsert(rw.key, rw.b.Bytes())
	rw.e.Unlock()
}
