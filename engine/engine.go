package engine

import (
	"bytes"
	"errors"
	"sync"

	"github.com/wv0m56/prefixed/skiplist"
)

// Engine wraps a skiplist data structure with all the goodies
// associated with caching (TTL, cache-filling mechanism, etc).
type Engine struct {
	s *skiplist.Skiplist
	sync.RWMutex
	fillMu map[string]sync.RWMutex
	// c      client.ClientPlugin
	// o      origin.OriginPlugin
}

func NewEngine() *Engine {
	return &Engine{
		skiplist.NewSkiplist(24),
		sync.RWMutex{},
		make(map[string]sync.RWMutex, 128),
	}
}

// Get returns a *bytes.Reader with the value associated with key as the
// underlying byte slice. Get triggers a cache fill upon cache miss.
func (e *Engine) Get(key string) (*bytes.Reader, error) {

	e.RLock()

	if el, ok := e.s.Get(key); ok {

		e.RUnlock()
		return el.ValReader(), nil
	}

	e.RUnlock()
	c, err := e.Load(key)
	if err != nil {
		return nil, err
	}
	<-c
	if el, ok := e.s.Get(key); ok {
		return el.ValReader(), nil
	}

	return nil, errors.New("value not found and cache-fill failed")
}

// GetCopy copies the byte slice associated with key into the returned []byte.
// GetCopy triggers a cache fill upon cache miss.
func (e *Engine) GetCopy(key string) ([]byte, error) {

	e.RLock()

	if el, ok := e.s.Get(key); ok {

		e.RUnlock()
		return el.ValCopy(), nil
	}

	e.RUnlock()
	c, err := e.Load(key)
	if err != nil {
		return nil, err
	}
	<-c
	if el, ok := e.s.Get(key); ok {
		return el.ValCopy(), nil
	}

	return nil, errors.New("value not found and cache-fill failed")
}

// GetByPrefix gets all the values reader associated with keys having prefix p.
// GetByPrefix does not trigger a cache fill upon cache miss. Returns nil if no
// value is associated with keys having prefix p.
func (e *Engine) GetByPrefix(p string) []*bytes.Reader {
	e.RLock()

	els := e.s.GetByPrefix(p)
	e.RUnlock()
	if els == nil {
		return nil
	}

	rs := make([]*bytes.Reader, len(els))
	for i, v := range els {
		rs[i] = v.ValReader()
	}
	return rs
}

// GetCopiesByPrefix is like GetByPrefix except it returns a slice of copies of
// []byte representing the values.
func (e *Engine) GetCopiesByPrefix(p string) [][]byte {
	e.RLock()
	defer e.RUnlock()

	els := e.s.GetByPrefix(p)
	if els == nil {
		return nil
	}

	rs := make([][]byte, len(els))
	for i, v := range els {
		rs[i] = v.ValCopy()
	}
	return rs
}

// RowWriter returns an object that satisfies io.Writer interface. Calling Write()
// on the returned object will write into a bytes buffer. Commit() will upsert
// (key, value bytes) into e.
func (e *Engine) RowWriter(key string) *RowWriter {
	return &RowWriter{key, &bytes.Buffer{}, e}
}

// Load fetches values from origin and upserts them into the engine. The returned
// channel is used to signal when the keys are done cache-filling.
func (e *Engine) Load(keys ...string) (<-chan struct{}, error) {

	// TODO TODO TODO TODO
	// dumb placeholder for simple test
	c := make(chan struct{})
	go func() {
		c <- struct{}{}
	}()
	return c, nil
}

// SetTTL sets TTL values of the given keys.
func (e *Engine) SetTTL(t ...TTLKV) {
	//
}

// TTLKV is the data pair to be passed into SetTTL().
type TTLKV struct {
	Key string
	Val int
}
