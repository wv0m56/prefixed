package engine

import (
	"bytes"
	"errors"
	"io/ioutil"
	"math"
	"sync"

	"github.com/wv0m56/prefixed/skiplist"
)

// Engine wraps a skiplist data structure with all the goodies
// associated with caching (TTL, cache-filling mechanism, etc).
type Engine struct {
	sync.RWMutex
	s      *skiplist.Skiplist
	fillMu map[string]sync.RWMutex
	// c      client.ClientPlugin
	// o      origin.OriginPlugin
}

// NewEngine creates a new cache engine with a skiplist as the
// underlying data structure. Use expectedLen <= 0 for default (10 million).
// NewEngine panics if expectedLen is positive and is less than 100.
func NewEngine(expectedLen int) *Engine {

	if expectedLen > 0 && expectedLen < 100 {
		panic("expectedLen must be >= 100")
	}

	var n float64
	if expectedLen <= 0 {
		n = float64(10 * 1000 * 1000)
	} else {
		n = math.Log2(float64(expectedLen) / 2)
	}

	return &Engine{
		sync.RWMutex{},
		skiplist.NewSkiplist(int(math.Floor(n))),
		make(map[string]sync.RWMutex, 128),
	}
}

// Get returns a *bytes.Reader with the value associated with key as the
// underlying byte slice. Get triggers a cache fill upon cache miss.
func (e *Engine) Get(key string) (r *bytes.Reader, err error) {
	return e.get(key)
}

// GetCopy copies the byte slice associated with key into the returned []byte.
// GetCopy triggers a cache fill upon cache miss.
func (e *Engine) GetCopy(key string) ([]byte, error) {
	r, err := e.get(key)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

func (e *Engine) get(key string) (*bytes.Reader, error) {
	e.RLock()

	if el, ok := e.s.Get(key); ok && el != nil {

		e.RUnlock()
		return el.ValReader(), nil
	}

	e.RUnlock()
	done, errCh := e.CacheFill(key)

	select {

	case <-done:
		if el, ok := e.s.Get(key); ok && el != nil {
			return el.ValReader(), nil
		}
		return nil, errors.New("value not found and cache-fill failed")

	case err := <-errCh:
		return nil, err
	}
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
	els := e.s.GetByPrefix(p)
	e.RUnlock()

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

// CacheFill fetches values from origin and upserts them into the engine. The
// returned channel is used to signal when the keys are done cache-filling.
func (e *Engine) CacheFill(keys ...string) (done <-chan struct{}, errCh <-chan error) {

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
