package engine

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"sync"

	"github.com/wv0m56/prefixed/plugin/origin"
	"github.com/wv0m56/prefixed/skiplist"
)

// Engine wraps a skiplist data structure with all the goodies
// associated with caching (TTL, cache-filling mechanism, etc).
type Engine struct {
	sync.RWMutex
	s        *skiplist.Skiplist
	fillCond map[string]*condition
	o        origin.Origin
	// c      client.ClientPlugin
}

// NewEngine creates a new cache engine with a skiplist as the
// underlying data structure. Use expectedLen <= 0 for default (10 million).
// NewEngine panics if expectedLen is positive and is less than 1024 (pointless).
func NewEngine(expectedLen int, o origin.Origin) (*Engine, error) {

	if expectedLen > 0 && expectedLen < 1024 {
		return nil, errors.New("non default expectedLen must be >= 1024")
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
		make(map[string]*condition),
		o,
	}, nil
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

	r := e.tryget(key)
	if r != nil { // cache hit
		return r, nil
	}

	// cache miss
	r, err := e.CacheFill(key)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (e *Engine) tryget(key string) *bytes.Reader {
	e.RLock()
	defer e.RUnlock()
	if el, ok := e.s.Get(key); ok && el != nil {
		return el.ValReader()
	}
	return nil
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

// CacheFill fetches value from origin and upserts it into the engine. The
// returned channel is used to signal when the keys are done cache-filling.
func (e *Engine) CacheFill(key string) (*bytes.Reader, error) {

	e.Lock()
	if el, ok := e.s.Get(key); ok && el != nil {
		e.Unlock()
		return el.ValReader(), nil
	}

	// still locked
	if cond, ok := e.fillCond[key]; ok && cond != nil {

		cond.count++
		return blockUntilFilled(e, key)

	} else if ok && cond == nil {

		// must never reach here
		e.Unlock()
		return nil, errors.New("nil condition during cache fill")

	} else {

		e.fillCond[key] = &condition{*sync.NewCond(e), 1, nil, nil}

		go func() {
			// fetch from remote and fill up buffer
			rc := e.o.Fetch(key)
			buf := &bytes.Buffer{}
			_, err := io.Copy(buf, rc)
			if err != nil {
				rc.Close()
				e.Lock()
				e.fillCond[key].err = err
				e.Unlock()
				e.fillCond[key].Broadcast()
				return
			}

			e.Lock()
			e.fillCond[key].buf = buf
			e.Unlock()
			rw := &RowWriter{key, buf, e}
			rw.Commit()
			e.fillCond[key].Broadcast()
		}()

		return blockUntilFilled(e, key)
	}
}

type condition struct {
	sync.Cond
	count int
	buf   *bytes.Buffer
	err   error
}

func blockUntilFilled(e *Engine, key string) (*bytes.Reader, error) {

	e.fillCond[key].Wait() // try without loop

	if c := e.fillCond[key]; c.err != nil || c.buf == nil {
		return nil, errors.New("cache-fill failed")
	}

	e.fillCond[key].count--
	b := e.fillCond[key].buf.Bytes()
	if e.fillCond[key].count == 0 {
		delete(e.fillCond, key)
	}
	e.Unlock()

	return bytes.NewReader(b), nil
}

// SetTTL sets TTL values of the given keys.
func (e *Engine) SetTTL(t ...TTL) {
	//
}

// TTL is the data pair to be passed into SetTTL().
type TTL struct {
	Key string
	TTL int
}
