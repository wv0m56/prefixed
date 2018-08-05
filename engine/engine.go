package engine

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"sync"
	"time"

	"github.com/wv0m56/prefixed/plugin/origin"
	"github.com/wv0m56/prefixed/skiplist"
)

// Engine wraps a skiplist data structure with all the goodies
// associated with caching (TTL, cache-filling mechanism, etc).
type Engine struct {
	sync.RWMutex
	s        *skiplist.Skiplist
	fillCond map[string]*condition
	ts       *ttlStore
	o        origin.Origin
	// c      client.ClientPlugin
}

// NewEngine creates a new cache engine with a skiplist as the underlying data
// structure. Use expectedLen <= 0 for default (10 million). It's better
// to overestimate expectedLen than to underestimate it.
// NewEngine panics if expectedLen is positive and is less than 1024 (pointless).
func NewEngine(expectedLen int, o origin.Origin) (*Engine, error) {

	if expectedLen > 0 && expectedLen < 1024 {
		return nil, errors.New("non default expectedLen must be >= 1024")
	}

	log2 := func(i int) int {
		return int(math.Floor(math.Log2(float64(i))))
	}

	var n int
	if expectedLen <= 0 {
		n = log2(10 * 1000 * 1000 / 2)
	} else {
		n = log2(expectedLen / 2)
	}

	e := &Engine{
		sync.RWMutex{},
		skiplist.NewSkiplist(n),
		make(map[string]*condition),
		&ttlStore{
			sync.Mutex{},

			// assume 50% of elements will be TTL'ed
			// configurable later
			*(skiplist.NewDuplist(n - 1)),

			nil,
		},
		o,
	}

	e.ts.e = e

	// configurable later
	go e.ts.startLoop(300 * time.Millisecond)

	return e, nil
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

// CacheFill fetches value from origin and upserts it into the engine. The
// returned Reader yields the row's value after CacheFill if read from.
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
			rw := &rowWriter{key, nil, e}

			var err error
			if rc != nil {
				_, err = io.Copy(rw, rc)
			} else {
				err = errors.New("nil ReadCloser from Fetch")
			}

			if err != nil {

				if rc != nil {
					_ = rc.Close()
				}
				e.Lock()
				e.fillCond[key].err = err

			} else {

				e.Lock()
				rw.Commit()
				e.fillCond[key].b = rw.b.Bytes()
			}

			e.Unlock()
			e.fillCond[key].Broadcast()

			return
		}()

		return blockUntilFilled(e, key)
	}
}

type condition struct {
	sync.Cond
	count int
	b     []byte
	err   error
}

func blockUntilFilled(e *Engine, key string) (r *bytes.Reader, err error) {

	c := e.fillCond[key]
	for c.b == nil && c.err == nil {
		e.fillCond[key].Wait()
	}

	if c.err != nil {
		err = c.err
	}

	if b := c.b; b != nil {
		r = bytes.NewReader(e.fillCond[key].b)
	}

	e.fillCond[key].count--
	if e.fillCond[key].count == 0 {
		delete(e.fillCond, key)
	}

	e.Unlock()

	return
}

func (e *Engine) del(keys ...string) {
	e.Lock()
	for _, k := range keys {
		e.s.Del(k)
	}
	e.Unlock()
}

type rowWriter struct {
	key string
	b   *bytes.Buffer
	e   *Engine
}

func (rw *rowWriter) Write(p []byte) (n int, err error) {
	if rw.b == nil {
		rw.b = bytes.NewBuffer(nil)
	}
	return rw.b.Write(p)
}

// no locking.
func (rw *rowWriter) Commit() {
	rw.e.s.Upsert(rw.key, rw.b.Bytes())
}
