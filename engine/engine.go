package engine

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"sync"
	"time"

	"github.com/tylertreat/BoomFilters"
	"github.com/wv0m56/prefixed/plugin/origin"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
	"github.com/wv0m56/prefixed/skiplist"
)

// Engine wraps a skiplist data structure with all the goodies
// associated with caching (TTL, cache-filling mechanism, etc).
type Engine struct {
	rwm      *sync.RWMutex
	s        *skiplist.Skiplist
	fillCond map[string]*condition
	ts       *ttlStore
	ep       *evictPolicy
	o        origin.Origin
	// c      client.ClientPlugin
	timeout time.Duration
}

type EngineOptions struct {
	ExpectedLen                int
	EvictPolicyRelevanceWindow time.Duration
	EvictPolicyTickStep        time.Duration
	TtlTickStep                time.Duration
	CacheFillTimeout           time.Duration
	O                          origin.Origin
}

var EngineOptionsDefault EngineOptions = EngineOptions{

	// Use ExpectedLen <= 0 for default (10 million). It's better
	// to overestimate ExpectedLen than to underestimate it.
	// NewEngine panics if ExpectedLen is positive and is less than 1024 (pointless).
	ExpectedLen: 10 * 1000 * 1000,

	EvictPolicyRelevanceWindow: 24 * 3600 * time.Second,
	EvictPolicyTickStep:        1 * time.Second,
	TtlTickStep:                250 * time.Millisecond,
	CacheFillTimeout:           250 * time.Millisecond,
	O:                          &fake.DelayedOrigin{}, // TODO: placeholder, must fix
}

// NewEngine creates a new cache engine with a skiplist as the underlying data
// structure.
func NewEngine(opts *EngineOptions) (*Engine, error) {

	if opts.ExpectedLen > 0 && opts.ExpectedLen < 1024 {
		return nil, errors.New("non default ExpectedLen must be >= 1024")
	}

	log2 := func(i int) int {
		return int(math.Floor(math.Log2(float64(i))))
	}

	var n int
	if opts.ExpectedLen <= 0 {
		n = log2(10 * 1000 * 1000 / 2)
	} else {
		n = log2(opts.ExpectedLen / 2)
	}

	e := &Engine{
		&sync.RWMutex{},

		skiplist.NewSkiplist(n),

		make(map[string]*condition),

		&ttlStore{
			sync.Mutex{},

			// assume 50% of elements will be TTL'ed
			// configurable later
			*(skiplist.NewDuplist(n - 1)),

			map[string]*skiplist.DupElement{},
			nil,
		},

		&evictPolicy{
			sync.Mutex{},
			boom.NewCountMinSketch(0.001, 0.99),
			&linkedList{},
			map[string]*llElement{},
			nil,
			opts.EvictPolicyRelevanceWindow,
		},

		opts.O,

		opts.CacheFillTimeout,
	}

	e.ts.e = e
	e.ep.e = e

	go e.ts.startLoop(opts.TtlTickStep)
	go e.ep.startLoop(opts.EvictPolicyTickStep)

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

	defer e.ep.addToWindow(key)

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
	e.rwm.RLock()
	defer e.rwm.RUnlock()
	if el, ok := e.s.Get(key); ok && el != nil {
		return el.ValReader()
	}
	return nil
}

// GetByPrefix gets all the values reader associated with keys having prefix p.
// GetByPrefix does not trigger a cache fill upon cache miss. Returns nil if no
// value is associated with keys having prefix p.
func (e *Engine) GetByPrefix(p string) []*bytes.Reader {

	e.rwm.RLock()
	els := e.s.GetByPrefix(p)
	e.rwm.RUnlock()

	if els == nil {
		return nil
	}

	rs := make([]*bytes.Reader, len(els))
	for i, v := range els {
		rs[i] = v.ValReader()
		e.ep.addToWindow(v.Key())
	}
	return rs
}

// GetCopiesByPrefix is like GetByPrefix except it returns a slice of copies of
// []byte representing the values.
func (e *Engine) GetCopiesByPrefix(p string) [][]byte {

	e.rwm.RLock()
	els := e.s.GetByPrefix(p)
	e.rwm.RUnlock()

	if els == nil {
		return nil
	}

	rs := make([][]byte, len(els))
	for i, v := range els {
		rs[i] = v.ValCopy()
		e.ep.addToWindow(v.Key())
	}
	return rs
}

// CacheFill fetches value from origin and upserts it into the engine. The
// returned Reader yields the row's value after CacheFill if read from.
func (e *Engine) CacheFill(key string) (*bytes.Reader, error) {

	e.rwm.Lock()
	if el, ok := e.s.Get(key); ok && el != nil {
		e.rwm.Unlock()
		return el.ValReader(), nil
	}

	// still locked
	if cond, ok := e.fillCond[key]; ok && cond != nil {

		cond.count++
		return e.blockUntilFilled(key)

	} else if ok && cond == nil {

		// must never reach here
		e.rwm.Unlock()
		return nil, errors.New("nil condition during cache fill")

	} else {

		e.fillCond[key] = &condition{*sync.NewCond(e.rwm), 1, nil, nil}

		go e.firstFill(key)

		return e.blockUntilFilled(key)
	}
}

func (e *Engine) firstFill(key string) {

	// fetch from remote and fill up buffer
	rc := e.o.Fetch(key, e.timeout)
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
		e.rwm.Lock()
		e.fillCond[key].err = err

	} else {

		e.rwm.Lock()
		rw.Commit()
		e.fillCond[key].b = rw.b.Bytes()
	}

	e.fillCond[key].Broadcast()
	e.rwm.Unlock()

	return
}

type condition struct {
	sync.Cond
	count int
	b     []byte
	err   error
}

func (e *Engine) blockUntilFilled(key string) (r *bytes.Reader, err error) {

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

	e.rwm.Unlock()

	return
}

// Delete deletes keys from the engine, also removing all promises
// of TTL eviction and occurences from eviction policy statistics.
// No-op for keys that don't exist.
func (e *Engine) Delete(keys ...string) {
	e.rwm.Lock()
	e.RemoveTTL(keys...)
	e.delWithoutTTLRemoval(keys...)
	e.rwm.Unlock()
}

// invoked by the ttl loop
func (e *Engine) delWithoutTTLRemoval(keys ...string) {
	for _, k := range keys {
		if el := e.s.Del(k); el != nil {
			go e.ep.removeFromWindow(el.Key()) // probabilistic
		}
	}
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
