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
	rwm       *sync.RWMutex
	dataStore *skiplist.Skiplist
	fillCond  map[string]*condition
	ts        *ttlStore
	ep        *evictPolicy
	o         origin.Origin
	// c      client.ClientPlugin
	timeout             time.Duration
	maxPayloadTotalSize int64
}

type Options struct {

	// ExpectedLen is the number of expected (k, v) rows in the cache.
	// It's better to overestimate ExpectedLen than to underestimate it.
	// NewEngine panics if ExpectedLen less than 1024 (pointless).
	ExpectedLen int64

	EvictPolicyRelevanceWindow time.Duration
	EvictPolicyTickStep        time.Duration
	TtlTickStep                time.Duration
	CacheFillTimeout           time.Duration

	// MaxPayloadTotalSize is the total sum of the length of all
	// value/payload (in bytes) from all rows.
	// It must be greater than 10*1000*1000 bytes.
	MaxPayloadTotalSize int64

	O origin.Origin
}

var OptionsDefault = Options{
	ExpectedLen:                10 * 1000 * 1000,
	EvictPolicyRelevanceWindow: 24 * 3600 * time.Second,
	EvictPolicyTickStep:        1 * time.Second,
	TtlTickStep:                250 * time.Millisecond,
	CacheFillTimeout:           250 * time.Millisecond,
	MaxPayloadTotalSize:        4 * 1000 * 1000 * 1000, // 4G, dunno
	O:                          &fake.DelayedOrigin{},  // TODO: placeholder, must fix
}

// NewEngine creates a new cache engine with a skiplist as the underlying data
// structure.
func NewEngine(opts *Options) (*Engine, error) {

	{ // sanity checks

		if opts.ExpectedLen < 1024 {
			return nil, errors.New("ExpectedLen must be >= 1024")
		}

		if opts.MaxPayloadTotalSize < 10*1000*1000 {
			return nil, errors.New("MaxPayloadTotalSize must be >= 10*1000*1000 bytes")
		}

		if opts.CacheFillTimeout < 10*time.Millisecond {
			return nil, errors.New("cachefill timeout too small")
		}

		if opts.TtlTickStep < 1*time.Millisecond {
			return nil, errors.New("TTL tick step too small")
		}

		if opts.EvictPolicyTickStep < 1*time.Millisecond ||
			opts.EvictPolicyTickStep > opts.EvictPolicyRelevanceWindow {

			return nil, errors.New("evict policy tick step too small or bigger than relevance window")
		}

		if opts.EvictPolicyRelevanceWindow < 100*time.Millisecond {
			return nil, errors.New("evict policy relevance window too small")
		}
	}

	// log2(ExpectedLen)
	n := int(math.Floor(math.Log2(float64(opts.ExpectedLen / 2))))

	e := &Engine{
		&sync.RWMutex{},

		skiplist.NewSkiplist(n),

		make(map[string]*condition),

		&ttlStore{
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
			opts.EvictPolicyRelevanceWindow,
			map[string]struct{}{},
		},

		opts.O,

		opts.CacheFillTimeout,

		opts.MaxPayloadTotalSize,
	}

	e.ts.e = e

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

// GetWithTTL calls Get and GetTTL and returns the combined info.
func (e *Engine) GetWithTTL(key string) (*bytes.Reader, float64, error) {
	r, err := e.get(key)
	if err != nil {
		return nil, -1, err
	}
	ttl := e.GetTTL(key)
	return r, ttl[0], nil
}

func (e *Engine) get(key string) (*bytes.Reader, error) {

	go e.ep.addToWindow(key)

	r := e.tryget(key)
	if r != nil { // cache hit
		return r, nil
	}

	// cache miss
	r, err := e.cacheFill(key)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (e *Engine) tryget(key string) *bytes.Reader {
	e.rwm.RLock()
	defer e.rwm.RUnlock()
	if el, ok := e.dataStore.Get(key); ok && el != nil {
		return el.ValReader()
	}
	return nil
}

// GetByPrefix gets all the values reader associated with keys having prefix p.
// GetByPrefix does not trigger a cache fill upon cache miss. Returns nil if no
// value is associated with keys having prefix p.
func (e *Engine) GetByPrefix(p string) []*bytes.Reader {

	e.rwm.RLock()
	els := e.dataStore.GetByPrefix(p)
	e.rwm.RUnlock()

	if els == nil {
		return nil
	}

	rs := make([]*bytes.Reader, len(els))
	for i, v := range els {
		rs[i] = v.ValReader()
		go e.ep.addToWindow(v.Key())
	}
	return rs
}

// GetCopiesByPrefix is like GetByPrefix except it returns a slice of copies of
// []byte representing the values.
func (e *Engine) GetCopiesByPrefix(p string) [][]byte {

	e.rwm.RLock()
	els := e.dataStore.GetByPrefix(p)
	e.rwm.RUnlock()

	if els == nil {
		return nil
	}

	rs := make([][]byte, len(els))
	for i, v := range els {
		rs[i] = v.ValCopy()
		go e.ep.addToWindow(v.Key())
	}
	return rs
}

func (e *Engine) cacheFill(key string) (*bytes.Reader, error) {

	e.rwm.Lock()
	if el, ok := e.dataStore.Get(key); ok && el != nil {
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
	rc, exp := e.o.Fetch(key, e.timeout)
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

		if rowPayloadSize := rw.b.Len(); e.dataStore.PayloadSize()+int64(rowPayloadSize) > e.maxPayloadTotalSize {
			e.evictUntilFree(rowPayloadSize)
		}

		if exp != nil && exp.After(time.Now()) {
			rw.Commit()
			e.setExpiry(key, *exp)
		} else if exp == nil {
			rw.Commit()
		}

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
	rw.e.dataStore.Upsert(rw.key, rw.b.Bytes())
}

// still holding top level lock throughout
func (e *Engine) evictUntilFree(wantedFreeSpace int) {

	var enoughFreed bool

	// try searching ep graveyard
	//*******************************************************
	e.ep.Lock()
	for k := range e.ep.graveyard {

		if delEl := e.dataStore.Del(k); delEl != nil {

			e.ts.del(k)

			go e.ep.dataDeletion(k)

			if freeSpace := e.maxPayloadTotalSize - e.dataStore.PayloadSize(); freeSpace > int64(wantedFreeSpace) {
				enoughFreed = true
				break
			}
		}
	}
	e.ep.Unlock()
	//*******************************************************

	// iterate over dataStore, indiscriminate, very inefficient at this point
	// time to rebuild cache with bigger RAM
	if !enoughFreed {
		e.ep.Lock()

		for i := 1; !enoughFreed; i *= 4 {

			for it := e.dataStore.First(); it != nil; it = it.Next() {

				if !e.ep.isRelevant(it.Key()) ||
					e.ep.cms.Count([]byte(it.Key())) <= uint64(i) {

					e.dataStore.Del(it.Key())

					e.ts.del(it.Key())

					go e.ep.dataDeletion(it.Key())

					if freeSpace := e.maxPayloadTotalSize - e.dataStore.PayloadSize(); freeSpace > int64(wantedFreeSpace) {
						enoughFreed = true
						break
					}
				}
			}
		}
		e.ep.Unlock()
	}
}

// Invalidate deletes keys from the data, TTL, and evict policy store.
// Only invoke Invalidate as a last resort for manual intervention.
// Normally, control the invalidation process by setting sensible TTL
// values at origin.
func (e *Engine) Invalidate(keys ...string) {
	e.rwm.Lock()
	for _, v := range keys {
		e.dataStore.Del(v)
		e.ts.del(v)
		go e.ep.dataDeletion(v)
	}
	e.rwm.Unlock()
}
