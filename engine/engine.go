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

	if opts.ExpectedLen < 1024 {
		return nil, errors.New("ExpectedLen must be >= 1024")
	}

	if opts.MaxPayloadTotalSize < 10*1000*1000 {
		return nil, errors.New("MaxPayloadTotalSize must be >= 10*1000*1000 bytes")
	}

	// log2(ExpectedLen)
	n := int(math.Floor(math.Log2(float64(opts.ExpectedLen / 2))))

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

// invoked by the ttl loop
func (e *Engine) delWithoutTTLRemoval(keys ...string) {
	for _, k := range keys {
		if el := e.dataStore.Del(k); el != nil {
			go e.ep.dataDeletion(el.Key()) // probabilistic
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
	rw.e.dataStore.Upsert(rw.key, rw.b.Bytes())
}

func (e *Engine) evictUntilFree(wantedFreeSpace int) {

	var enoughFreed bool

	// try searching ep graveyard
	//*******************************************************
	e.ep.Lock()
	for k := range e.ep.graveyard {
		if delEl := e.dataStore.Del(k); delEl != nil {

			{ // del from ttlStore
				de, _ := e.ts.m[k]
				e.ts.DelElement(de)
				delete(e.ts.m, k)
			}

			go e.ep.dataDeletion(k)

			if freeSpace := e.maxPayloadTotalSize - e.dataStore.PayloadSize(); freeSpace > int64(wantedFreeSpace) {
				enoughFreed = true
				break
			}
		}
	}
	e.ep.Unlock()
	//*******************************************************

	del := func(key string) {
		e.dataStore.Del(key)

		{ // del from ttlStore
			de, _ := e.ts.m[key]
			e.ts.DelElement(de)
			delete(e.ts.m, key)
		}

		go e.ep.dataDeletion(key)
	}

	// try ttl list
	//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	if !enoughFreed {
		e.ts.Lock()

		for it := e.ts.First(); it != nil; it = it.Next() {

			if key := it.Val(); !e.ep.isRelevant(key) { // bound for expiry and not relevant

				del(key)

				if freeSpace := e.maxPayloadTotalSize - e.dataStore.PayloadSize(); freeSpace > int64(wantedFreeSpace) {
					enoughFreed = true
					break
				}
			}
		}
		e.ts.Unlock()
	}
	//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

	// iterate over dataStore, indiscriminate, very inefficient at this point
	// time to rebuild cache with bigger RAM
	if !enoughFreed {
		e.ep.Lock()

		// still holding top level lock
		for i := 1; !enoughFreed; i *= 4 {

			for it := e.dataStore.First(); it != nil; it = it.Next() {

				if count := e.ep.cms.Count([]byte(it.Key())); count <= uint64(i) {

					del(it.Key())

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
