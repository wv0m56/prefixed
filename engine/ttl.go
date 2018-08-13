package engine

import (
	"sync"
	"time"

	"github.com/wv0m56/prefixed/skiplist"
)

// TTL is the data pair to be passed into SetTTL().
type TTL struct {
	Key     string
	Seconds int
}

// SetTTL sets TTL values in seconds of the given keys. For keys that don't
// exist or where Seconds <= 0, SetTTL is a no-op.
func (e *Engine) SetTTL(ttl ...*TTL) {
	e.setTTL(time.Second, ttl...)
}

// for testing with lower time resolution
func (e *Engine) setTTL(unit time.Duration, ttl ...*TTL) {
	e.ts.Lock()
	defer e.ts.Unlock()

	now := time.Now()
	for _, v := range ttl {
		if v.Seconds <= 0 {
			continue
		}

		e.rwm.RLock()
		if _, ok := e.s.Get(v.Key); !ok {
			e.rwm.RUnlock()
			continue

		} else {

			deadline := now.Add(time.Duration(int64(v.Seconds) * int64(unit)))
			if de, ok := e.ts.m[v.Key]; ok {
				e.ts.DelElement(de)
			}
			insertedTTL := e.ts.Insert(deadline, v.Key)
			e.ts.m[v.Key] = insertedTTL
		}
		e.rwm.RUnlock()
	}
}

// GetTTL returns the number of seconds left until expiry for the given keys, in
// the order in which keys are passed into args.
// Keys without TTL will yield negative values.
func (e *Engine) GetTTL(keys ...string) []float64 {
	e.ts.Lock()
	defer e.ts.Unlock()

	var t []float64
	now := time.Now()
	for _, k := range keys {
		d, ok := e.ts.m[k]
		if ok {
			t = append(t, d.Key().Sub(now).Seconds())
		} else {
			t = append(t, -1)
		}
	}

	return t
}

// RemoveTTL cancels the expiration of keys after a set period. If the TTL was
// not set in the first place for a given key, RemoveTTL is a no-op.
func (e *Engine) RemoveTTL(keys ...string) {
	e.ts.Lock()
	defer e.ts.Unlock()

	for _, k := range keys {
		if de, ok := e.ts.m[k]; !ok {
			continue
		} else {
			e.ts.DelElement(de)
			delete(e.ts.m, k)
		}
	}
}

type ttlStore struct {
	sync.Mutex
	skiplist.Duplist
	m map[string]*skiplist.DupElement
	e *Engine
}

// to be invoked as a goroutine e.g. go startLoop()
func (ts *ttlStore) startLoop(step time.Duration) {

	for range time.Tick(step) {
		ts.Lock()

		now := time.Now()
		var dataKeysToDelete []string
		for f := ts.First(); f != nil && now.After(f.Key()); f = ts.First() {

			dataKeysToDelete = append(dataKeysToDelete, f.Val())
			ts.DelFirst()
			delete(ts.m, f.Val())
		}

		if len(dataKeysToDelete) > 0 {
			ts.e.rwm.Lock()
			ts.e.delWithoutTTLRemoval(dataKeysToDelete...)
			ts.e.rwm.Unlock()
		}

		ts.Unlock()
	}
}
