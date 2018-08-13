package engine

import (
	"sync"
	"time"

	"github.com/wv0m56/prefixed/skiplist"
)

func (e *Engine) setExpiry(key string, expiry time.Time) {
	e.ts.Lock()
	defer e.ts.Unlock()

	if de, ok := e.ts.m[key]; ok {
		e.ts.DelElement(de)
	}
	insertedTTL := e.ts.Insert(expiry, key)
	e.ts.m[key] = insertedTTL
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
