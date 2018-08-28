package engine

import (
	"time"

	"github.com/wv0m56/prefixed/skiplist"
)

func (e *Engine) setExpiry(key string, expiry time.Time) {

	if de, ok := e.ts.m[key]; ok {
		e.ts.DelElement(de)
	}
	insertedTTL := e.ts.Insert(expiry, key)
	e.ts.m[key] = insertedTTL
}

// GetTTL returns the number of seconds left until expiry for the given keys, in
// the order in which keys are passed into args.
// Keys without TTL yields negative values.
func (e *Engine) GetTTL(keys ...string) []float64 {

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
	skiplist.Duplist
	m map[string]*skiplist.DupElement
	e *Engine
}

// to be invoked as a goroutine e.g. go startLoop()
func (ts *ttlStore) startLoop(step time.Duration) {

	for range time.Tick(step) {

		var somethingExpired bool
		now := time.Now()

		ts.e.rwm.RLock()
		if f := ts.First(); f != nil && now.After(f.Key()) {
			somethingExpired = true
		}
		ts.e.rwm.RUnlock()

		if somethingExpired {
			ts.e.rwm.Lock()
			for f := ts.First(); f != nil && now.After(f.Key()); f = ts.First() {
				ts.DelFirst()
				delete(ts.m, f.Val())
				if el := ts.e.dataStore.Del(f.Val()); el != nil {
					go ts.e.ep.dataDeletion(el.Key()) // stats, no need to be precise
				}
			}
			ts.e.rwm.Unlock()
		}
	}
}

// no lock
func (ts *ttlStore) del(key string) {
	de, _ := ts.e.ts.m[key]
	ts.DelElement(de)
	delete(ts.m, key)
}
