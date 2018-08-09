package engine

import (
	"sync"
	. "time"

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
	e.setTTL(Second, ttl...)
}

// for testing with lower time resolution
func (e *Engine) setTTL(unit Duration, ttl ...*TTL) {
	e.ts.Lock()
	defer e.ts.Unlock()

	now := Now()
	for _, v := range ttl {
		if v.Seconds <= 0 {
			continue
		}
		deadline := now.Add(Duration(int64(v.Seconds) * int64(unit)))
		de := e.ts.Insert(deadline, v.Key)
		e.ts.m[v.Key] = de
	}
}

// RemoveTTL cancels the expiration of keys after a set period. If the TTL was
// never set in the first place for a given key, RemoveTTL is a no-op.
func (e *Engine) RemoveTTL(keys ...string) {
	e.ts.Lock()
	defer e.ts.Unlock()

	for _, k := range keys {
		if de, ok := e.ts.m[k]; !ok {
			continue
		} else {
			e.ts.DelElement(de)
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
func (ts *ttlStore) startLoop(step Duration) {

	for range Tick(step) {
		ts.Lock()

		var keysToDelete []string
		for f := ts.First(); f != nil && f.Key().After(Now()); f = ts.First() {

			keysToDelete = append(keysToDelete, ts.First().Val())
			ts.DelFirst()
			delete(ts.m, f.Val())
		}

		ts.e.rwm.Lock()
		ts.e.del(keysToDelete...)
		ts.e.rwm.Unlock()

		ts.Unlock()
	}
}
