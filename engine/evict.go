package engine

import (
	"sync"
	"time"

	"github.com/tylertreat/BoomFilters"
	"github.com/wv0m56/prefixed/skiplist"
)

// evictPolicy is the data structure determining which row of the cache should
// be removed in case of space contention. evictPolicy maintains a
// RelevanceWindow, inside of which access frequency of all keys are counted
// probabilistically using a count min sketch.
type evictPolicy struct {
	sync.Mutex
	cms             *boom.CountMinSketch
	ll              *linkedList
	e               *Engine
	RelevanceWindow time.Duration
}

func (ep *evictPolicy) addToWindow(e *skiplist.Element) {
	//
}

func (ep *evictPolicy) removeFromWindow(e *skiplist.Element) {
	//
}

func (ep *evictPolicy) startLoop(step time.Duration) {

	for range time.Tick(step) {
		ep.Lock()

		for f := ep.ll.front; f != nil && f.key.After(time.Now()); f = ep.ll.front {

			_ = ep.cms.TestAndRemove([]byte(f.val), 1)
			ep.ll.delFront()
		}
		ep.Unlock()
	}
}

type linkedList struct {
	front *llElement
	back  *llElement
}

type llElement struct {
	key  time.Time
	val  string
	next *llElement
}

// approximately sorted
func (ll *linkedList) addToBack(val string) {
	e := &llElement{time.Now(), val, nil}
	if ll.back != nil {
		ll.back.next = e
		ll.back = e
	} else {
		ll.front = e
		ll.back = e
	}
}

func (ll *linkedList) delFront() {
	if ll.front == nil && ll.back == nil {
		return
	}
	if ll.front == ll.back { // 1 element
		ll.back = nil
	}
	ll.front = ll.front.next
}
