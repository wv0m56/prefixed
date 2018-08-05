package engine

import (
	"sync"
	"time"

	"github.com/tylertreat/BoomFilters"
)

// EvictPolicy is the data structure determining which row of the cache should
// be removed in case of space contention. EvictPolicy maintains a
// RelevanceWindow, inside of which access frequency of all keys are counted
// probabilistically using a count min sketch.
type EvictPolicy struct {
	mu              *sync.Mutex
	cms             *boom.CountMinSketch
	ll              *linkedList
	e               *Engine
	RelevanceWindow time.Duration
}

// approximately sorted
type linkedList struct {
	front *llElement
	back  *llElement
}

type llElement struct {
	key  time.Time
	val  string
	next *llElement
}

func (ll *linkedList) addToBack(key time.Time, val string) {
	e := &llElement{key, val, nil}
	if ll.back != nil {
		ll.back.next = e
		ll.back = e
		return
	}
	ll.front = e
	ll.back = e
}

func (ll *linkedList) delFront() {
	if ll.front == nil && ll.back == nil {
		return
	}
	if ll.front == ll.back {
		ll.back = nil
	}
	if ll.front != nil {
		ll.front = ll.front.next
	}
}
