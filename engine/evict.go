package engine

import (
	"sync"
	"time"

	"github.com/tylertreat/BoomFilters"
)

const graveyardSize = 1000

// evictPolicy is the data structure determining which row of the cache should
// be removed in case of space contention. evictPolicy maintains a
// relevanceWindow, inside of which access frequency of all keys are counted
// probabilistically using a count min sketch.
type evictPolicy struct {
	sync.Mutex
	cms             *boom.CountMinSketch
	ll              *linkedList
	listElPtr       map[string]*llElement
	relevanceWindow time.Duration
	graveyard       map[string]struct{}
}

func (ep *evictPolicy) isRelevant(key string) bool {
	_, ok := ep.listElPtr[key]
	return ok
}

// lock ok because called from goroutine
func (ep *evictPolicy) addToWindow(key string) {

	ep.Lock()
	defer ep.Unlock()

	if ptr, ok := ep.listElPtr[key]; ok {
		ep.ll.delByPtr(ptr)
		// no need to delete map element, overwritten later
	}

	ep.cms.Add([]byte(key))
	ptr := ep.ll.addToBack(key)
	ep.listElPtr[key] = ptr
	delete(ep.graveyard, key)
}

// lock ok because called from goroutine
func (ep *evictPolicy) dataDeletion(key string) {
	ep.Lock()
	defer ep.Unlock()

	ep.del(key)
	delete(ep.graveyard, key)
}

func (ep *evictPolicy) outRelevanceWindow(key string) {

	ep.del(key)

	// maintain graveyard size below max
	if len(ep.graveyard) == graveyardSize {

		// delete a random graveyard member
		for k := range ep.graveyard {
			delete(ep.graveyard, k)
			break
		}
	}

	ep.graveyard[key] = struct{}{}
}

func (ep *evictPolicy) del(key string) {
	_ = ep.cms.TestAndRemoveAll([]byte(key))
	if ptr, ok := ep.listElPtr[key]; ok {
		ep.ll.delByPtr(ptr)
	}
	delete(ep.listElPtr, key)
}

func (ep *evictPolicy) startLoop(step time.Duration) {

	for range time.Tick(step) {
		ep.Lock()
		for it := ep.ll.front; it != nil && it.lastReadTime.Add(ep.relevanceWindow).Before(time.Now()); it = it.next {
			ep.outRelevanceWindow(it.val)
		}
		ep.Unlock()
	}
}

type linkedList struct {
	front *llElement
	back  *llElement
}

type llElement struct {
	lastReadTime time.Time // last accessed time
	val          string
	prev         *llElement
	next         *llElement
}

// approximately sorted
func (ll *linkedList) addToBack(val string) *llElement {

	e := &llElement{time.Now(), val, nil, nil}
	e.prev = ll.back

	if ll.back != nil {

		ll.back.next = e
		ll.back = e

	} else {

		ll.front = e
		ll.back = e
	}

	return e
}

func (ll *linkedList) delFront() {

	if ll.front == nil && ll.back == nil {
		return
	}

	if ll.front == ll.back { // 1 element
		ll.back = nil
	}

	ll.front = ll.front.next
	if ll.front != nil {
		ll.front.prev = nil
	}
}

func (ll *linkedList) delByPtr(e *llElement) {

	if e.prev == nil {

		ll.delFront()
		return
	}

	e.prev.next = e.next
	if e.next != nil {
		e.next.prev = e.prev
	}
}
