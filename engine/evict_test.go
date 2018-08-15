package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tylertreat/BoomFilters"
)

// internals
func TestEvictPolicy(t *testing.T) {

	ep := &evictPolicy{
		sync.Mutex{},
		boom.NewCountMinSketch(0.001, 0.99),
		&linkedList{},
		map[string]*llElement{},
		50 * time.Millisecond,
		map[string]struct{}{},
	}

	go ep.startLoop(time.Millisecond)

	ep.addToWindow("foo")
	ep.addToWindow("bar")
	ep.addToWindow("baz")

	ep.Lock()

	assert.Equal(t, 3, len(ep.listElPtr))
	for _, v := range ep.listElPtr {
		if !(v.val == "foo" || v.val == "bar" || v.val == "baz") {
			t.Error("map wrong")
		}
	}

	assert.Equal(t, uint64(1), ep.cms.Count([]byte("foo")))
	assert.Equal(t, uint64(1), ep.cms.Count([]byte("bar")))
	assert.Equal(t, uint64(1), ep.cms.Count([]byte("baz")))
	assert.Equal(t, uint64(0), ep.cms.Count([]byte("zzz")))

	ep.Unlock()

	time.Sleep(60 * time.Millisecond)

	ep.Lock()

	assert.Equal(t, 0, len(ep.listElPtr))

	assert.Equal(t, uint64(0), ep.cms.Count([]byte("foo")))
	assert.Equal(t, uint64(0), ep.cms.Count([]byte("bar")))
	assert.Equal(t, uint64(0), ep.cms.Count([]byte("baz")))
	assert.Equal(t, uint64(0), ep.cms.Count([]byte("zzz")))

	ep.Unlock()
}

// internals
func TestLinkedList(t *testing.T) {

	ll := &linkedList{}
	ll.delFront()

	ll.addToBack("one")
	assert.NotNil(t, ll.front)
	assert.NotNil(t, ll.back)
	assert.Equal(t, ll.front, ll.back)

	ll.delFront()
	assert.Nil(t, ll.front)
	assert.Nil(t, ll.back)

	ptr1 := ll.addToBack("one")
	ll.addToBack("two")
	ptr2 := ll.addToBack("3")
	ll.addToBack("4")
	assert.Equal(t, "one", ll.front.val)
	assert.Equal(t, "4", ll.back.val)

	var vals string
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "onetwo34", vals)

	ll.delByPtr(ptr2)
	vals = ""
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "onetwo4", vals)

	ll.delByPtr(ptr1)
	vals = ""
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "two4", vals)

	ptr3 := ll.addToBack("back")
	vals = ""
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "two4back", vals)

	ll.delByPtr(ptr3)
	vals = ""
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "two4", vals)
}
