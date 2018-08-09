package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	ll.addToBack("one")
	ll.addToBack("two")
	ll.addToBack("3")
	ll.addToBack("4")
	assert.Equal(t, "one", ll.front.val)
	assert.Equal(t, "4", ll.back.val)

	var vals string
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "onetwo34", vals)
}
