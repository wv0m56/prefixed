package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLinkedList(t *testing.T) {

	ll := &linkedList{}
	ll.delFront()

	ll.addToBack(time.Now(), "one")
	assert.NotNil(t, ll.front)
	assert.NotNil(t, ll.back)
	assert.Equal(t, ll.front, ll.back)

	ll.delFront()
	assert.Nil(t, ll.front)
	assert.Nil(t, ll.back)

	ll.addToBack(time.Now(), "one")
	ll.addToBack(time.Now(), "two")
	ll.addToBack(time.Now(), "3")
	ll.addToBack(time.Now(), "4")
	assert.Equal(t, "one", ll.front.val)
	assert.Equal(t, "4", ll.back.val)

	var vals string
	for it := ll.front; it != nil; it = it.next {
		vals += it.val
	}
	assert.Equal(t, "onetwo34", vals)
}
