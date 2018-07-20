package skiplist

import (
	"bytes"
)

const maxHeight = 32

// An Element is a KV node in the skiplist. Internally, it holds the height information
// determined by a series of coin flips and pointers to the next element at each level
// up to its height.
type Element struct {
	key   string
	val   []byte
	nexts []*Element
}

func (e *Element) Key() string {
	return e.key
}

// ValReader returns a Reader to read from the byte slice contained in the element.
func (e *Element) ValReader() *bytes.Reader {
	return bytes.NewReader(e.val)
}

// ValCopy returns a copy of the byte slice contained in the element.
// Mutating the reutrned slice will not mutate the slice inside the skiplist.
func (e *Element) ValCopy() []byte {
	if e.val != nil {
		b := make([]byte, len(e.val))
		copy(b, e.val)
		return b
	}
	return nil
}

// Next returns the next element using the 0th level pointer.
func (e *Element) Next() *Element {
	return e.nexts[0]
}

func newElem(key string, val []byte) *Element {
	e := &Element{}
	lvl := 1 + addHeight()
	e.key = key
	e.val = val
	e.nexts = make([]*Element, lvl)
	return e
}

func addHeight() int {
	var n int
	for n = 0; n < maxHeight-1; n++ {
		if !flipCoin() {
			break
		}
	}
	return n
}
