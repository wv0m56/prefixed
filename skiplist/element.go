package skiplist

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

// Mutating the returned slice will mutate the slice inside the skiplist.
func (e *Element) Val() []byte {
	return e.val
}

// ValCopy is just like Val except mutating the returned slice will not
// mutate the slice inside the skiplist.
func (e *Element) ValCopy() []byte {
	b := make([]byte, len(e.val))
	copy(b, e.val)
	return b
}

func newElem(key string, val []byte) *Element {
	e := &Element{}
	l := 1 + addHeight()
	e.key = key
	e.val = val
	e.nexts = make([]*Element, l)
	return e
}

func addHeight() int {
	var n int
	for n = 0; n < maxHeight; n++ {
		if !flipCoin() {
			break
		}
	}
	return n
}

func (e *Element) insert(left []*Element, right *Element) {
	if e.key == right.key {
		e.replace(left, right)
	} else {
		e.insertBetween(left, right)
	}
}
