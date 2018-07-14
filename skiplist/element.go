package skiplist

const maxHeight = 32

// An Element is a node in the skiplist. Internally, it holds the height information
// determined by a series of coin flips and pointers to the next element at each level
// up to its height.
type Element struct {
	Key string
	// TODO: Val []byte
	nexts  []*Element
	height int
}

// TODO: new(k, v)
func newElem(key string) *Element {
	return &Element{key, nil, 1 + addHeight()}
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
