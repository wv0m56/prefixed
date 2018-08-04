package skiplist

type DupElement struct {
	key   int64
	val   string
	nexts []*DupElement
}

func (de *DupElement) Key() int64 {
	return de.key
}

func (de *DupElement) Val() string {
	return de.val
}

func (de *DupElement) Next() *DupElement {
	return de.nexts[0]
}

func newDupElem(key int64, val string, maxHeight int) *DupElement {
	lvl := 1 + addHeight(maxHeight)
	return &DupElement{key, val, make([]*DupElement, lvl)}
}
