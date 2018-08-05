package skiplist

import "time"

type DupElement struct {
	key   time.Time
	val   string
	nexts []*DupElement
}

func (de *DupElement) Key() time.Time {
	return de.key
}

func (de *DupElement) Val() string {
	return de.val
}

func (de *DupElement) Next() *DupElement {
	return de.nexts[0]
}

func newDupElem(key time.Time, val string, maxHeight int) *DupElement {
	lvl := 1 + addHeight(maxHeight)
	return &DupElement{key, val, make([]*DupElement, lvl)}
}
