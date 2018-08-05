package skiplist

// Duplist is a modified skiplist implementation allowing duplicate int64
// keys to exist inside the same list. Elements with duplicate keys are
// adjacent inside Duplist, with a later insert placed left of earlier ones.
// Elements with different keys are sorted in ascending order as usual.
// Duplist is required for implementing TTL and cache eviction functionality.
// Duplist does not allow random get or delete and instead only allows
// get or delete on the first element of the list.
type Duplist struct {
	front     []*DupElement
	maxHeight int
}

func NewDuplist(maxHeight int) *Duplist {
	d := &Duplist{}
	d.Init(maxHeight)
	return d
}

func (d *Duplist) Init(maxHeight int) {
	d.front = make([]*DupElement, maxHeight)
	if !(maxHeight < 2 || maxHeight >= 64) {
		d.maxHeight = maxHeight
	} else {
		panic(`Duplist maximum height must be between 2 and 64`)
	}
}

func (d *Duplist) First() *DupElement {
	return d.front[0]
}

func (d *Duplist) Insert(key int64, val string) {
	if key <= 0 { // no-op
		return
	}

	de := newDupElem(key, val, d.maxHeight)

	if d.front[0] == nil {

		d.insert(d.front, de, nil)

	} else {

		d.searchAndInsert(de)
	}
}

func (d *Duplist) searchAndInsert(de *DupElement) {
	left, iter := d.search(de.key)
	d.insert(left, de, iter)
}

func (d *Duplist) search(key int64) (left []*DupElement, iter *DupElement) {
	left = make([]*DupElement, d.maxHeight)

	for h := d.maxHeight - 1; h >= 0; h-- {

		if h == d.maxHeight-1 || left[h+1] == nil {
			iter = d.front[h]
		} else {
			left[h] = left[h+1]
			iter = left[h].nexts[h]
		}

		for {
			if iter == nil || key <= iter.key {
				break
			} else {
				left[h] = iter
				iter = iter.nexts[h]
			}
		}
	}

	return
}

func (d *Duplist) insert(left []*DupElement, de, right *DupElement) {
	for i := 0; i < len(de.nexts); i++ {
		if right != nil && i < len(right.nexts) {

			de.nexts[i] = right

		} else {

			d.takeNextsFromLeftAtIndex(i, left, de)
		}

		d.reassignLeftAtIndex(i, left, de)
	}
}

func (d *Duplist) takeNextsFromLeftAtIndex(i int, left []*DupElement, de *DupElement) {
	if left[i] != nil {
		de.nexts[i] = left[i].nexts[i]
	} else {
		de.nexts[i] = d.front[i]
	}
}

func (d *Duplist) reassignLeftAtIndex(i int, left []*DupElement, de *DupElement) {
	if left[i] == nil {
		d.front[i] = de
	} else {
		left[i].nexts[i] = de
	}
}

func (d *Duplist) DelFirst() {

	for i := 0; i < d.maxHeight; i++ {
		if d.front[i] == nil || d.front[i] != d.front[0] {
			continue
		}
		d.front[i] = d.front[i].nexts[i]
	}
}
