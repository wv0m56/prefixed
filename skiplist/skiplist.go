package skiplist

import (
	"strings"
)

// Skiplist implements a skip list. It is not thread safe and should be protected
// by RWMutex when used concurrently.
type Skiplist struct {
	front            []*Element
	len, payloadSize int64
	maxHeight        int
}

// NewSkiplist returns Skiplist with a height of maxHeight. maxHeight must be
// between 2 and 64 (inclusive), otherwise it panics. A reasonable number
// is log2(N/2), where N is the expected number of elements in the skiplist.
func NewSkiplist(maxHeight int) *Skiplist {
	s := &Skiplist{}
	s.Init(maxHeight)
	return s
}

// Init must be called on a skiplist created without calling NewSkiplist().
// It empties the skiplist.
func (s *Skiplist) Init(maxHeight int) {
	s.front = make([]*Element, maxHeight)
	s.len = 0
	s.payloadSize = 0
	if !(maxHeight < 2 || maxHeight >= 64) {
		s.maxHeight = maxHeight
	} else {
		panic(`skiplist maximum height must be between 2 and 64`)
	}
}

// Len returns the number of elements inside the skiplist.
func (s *Skiplist) Len() int64 {
	return s.len
}

// PayloadSize returns the total sum of len(Val) from all elements.
func (s *Skiplist) PayloadSize() int64 {
	return s.payloadSize
}

// First returns the first element in the skiplist.
func (s *Skiplist) First() *Element {
	return s.front[0]
}

// Upsert searches for insert position and insert into that position.
// It overwrites existing key if it already exists.
// Upsert does nothing if key == "".
func (s *Skiplist) Upsert(key string, val []byte) {

	if key == "" {
		return
	}
	e := newElem(key, val, s.maxHeight)

	if s.len == 0 {

		s.insert(s.front, e, nil)

	} else {

		s.searchAndUpsert(e)
	}

	s.len++
}

// Get finds an Element by key according to the comma-ok idiom.
// Returns a non-nil *Element and true if key is found. Else returns nil, false.
func (s *Skiplist) Get(key string) (*Element, bool) {

	_, it := s.search(key)
	if it != nil && it.key == key {
		return it, true
	}
	return nil, false
}

// GetByPrefix returns a slice of Elements whose keys are prefixed by p.
// It returns nil if no such thing is found.
func (s *Skiplist) GetByPrefix(p string) (es []*Element) {

	_, it := s.search(p)

	for ; it != nil && strings.HasPrefix(it.key, p); it = it.Next() {
		es = append(es, it)
	}
	return
}

// Del deletes the element refered by key. It only removes all references to
// underlying *Element. As long as another part of the program is holding the
// deleted *Element, it will not be garbage collected. Return the deleted
// element if key is found or nil if it doesn't exist.
func (s *Skiplist) Del(key string) *Element {

	left, it := s.search(key)
	if it != nil && key == it.key {
		s.del(left, it)
		return it
	}
	return nil
}

// DelByPrefix deletes elements with keys which have prefix p.
func (s *Skiplist) DelByPrefix(p string) {

	left, it := s.search(p)
	for ; strings.HasPrefix(it.key, p); it = it.Next() {
		s.del(left, it)
		if it.Next() == nil {
			return
		}
	}
	return
}

func (s *Skiplist) del(left []*Element, e *Element) {

	for i := 0; i < len(e.nexts); i++ {
		s.reassignLeftAtIndex(i, left, e.nexts[i])
	}
	s.payloadSize -= int64(len(e.val))
	s.len--
}

func (s *Skiplist) searchAndUpsert(e *Element) {
	left, iter := s.search(e.key)
	s.insert(left, e, iter)
}

func (s *Skiplist) search(key string) (left []*Element, iter *Element) {
	left = make([]*Element, s.maxHeight)

	for h := s.maxHeight - 1; h >= 0; h-- { // level descending loop

		if h == s.maxHeight-1 || left[h+1] == nil {

			iter = s.front[h]

		} else {

			left[h] = left[h+1]
			iter = left[h].nexts[h]
		}

		for { // forward loop

			if iter == nil || key <= iter.key {
				break // break the forward loop and go down a level

			} else {

				left[h] = iter
				iter = iter.nexts[h]
			}
		}
	}
	return
}

// right is a redundant but cheap-to-have piece of information.
func (s *Skiplist) insert(left []*Element, e, right *Element) {

	if right != nil && e.key == right.key {

		s.replace(left, e, right)

	} else {

		s.insertBetween(left, e, right)
	}
}

func (s *Skiplist) insertBetween(left []*Element, e, right *Element) {

	for i := 0; i < len(e.nexts); i++ {

		if right != nil && i < len(right.nexts) {

			e.nexts[i] = right

		} else {

			s.takeNextsFromLeftAtIndex(i, left, e)
		}

		s.reassignLeftAtIndex(i, left, e)
	}
	s.payloadSize += int64(len(e.val))
}

func (s *Skiplist) replace(left []*Element, e, right *Element) {

	s.payloadSize -= int64(len(right.val))

	for i := 0; i < max(len(e.nexts), len(right.nexts)); i++ {

		if i < len(e.nexts) {

			if i < len(right.nexts) {

				e.nexts[i] = right.nexts[i]

			} else {

				s.takeNextsFromLeftAtIndex(i, left, e)
			}
			s.reassignLeftAtIndex(i, left, e)

		} else {

			s.reassignLeftAtIndex(i, left, right.nexts[i])
		}
	}
	s.payloadSize += int64(len(e.val))
}

func (s *Skiplist) takeNextsFromLeftAtIndex(i int, left []*Element, e *Element) {
	if left[i] != nil {
		e.nexts[i] = left[i].nexts[i]
	} else {
		e.nexts[i] = s.front[i]
	}
}

func (s *Skiplist) reassignLeftAtIndex(i int, left []*Element, e *Element) {
	if left[i] == nil {
		s.front[i] = e
	} else {
		left[i].nexts[i] = e
	}
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
