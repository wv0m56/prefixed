package skiplist

import (
	"sync"
)

// Skiplist implements a skip list guarded by a RWMutex.
type Skiplist struct {
	front            []*Element
	len, payloadSize int64
	mu               sync.RWMutex
}

// NewSkiplist returns Skiplist with a height of 32.
func NewSkiplist() *Skiplist {
	s := &Skiplist{}
	s.Init()
	return s
}

// Init must be called on a skiplist created without calling NewSkiplist().
func (s *Skiplist) Init() {
	s.mu.Lock()
	s.front = make([]*Element, maxHeight)
	s.len = 0
	s.mu.Unlock()
}

// Len returns the number of elements inside the skiplist.
func (s *Skiplist) Len() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.len
}

// PayloadSize returns the total sum of len(Val) from all elements.
func (s *Skiplist) PayloadSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.payloadSize
}

// First returns the first element in the skiplist.
func (s *Skiplist) First() *Element {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.front[0]
}

// Upsert searches for insert position and insert into that position.
// It overwrites existing key if it already exists.
// Upsert does nothing if key == "".
func (s *Skiplist) Upsert(key string, val []byte) {

	if key == "" {
		return
	}
	e := newElem(key, val)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.len == 0 {

		for i := 0; i < len(e.nexts); i++ {

			s.front[i] = e // e.nexts implied from zero vals
		}

	} else {

		s.searchAndUpsert(e)
	}

	s.len++
}

// Get finds an Element by key. Returns nil if not found.
func (s *Skiplist) Get(key string) *Element {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, it := s.search(key)
	if it.key == key {
		return it
	}
	return nil
}

func (s *Skiplist) searchAndUpsert(e *Element) {
	left, iter := s.search(e.key)
	s.insert(left, e, iter)
}

func (s *Skiplist) search(key string) (left []*Element, iter *Element) {
	left = make([]*Element, maxHeight)

	for h := maxHeight - 1; h >= 0; h-- { // level descending loop

		if h == maxHeight-1 || left[h+1] == nil {

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

		if i < len(e.nexts) { // up to equal height, damn zero based index

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
