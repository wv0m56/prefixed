package skiplist

import (
	"sync"
)

// Skiplist implements a skip list guarded by a RWMutex.
type Skiplist struct {
	front []*Element
	size  int
	mu    sync.RWMutex
}

// NewSkiplist returns Skiplist with a height of 32.
func NewSkiplist() *Skiplist {
	s := &Skiplist{}
	s.Init()
	return s
}

// Init must be called on a skiplist created without calling NewSkiplist().
func (s *Skiplist) Init() {
	s.front = make([]*Element, maxHeight)
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
	if s.size == 0 {
		for i := 0; i < len(e.nexts); i++ {
			s.front[i] = e
		}
	} else {
		s.searchAndUpsert(e)
	}
	s.size++
}

func (s *Skiplist) searchAndUpsert(e *Element) {
	e.insert(s.search(e))
}

// not done
func (s *Skiplist) search(e *Element) (left []*Element, iter *Element) {
	left = make([]*Element, maxHeight)

	for h := maxHeight - 1; h >= 0; h-- { // level descending loop

		if h == maxHeight-1 || left[h+1] == nil {
			iter = s.front[h]
		} else {
			left[h] = left[h+1]
			iter = left[h+1].nexts[h]
		}

		for { // forward loop
			if iter == nil || e.key <= iter.key {
				break // break the forward loop and go down a level
			} else {
				left[h] = iter
				iter = iter.nexts[h]
			}
		}
	}
	return
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
