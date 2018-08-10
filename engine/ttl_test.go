package engine

import (
	"sync"
	"testing"
	. "time"

	"github.com/stretchr/testify/assert"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
	"github.com/wv0m56/prefixed/skiplist"
)

func TestTTL(t *testing.T) {

	opts := EngineOptionsDefault
	opts.O = &fake.NoDelayOrigin{}
	e, err := NewEngine(&opts)
	assert.Nil(t, err)

	e.CacheFill("a")
	e.CacheFill("b")
	e.CacheFill("c")
	e.CacheFill("d")
	e.CacheFill("e")
	e.CacheFill("f")

	e.ts = &ttlStore{
		sync.Mutex{},
		*(skiplist.NewDuplist(20)),
		map[string]*skiplist.DupElement{},
		e,
	} // leak but dont care

	go e.ts.startLoop(1 * Millisecond)

	e.setTTL(Millisecond, &TTL{"c", 19}, &TTL{"f", 25}, &TTL{"z", 11})

	vals := ""
	e.rwm.RLock()
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	e.rwm.RUnlock()
	assert.Equal(t, "abcdef", vals)

	Sleep(20 * Millisecond)
	vals = ""
	e.rwm.RLock()
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	e.rwm.RUnlock()
	assert.Equal(t, "abde", vals)
}
