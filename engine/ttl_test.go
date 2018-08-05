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

	e, err := NewEngine(-1, &fake.BenchImpl{}) // BenchImpl for 0 network delay
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
		e,
	} // leak but dont care

	go e.ts.startLoop(1 * Millisecond)

	e.setTTL(Millisecond, &TTL{"c", 19}, &TTL{"f", 25}, &TTL{"z", 11})

	vals := ""
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	assert.Equal(t, "abcdef", vals)

	Sleep(20 * Millisecond)
	vals = ""
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	assert.Equal(t, "abde", vals)
}
