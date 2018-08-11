package engine

import (
	"testing"
	. "time"

	"github.com/stretchr/testify/assert"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
)

func TestTTL(t *testing.T) {

	opts := EngineOptionsDefault
	opts.O = &fake.NoDelayOrigin{}
	opts.TtlTickStep = 1 * Millisecond
	e, err := NewEngine(&opts)
	assert.Nil(t, err)

	e.CacheFill("a")
	e.CacheFill("b")
	e.CacheFill("c")
	e.CacheFill("d")
	e.CacheFill("e")
	e.CacheFill("f")

	e.setTTL(Millisecond, &TTL{"c", 19}, &TTL{"f", 25}, &TTL{"z", 11})

	vals := ""
	e.rwm.RLock()
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	e.rwm.RUnlock()
	assert.Equal(t, "abcdef", vals)

	// confirm element deletion after expiry
	Sleep(20 * Millisecond)
	vals = ""
	e.rwm.RLock()
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	e.rwm.RUnlock()
	assert.Equal(t, "abdef", vals)

	Sleep(6 * Millisecond)
	vals = ""
	e.rwm.RLock()
	for it := e.s.First(); it != nil; it = it.Next() {
		vals += it.Key()
	}
	e.rwm.RUnlock()
	assert.Equal(t, "abde", vals)

	// GetTTL
	e.SetTTL(&TTL{"d", 15}, &TTL{"a", 24})
	secs := e.GetTTL("a", "d", "ff")
	assert.Equal(t, 3, len(secs))

	assert.True(t, roughly(24.0, secs[0]))
	assert.True(t, roughly(15.0, secs[1]))
	assert.Equal(t, -1.0, secs[2])

	Sleep(50 * Millisecond)

	secs = e.GetTTL("a", "d", "ff")
	assert.True(t, roughly(23.95, secs[0]))
	assert.True(t, roughly(14.95, secs[1]))

	// Overwrite existing TTL
	e.SetTTL(&TTL{"a", 700})
	secs = e.GetTTL("a", "d", "ff")
	assert.True(t, roughly(700, secs[0]))

	// RemoveTTL
	e.RemoveTTL("a", "d", "ff")
	secs = e.GetTTL("a", "d", "ff")
	Sleep(1 * Millisecond)
	assert.Equal(t, -1.0, secs[0])
	assert.Equal(t, -1.0, secs[1])
	assert.Equal(t, -1.0, secs[2])
}

func roughly(a, b float64) bool {
	if (a/b > 0.999 && a/b <= 1.0) || (a/b >= 1.0 && a/b < 1.001) {
		return true
	}
	return false
}
