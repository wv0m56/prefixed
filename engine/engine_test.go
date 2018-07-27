package engine

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleIO(t *testing.T) {

	e := NewEngine(1000)
	valR, err := e.Get("water")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
	rw := e.RowWriter("water")
	n, err := rw.Write([]byte("wet"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	valR, err = e.Get("water")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
	rw.Commit()
	valR, err = e.Get("water")
	assert.Nil(t, err)
	assert.NotNil(t, valR)
	b, err := ioutil.ReadAll(valR)
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
	b[1]++
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
}

func TestPrefix(t *testing.T) {

	e := NewEngine(1000)
	rw := e.RowWriter("water")
	n, err := rw.Write([]byte("wet"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	rw.Commit()
	rw = e.RowWriter("waterfall")
	n, err = rw.Write([]byte("very wet"))
	assert.Nil(t, err)
	assert.Equal(t, 8, n)
	rs := e.GetByPrefix("water")
	assert.Equal(t, 1, len(rs))
	rw.Commit()
	rs = e.GetByPrefix("water")
	assert.Equal(t, 2, len(rs))
	b := e.GetCopiesByPrefix("water")
	assert.Equal(t, 2, len(b))
}
