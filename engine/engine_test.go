package engine

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleIO(t *testing.T) {

	e := NewEngine()
	rw := e.RowWriter("water")
	rw.Write([]byte("wet"))
	valR, err := e.Get("water")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
	rw.Commit()
	valR, err = e.Get("water")
	assert.Nil(t, err)
	assert.NotNil(t, valR)
	b, err := ioutil.ReadAll(valR)
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
}
