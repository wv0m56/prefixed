package engine

import (
	"io/ioutil"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
)

func TestSimpleIO(t *testing.T) {

	e, err := NewEngine(1025, &fake.Impl{})
	assert.Nil(t, err)
	assert.Nil(t, err)
	valR, err := e.Get("water")
	assert.Nil(t, err)
	b, err := ioutil.ReadAll(valR)
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))
	assert.Nil(t, err)
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))
	b[1]++
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))

	// trigger error, see fake.fakeReadCloser
	valR, err = e.Get("error")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
	valR, err = e.Get("error") // make sure the row was not committed above
	assert.NotNil(t, err)
	assert.Nil(t, valR)
}

func TestPrefix(t *testing.T) {

	e, err := NewEngine(1025, &fake.Impl{})
	assert.Nil(t, err)
	r1, err := e.CacheFill("water")
	assert.Nil(t, err)
	r2, err := e.CacheFill("waterfall")
	assert.Nil(t, err)
	b, err := ioutil.ReadAll(r1)
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))
	b, err = ioutil.ReadAll(r2)
	assert.Nil(t, err)
	assert.Equal(t, "waterfall", string(b))
	rows := e.GetByPrefix("water")
	assert.Equal(t, 2, len(rows))
	bs := e.GetCopiesByPrefix("water")
	assert.Equal(t, 2, len(bs))
}

func TestHotKey(t *testing.T) {

	e, err := NewEngine(1025, &fake.Impl{})
	assert.Nil(t, err)
	wg := sync.WaitGroup{}
	N := 10000
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			r, err := e.Get("hot key")
			assert.Nil(t, err)
			b, err2 := ioutil.ReadAll(r)
			assert.Nil(t, err2)
			assert.Equal(t, "hot key", string(b))
			wg.Done()
		}()
	}
	wg.Wait()

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			r, err := e.Get("error")
			assert.NotNil(t, err)
			assert.Nil(t, r)
			wg.Done()
		}()
	}
	wg.Wait()
}

// Test how much time N concurrent calls to CacheFill spend resolving lock
// contention, given 0 network delay.
func BenchmarkHotKey(b *testing.B) {

	N := 10000
	e, _ := NewEngine(1025, &fake.BenchImpl{})

	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(N)
		for j := 0; j < N; j++ {
			go func() {
				e.Get("hot key 2")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

// Similar to BenchmarkHotKey, except this time origin returns an error.
func BenchmarkErrorKey(b *testing.B) {

	N := 10000
	e, _ := NewEngine(1025, &fake.BenchImpl{})

	for i := 0; i < b.N; i++ {
		wg := sync.WaitGroup{}
		wg.Add(N)
		for j := 0; j < N; j++ {
			go func() {
				e.Get("bench error")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
