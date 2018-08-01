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
	rw := e.RowWriter("water")
	n, err := rw.Write([]byte("wet"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	valR, err := e.Get("water") // now trigger a cache fill since not yet committed
	assert.Nil(t, err)
	b, err := ioutil.ReadAll(valR)
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))
	assert.Nil(t, err)
	rw.Commit()
	valR, err = e.Get("water")
	assert.Nil(t, err)
	assert.NotNil(t, valR)
	b, err = ioutil.ReadAll(valR)
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))
	b[1]++
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "wet", string(b))

	// trigger error, see fake.fakeReadCloser
	valR, err = e.Get("error")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
}

func TestPrefix(t *testing.T) {

	e, err := NewEngine(1025, &fake.Impl{})
	assert.Nil(t, err)
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
				e.Get("hot key")
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
				e.Get("error")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
