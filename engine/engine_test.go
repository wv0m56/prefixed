package engine

import (
	"bytes"
	"io/ioutil"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
)

func TestSimpleIO(t *testing.T) {

	e, err := NewEngine(&OptionsDefault)
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
	b[1]++ // mutate returned []byte
	b, err = e.GetCopy("water")
	assert.Nil(t, err)
	assert.Equal(t, "water", string(b))
	_, err = e.GetCopy("error")
	assert.NotNil(t, err)

	// trigger error, see fake.fakeReadCloser
	valR, err = e.Get("error")
	assert.NotNil(t, err)
	assert.Nil(t, valR)
	valR, err = e.Get("error") // make sure the row was not committed above
	assert.NotNil(t, err)
	assert.Nil(t, valR)
}

func TestCachefillTimeout(t *testing.T) {

	opts := OptionsDefault // origin has 100 ms delay
	opts.CacheFillTimeout = 110 * time.Millisecond
	e, err := NewEngine(&opts)
	assert.Nil(t, err)

	_, err = e.Get("TestCachefillTimeout")
	assert.Nil(t, err)

	opts.CacheFillTimeout = 90 * time.Millisecond
	e2, err := NewEngine(&opts)
	assert.Nil(t, err)
	_, err = e2.Get("TestCachefillTimeout2")
	assert.NotNil(t, err)
	assert.Equal(t, "context deadline exceeded", err.Error())
}

func TestPrefix(t *testing.T) {

	e, err := NewEngine(&OptionsDefault)
	assert.Nil(t, err)

	r1, err := e.Get("water")
	assert.Nil(t, err)

	r2, err := e.Get("waterfall")
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

	e, err := NewEngine(&OptionsDefault)
	assert.Nil(t, err)
	wg := sync.WaitGroup{}
	N := 8000
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

// API + internals
func TestEvictUponDelete(t *testing.T) {

	opts := OptionsDefault
	opts.O = &fake.NoDelayOrigin{}

	// too small value will fail test -race due to slower execution
	opts.EvictPolicyRelevanceWindow = 100 * time.Millisecond

	opts.EvictPolicyTickStep = 1 * time.Millisecond

	eng, err := NewEngine(&opts)
	assert.Nil(t, err)

	b, err := eng.GetCopy("abc")
	assert.Nil(t, err)
	assert.Equal(t, "abc", string(b))

	eng.Get("abc")
	eng.GetCopy("abc")
	eng.Get("abc")

	time.Sleep(10 * time.Millisecond) // wait a bit since stats is updated via goroutine
	eng.ep.Lock()
	ptr, ok := eng.ep.listElPtr["abc"]
	assert.True(t, ok)
	assert.Equal(t, "abc", ptr.val)
	assert.Equal(t, uint64(4), eng.ep.cms.Count([]byte("abc")))
	eng.ep.Unlock()

	time.Sleep(opts.EvictPolicyRelevanceWindow + 10*time.Millisecond)

	eng.ep.Lock()
	ptr, ok = eng.ep.listElPtr["abc"]
	assert.False(t, ok)
	assert.Nil(t, ptr)
	assert.Equal(t, uint64(0), eng.ep.cms.Count([]byte("abc")))
	eng.ep.Unlock()
}

func TestSimpleEvictUponFullCache(t *testing.T) {

	opts := OptionsDefault
	opts.O = &fake.ZeroesPayloadOrigin{}
	opts.MaxPayloadTotalSize = 10 * 1000 * 1000
	opts.EvictPolicyTickStep = 10 * time.Millisecond

	// large value for -race
	opts.EvictPolicyRelevanceWindow = 1 * time.Second

	e, err := NewEngine(&opts)
	assert.Nil(t, err)

	e.ep.Lock()
	assert.Equal(t, 0, len(e.ep.graveyard))
	e.ep.Unlock()

	for i := 0; i < 1000; i++ {
		e.Get(strconv.Itoa(i))
	}

	assert.Equal(t, opts.MaxPayloadTotalSize, e.dataStore.PayloadSize())

	e.ep.Lock()
	assert.Equal(t, 0, len(e.ep.graveyard))
	e.ep.Unlock()

	e.Get("abc")
	r, err := e.Get("abc")
	assert.Nil(t, err)
	buf := bytes.NewBuffer(nil)
	_, err = r.WriteTo(buf)
	assert.Nil(t, err)
	assert.True(t, bytes.Equal(make([]byte, 10000), buf.Bytes()))

	e.ep.Lock()
	assert.Equal(t, 0, len(e.ep.graveyard))
	e.ep.Unlock()

	time.Sleep(opts.EvictPolicyRelevanceWindow)

	e.ep.Lock()
	assert.True(t, len(e.ep.graveyard) > 0)
	e.ep.Unlock()

	for i := 888888; i < 888888+100; i++ {
		_, err = e.Get(strconv.Itoa(i))
		assert.Nil(t, err)
		time.Sleep(1 * time.Millisecond)
	}
}

// API + internals
func TestEngineTTL(t *testing.T) {

	opts := OptionsDefault
	opts.O = &fake.ExpiringOrigin{}

	e, err := NewEngine(&opts)
	assert.Nil(t, err)

	e.Get("asdfg")
	secs := e.GetTTL("zzzz", "asdfg")
	assert.Equal(t, 2, len(secs))
	assert.Equal(t, -1.0, secs[0])
	assert.True(t, roughly(24*3600, secs[1]))

	_, sec, _ := e.GetWithTTL("key")
	assert.True(t, roughly(24*3600, sec))

	opts.O = &fake.NoDelayOrigin{}
	e, err = NewEngine(&opts)
	assert.Nil(t, err)

	e.Get("pppp")
	secs = e.GetTTL("zzzz", "pppp")
	assert.Equal(t, -1.0, secs[1])
	_, sec, _ = e.GetWithTTL("zzz")
	assert.Equal(t, -1.0, sec)

	_, sec, err = e.GetWithTTL("bench error") // knowing that this key triggers error
	assert.NotNil(t, err)
	assert.Equal(t, -1.0, sec)
}

// Test how much time N concurrent calls to CacheFill spend resolving lock
// contention, given 0 network delay.
func BenchmarkHotKey(b *testing.B) {

	N := 10000
	opts := OptionsDefault
	opts.O = &fake.NoDelayOrigin{}
	e, _ := NewEngine(&opts)

	b.ResetTimer()
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
	opts := OptionsDefault
	opts.O = &fake.NoDelayOrigin{}
	e, _ := NewEngine(&opts)

	b.ResetTimer()
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
