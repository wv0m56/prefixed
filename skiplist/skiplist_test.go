package skiplist

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// API calls are tested together with internals
func TestInsertsInternal(t *testing.T) {

	SetRandSource(rand.NewSource(53535353))
	skip := NewSkiplist(32)

	// "tokyo"
	skip.Upsert("tokyo", nil)
	assert.Equal(t, 1, int(skip.Len()))
	first := skip.First()
	assert.NotNil(t, first)
	assert.Nil(t, first.Next())
	heightOfTokyo := 2
	assert.Equal(t, heightOfTokyo, len(first.nexts))

	for i := 0; i < skip.maxHeight; i++ {

		if i < heightOfTokyo {

			assert.Equal(t, skip.front[i], first)
			assert.Nil(t, first.nexts[i])

		} else {

			assert.Nil(t, skip.front[i])
		}
	}

	// "zulu"
	skip.Upsert("zulu", nil)
	assert.Equal(t, 2, int(skip.Len()))
	first = skip.First()
	assert.Equal(t, "tokyo", first.Key())
	for i := 0; i < skip.maxHeight; i++ {

		if i < heightOfTokyo {

			assert.Equal(t, skip.front[i], first)
			assert.NotNil(t, first.nexts[i])

		} else {

			assert.Nil(t, skip.front[i])
		}
	}
	assert.Equal(t, "zulu", first.Next().Key())

	// "angola"
	skip.Upsert("angola", nil)
	first = skip.First()
	assert.Equal(t, "angola", first.Key())
	next := first.Next()
	assert.Equal(t, "tokyo", next.Key())
	next = next.Next()
	assert.Equal(t, "zulu", next.Key())

	// ""
	skip.Upsert("", nil)
	assert.Equal(t, 3, int(skip.Len()))
	assert.Equal(t, 0, int(skip.PayloadSize()))

	// payload size
	skip.Upsert("aaaaaaaaaaaa", []byte("aaaaaaaaaaaa"))
	assert.Equal(t, 12, int(skip.PayloadSize()))
	skip.Upsert("123", []byte("123"))
	assert.Equal(t, 15, int(skip.PayloadSize()))
	skip.Upsert("123", []byte("12345"))
	assert.Equal(t, 17, int(skip.PayloadSize()))

	// init
	skip.Init(32)
	assert.Nil(t, skip.First())

	// insert lots
	strs := []string{
		"nyc", "seoul", "korea", "pyongyang", "texas", "dallas", "singapore", "abc",
		"oregon", "portland", "seattle", "washington", "youtube", "twitter", "ground",
		"facebook", "google", "Microsoft", "microsoft", "president", "earth", "++++",
		"Mars", "mars", "go", "Go", "rice", "cake", "bread", "123345", "twew044329-",
		"{{{{{{{{}@#", "@#!$@!$!", "!!!!!!!", "mercy", "paris", "you", "london",
		"uk", "usa", "morning", "UK", "beijing", "china", "lew", "me", "I", "i",
		",,,,,,,", "dollar", "food", "car", "bike", "word", "number", ">>>>", "fly",
		"9999999", "keyboard", "mouse", "type", "typing", "browser", "ear", "eat",
		"payload", "return", "Ruby", "python", "repeat", "helm", "help", "sometimes",
		"jump", "zero", "panic", "phone", "it", "is", "white", "apple", "name",
		"korea", "korea", "browser", "panic"}

	for _, v := range strs {
		skip.Upsert(v, nil)
	}

	var appended string
	for e := skip.First(); e != nil; e = e.Next() {
		appended += e.Key()
	}
	sort.Strings(strs)
	var strs2 []string
	for i, v := range strs {
		if i != 0 && v == strs[i-1] {
			// ignore dupes
			continue
		}
		strs2 = append(strs2, v)
	}
	assert.Equal(t, appended, strings.Join(strs2, ""))

	// Get
	assert.Nil(t, skip.Get("no such key"))
	it := skip.Get("python")
	assert.NotNil(t, it)
	assert.Equal(t, "python", it.Key())

	// GetByPrefix
	skip.Upsert("cartoon", nil)
	skip.Upsert("carnival", nil)
	skip.Upsert("carnivore", nil)
	skip.Upsert("caravan", nil)
	skip.Upsert("caricature", nil)
	skip.Upsert("cargo", nil)

	es := skip.GetByPrefix("car")

	assert.Equal(t, 7, len(es))
	assert.Equal(t, "car", es[0].Key())
	assert.Equal(t, "caravan", es[1].Key())
	assert.Equal(t, "cargo", es[2].Key())
	assert.Equal(t, "caricature", es[3].Key())
	assert.Equal(t, "carnival", es[4].Key())
	assert.Equal(t, "carnivore", es[5].Key())
	assert.Equal(t, "cartoon", es[6].Key())

	es = skip.GetByPrefix("no such key")
	assert.Nil(t, es)
	es = skip.GetByPrefix("{{{{{{{{}@#")
	assert.Equal(t, 1, len(es))
	es = skip.GetByPrefix("carni")
	assert.Equal(t, 2, len(es))

	// Del and DelByPrefix
	skip.Init(32)
	skip.Upsert("park", []byte("park"))
	skip.Upsert("animal", []byte("animal"))
	skip.Upsert("moon", []byte("moon"))
	skip.Upsert("noon", []byte("noon"))
	skip.Upsert("lock", []byte("lock"))
	skip.Upsert("low", []byte("low"))
	skip.Upsert("lonely", []byte("lonely"))
	skip.Upsert("loop", []byte("loop"))
	assert.Equal(t, int64(8), skip.Len())
	assert.Equal(t, int64(35), skip.PayloadSize())
	skip.Del("animal")
	assert.Equal(t, int64(7), skip.Len())
	assert.Equal(t, int64(35-6), skip.PayloadSize())
	skip.DelByPrefix("lo")
	assert.Equal(t, int64(3), skip.Len())
	assert.Equal(t, int64(35-6-17), skip.PayloadSize())
}

func BenchmarkInserts(b *testing.B) {

	rand.Seed(42394084908978634)

	N := 1000 * 10
	skip := NewSkiplist(int(math.Floor(math.Log2(float64(N / 2)))))
	for i := 0; i < N; i++ {
		skip.Upsert(strconv.Itoa(rand.Int()), nil)
	}

	k := strconv.Itoa(rand.Int())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		skip.Upsert(k, nil)
	}
}

func BenchmarkGet(b *testing.B) {

	rand.Seed(902574429084211)

	N := 1000 * 10
	skip := NewSkiplist(int(math.Floor(math.Log2(float64(N / 2)))))
	for i := 0; i < N; i++ {
		skip.Upsert(strconv.Itoa(rand.Int()), nil)
	}

	skip.Upsert("85811", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		skip.Get("85811")
	}
}

func BenchmarkGetByPrefix(b *testing.B) {

	rand.Seed(14242398490234)

	N := 1000 * 10
	skip := NewSkiplist(int(math.Floor(math.Log2(float64(N / 2)))))
	for i := 0; i < N; i++ {
		skip.Upsert(strconv.Itoa(rand.Int()), nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = skip.GetByPrefix("99")
	}
}

func BenchmarkDel(b *testing.B) {

	rand.Seed(104467541040234)

	N := 1000 * 10
	skip := NewSkiplist(int(math.Floor(math.Log2(float64(N / 2)))))
	for i := 0; i < N; i++ {
		skip.Upsert(strconv.Itoa(rand.Int()), nil)
	}
	skip.Upsert("8787128", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		skip.Del("8787128")
	}
}
