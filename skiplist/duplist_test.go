package skiplist

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDuplist(t *testing.T) {

	d := NewDuplist(24)
	assert.Nil(t, d.First())

	now := time.Now()
	d.Insert(now.Add(50*time.Millisecond), "foo")
	d.Insert(now.Add(50*time.Millisecond), "bar")
	d.Insert(now.Add(50*time.Millisecond), "baz")
	d.Insert(now.Add(70*time.Millisecond), "qux")
	d.Insert(now.Add(30*time.Millisecond), "first")

	first := d.First()
	assert.Equal(t, now.Add(30*time.Millisecond), first.Key())
	assert.Equal(t, "first", first.Val())

	var vals string
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "firstbazbarfooqux", vals)

	d.DelFirst()
	vals = ""
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "bazbarfooqux", vals)

	d.DelFirst()
	vals = ""
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "barfooqux", vals)

	d.DelFirst()
	vals = ""
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "fooqux", vals)

	d.DelFirst()
	vals = ""
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "qux", vals)

	d.DelFirst()
	vals = ""
	for it := d.First(); it != nil; it = it.Next() {
		vals += it.Val()
	}
	assert.Equal(t, "", vals)
}

func BenchmarkDuplistInsert(b *testing.B) {

	N := 1000 * 10
	dup := NewDuplist(22)
	for i := 0; i < N; i++ {
		dup.Insert(time.Now(), time.Now().String())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dup.Insert(time.Now(), time.Now().String())
	}
}
