package skiplist

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDupList(t *testing.T) {

	d := NewDuplist(24)
	assert.Nil(t, d.First())

	d.Insert(111, "foo")
	d.Insert(111, "bar")
	d.Insert(111, "baz")
	d.Insert(222, "qux")
	d.Insert(11, "first")
	d.Insert(-5, "minus5")

	first := d.First()
	assert.Equal(t, int64(11), first.Key())
	assert.Equal(t, "first", first.Val())

	var keys, vals string
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "firstbazbarfooqux", vals)
	assert.Equal(t, "11111111111222", string(keys))

	d.DelFirst()
	keys, vals = "", ""
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "bazbarfooqux", vals)
	assert.Equal(t, "111111111222", string(keys))

	d.DelFirst()
	keys, vals = "", ""
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "barfooqux", vals)
	assert.Equal(t, "111111222", string(keys))

	d.DelFirst()
	keys, vals = "", ""
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "fooqux", vals)
	assert.Equal(t, "111222", string(keys))

	d.DelFirst()
	keys, vals = "", ""
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "qux", vals)
	assert.Equal(t, "222", string(keys))

	d.DelFirst()
	keys, vals = "", ""
	for it := d.First(); it != nil; it = it.Next() {
		keys += strconv.FormatInt(it.Key(), 10)
		vals += it.Val()
	}
	assert.Equal(t, "", vals)
	assert.Equal(t, "", string(keys))
}
