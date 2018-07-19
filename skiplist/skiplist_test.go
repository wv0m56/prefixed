package skiplist

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertsInternal(t *testing.T) {

	SetRandSource(rand.NewSource(53535353))
	skip := NewSkiplist()

	// "tokyo"
	skip.Upsert("tokyo", nil)
	first := skip.First()
	assert.NotNil(t, first)
	assert.Nil(t, first.Next())
	heightOfTokyo := 2
	assert.Equal(t, heightOfTokyo, len(first.nexts))

	for i := 0; i < maxHeight; i++ {

		if i < heightOfTokyo {

			assert.Equal(t, skip.front[i], first)
			assert.Nil(t, first.nexts[i])

		} else {

			assert.Nil(t, skip.front[i])
		}
	}

	// "zulu"
	skip.Upsert("zulu", nil)
	first = skip.First()
	assert.Equal(t, "tokyo", first.Key())
	for i := 0; i < maxHeight; i++ {

		if i < heightOfTokyo {

			assert.Equal(t, skip.front[i], first)
			assert.NotNil(t, first.nexts[i])

		} else {

			assert.Nil(t, skip.front[i])
		}
	}
	assert.Equal(t, "zulu", first.Next().Key())
	fmt.Println(len(first.nexts))
}
