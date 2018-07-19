package skiplist

import (
	"math/rand"
	"time"
)

// not cryptographically secure, secure one is slow
var randSource = rand.NewSource((time.Now().UnixNano()/39-39)*39 + 222)

// true=heads, false=tails
func flipCoin() bool {
	if randSource.Int63()%2 == 0 {
		return true
	}
	return false
}

// SetRandSource sets the random number generator used to perform the coin flips
// to determine an element's "height". It is not thread safe and is meant to be
// called only once before using the package.
func SetRandSource(src rand.Source) {
	randSource = src
}
