package stress

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wv0m56/prefixed/engine"
	"github.com/wv0m56/prefixed/plugin/origin/fake"
)

// basically tests with opt values near the allowed limit
func TestStressEngine(t *testing.T) {

	rand.Seed(time.Now().UnixNano())

	opts := engine.OptionsDefault
	opts.O = &fake.RandomOrigin{}
	opts.MaxPayloadTotalSize = 10 * 1000 * 1000
	opts.EvictPolicyTickStep = 1 * time.Millisecond
	opts.EvictPolicyRelevanceWindow = 100 * time.Millisecond
	opts.CacheFillTimeout = 10 * time.Millisecond
	opts.TtlTickStep = 1 * time.Millisecond

	e, err := engine.NewEngine(&opts)
	assert.Nil(t, err)

	start := time.Now()
	N := 10 * 1000
	ijMu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(N)

	for i := 0; i < N; i++ {
		ijMu.Lock()
		go func(j int) {
			e.Get(strconv.Itoa(j)) // timeout errors abound
			wg.Done()
		}(i)
		ijMu.Unlock()
	}

	wg.Wait()
	fmt.Println(time.Since(start))
}
