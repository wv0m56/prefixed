package origin

import (
	"io"
	"time"
)

// Origin is to be implemented by objects which serve as the cache engine's
// backend. Fetch fetches the data associated with key (usually over the network)
// and returns it as a reader stream.
type Origin interface {
	Fetch(key string, timeout time.Duration) io.ReadCloser
}
