package origin

import "io"

type OriginPlugin interface {
	ReadFrom(key string, rc io.ReadCloser)
}
