package client

import "io"

// ClientPlugin interfaces network API into engine's Go API. It handles
// requests from network clients and sends them replies.
type ClientPlugin interface {
	ReplyTo(key, w io.Writer)
}
