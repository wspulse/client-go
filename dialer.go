package client

import (
	"net/http"

	"github.com/gorilla/websocket"

	wspulse "github.com/wspulse/core"
)

// dialer abstracts the WebSocket dial operation for testability.
type dialer interface {
	Dial(url string, requestHeader http.Header) (wspulse.Transport, error)
}

// gorillaDialer uses gorilla/websocket.DefaultDialer.
type gorillaDialer struct{}

func (gorillaDialer) Dial(url string, requestHeader http.Header) (wspulse.Transport, error) {
	conn, resp, err := websocket.DefaultDialer.Dial(url, requestHeader)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn, err
}
