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
	conn, _, err := websocket.DefaultDialer.Dial(url, requestHeader)
	return conn, err
}
