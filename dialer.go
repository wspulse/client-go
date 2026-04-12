package client

import (
	"context"
	"net/http"

	"github.com/coder/websocket"

	wspulse "github.com/wspulse/core"
)

// dialer abstracts the WebSocket dial operation for testability.
type dialer interface {
	Dial(ctx context.Context, url string, requestHeader http.Header) (wspulse.Transport, error)
}

// coderDialer uses github.com/coder/websocket.
type coderDialer struct{}

func (coderDialer) Dial(ctx context.Context, url string, requestHeader http.Header) (wspulse.Transport, error) {
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: requestHeader,
	})
	if err != nil {
		return nil, err
	}
	return &coderTransport{conn: conn}, nil
}
