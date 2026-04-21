package client

import (
	"context"
	"errors"

	"github.com/coder/websocket"

	wspulse "github.com/wspulse/core"
)

// transport is the client-local WebSocket connection contract.
// It omits Ping — dead-connection detection is server-side only (hub heartbeat).
type transport interface {
	Read(ctx context.Context) (wspulse.MessageType, []byte, error)
	Write(ctx context.Context, typ wspulse.MessageType, data []byte) error
	SetReadLimit(n int64)
	Close(code wspulse.StatusCode, reason string) error
	CloseNow() error
}

// Compile-time assertions: numeric values must match between coder/websocket
// and wspulse/core (both follow RFC 6455). A mismatch here means frames
// would be silently mis-typed at runtime.
var _ = [1]struct{}{}[websocket.MessageText-websocket.MessageType(wspulse.TextMessage)]
var _ = [1]struct{}{}[websocket.MessageBinary-websocket.MessageType(wspulse.BinaryMessage)]

// coderTransport wraps *websocket.Conn to satisfy the local transport interface.
// All type conversions are simple casts — numeric values are identical
// between coder/websocket and wspulse/core (both follow RFC 6455).
type coderTransport struct {
	conn *websocket.Conn
}

var _ transport = (*coderTransport)(nil)

func (t *coderTransport) Read(ctx context.Context) (wspulse.MessageType, []byte, error) {
	typ, data, err := t.conn.Read(ctx)
	if err != nil {
		// Server sent a WebSocket close frame. Extract the exact code and
		// reason into *ServerClosedError so callers can inspect them via
		// errors.As without importing coder/websocket.
		var ce websocket.CloseError
		if errors.As(err, &ce) {
			err = &ServerClosedError{
				Code:   wspulse.StatusCode(ce.Code),
				Reason: ce.Reason,
			}
		}
	}
	return wspulse.MessageType(typ), data, err
}

func (t *coderTransport) Write(ctx context.Context, typ wspulse.MessageType, data []byte) error {
	return t.conn.Write(ctx, websocket.MessageType(typ), data)
}

func (t *coderTransport) SetReadLimit(n int64) {
	t.conn.SetReadLimit(n)
}

func (t *coderTransport) Close(code wspulse.StatusCode, reason string) error {
	return t.conn.Close(websocket.StatusCode(code), reason)
}

func (t *coderTransport) CloseNow() error {
	return t.conn.CloseNow()
}
