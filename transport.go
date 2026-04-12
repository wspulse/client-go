package client

import (
	"context"

	"github.com/coder/websocket"

	wspulse "github.com/wspulse/core"
)

// Compile-time assertions: numeric values must match between coder/websocket
// and wspulse/core (both follow RFC 6455). A mismatch here means frames
// would be silently mis-typed at runtime.
var _ = [1]struct{}{}[websocket.MessageText-websocket.MessageType(wspulse.TextMessage)]
var _ = [1]struct{}{}[websocket.MessageBinary-websocket.MessageType(wspulse.BinaryMessage)]

// coderTransport wraps *websocket.Conn to satisfy core.Transport.
// All type conversions are simple casts — numeric values are identical
// between coder/websocket and wspulse/core (both follow RFC 6455).
type coderTransport struct {
	conn *websocket.Conn
}

func (t *coderTransport) Read(ctx context.Context) (wspulse.MessageType, []byte, error) {
	typ, data, err := t.conn.Read(ctx)
	return wspulse.MessageType(typ), data, err
}

func (t *coderTransport) Write(ctx context.Context, typ wspulse.MessageType, data []byte) error {
	return t.conn.Write(ctx, websocket.MessageType(typ), data)
}

func (t *coderTransport) Ping(ctx context.Context) error {
	return t.conn.Ping(ctx)
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
