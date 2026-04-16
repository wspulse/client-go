package client_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wspulse/client-go"
)

// B1 — ErrServerClosed: server close frame path (component — mock transport)

func TestOnTransportDrop_ServerClosedFrame_IsErrServerClosed(t *testing.T) {
	t.Parallel()
	// The transport adapter returns ErrServerClosed directly when the server
	// sends a close frame. Inject it via mock to verify the
	// readPump → onTransportDrop propagation path.
	dropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(e error) { dropErr <- e }),
	)
	t.Cleanup(func() { _ = c.Close() })

	mt.InjectError(client.ErrServerClosed)

	got := requireReceive(t, dropErr)
	assert.ErrorIs(t, got, client.ErrServerClosed,
		"want ErrServerClosed on server close frame, got: %v", got)
}
