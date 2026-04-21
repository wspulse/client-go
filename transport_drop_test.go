package client_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
)

// B1 — ServerClosedError: server close frame path (component — mock transport)

func TestOnTransportDrop_ServerClosedFrame_IsServerClosedError(t *testing.T) {
	t.Parallel()
	// The transport adapter returns *ServerClosedError directly when the
	// server sends a close frame. Inject it via mock to verify the
	// readPump → onTransportDrop propagation preserves Code and Reason.
	dropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(e error) { dropErr <- e }),
	)
	t.Cleanup(func() { _ = c.Close() })

	injected := &client.ServerClosedError{
		Code:   wspulse.StatusGoingAway,
		Reason: "server shutting down",
	}
	mt.InjectError(injected)

	got := requireReceive(t, dropErr)
	var sce *client.ServerClosedError
	require.ErrorAs(t, got, &sce,
		"want *ServerClosedError on server close frame, got: %v", got)
	assert.Equal(t, wspulse.StatusGoingAway, sce.Code)
	assert.Equal(t, "server shutting down", sce.Reason)
	assert.ErrorIs(t, got, &client.ServerClosedError{},
		"errors.Is should match any *ServerClosedError as a type-check shortcut")
}
