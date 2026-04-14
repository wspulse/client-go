package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wspulse/client-go"
)

// A1 — ErrNetworkUnhealthy: pong timeout path

func TestOnTransportDrop_PongTimeout_IsErrNetworkUnhealthy(t *testing.T) {
	t.Parallel()
	// pingFunc returns DeadlineExceeded immediately so the test does not
	// depend on any real wall-clock timeout. OnTransportDrop must receive
	// ErrNetworkUnhealthy.
	mt := newMockTransport()
	mt.pingFunc = func(context.Context) error {
		return context.DeadlineExceeded
	}
	fc := newFakeClock()

	dropErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(newMockDialer(mockDialResult{transport: mt})),
		client.WithClock(fc),
		client.WithPingInterval(50*time.Millisecond),
		client.WithOnTransportDrop(func(e error) { dropErr <- e }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Wait for pingPump to register its ticker, then fire it.
	requireReceive(t, fc.tickerAdded)
	fc.fireTicker(0)

	got := requireReceive(t, dropErr)
	assert.ErrorIs(t, got, client.ErrNetworkUnhealthy,
		"want ErrNetworkUnhealthy on pong timeout, got: %v", got)
}

// B1 — ErrServerClosed: server close frame path (component — mock transport)

func TestOnTransportDrop_ServerClosedFrame_IsErrServerClosed(t *testing.T) {
	t.Parallel()
	// The transport adapter wraps coder/websocket close frame errors into
	// ErrServerClosed before they reach readPump. Inject the already-wrapped
	// error via mock to verify the readPump → onTransportDrop plumbing.
	dropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(e error) { dropErr <- e }),
	)
	t.Cleanup(func() { _ = c.Close() })

	mt.InjectError(fmt.Errorf("wspulse: server closed connection: %w", client.ErrServerClosed))

	got := requireReceive(t, dropErr)
	assert.ErrorIs(t, got, client.ErrServerClosed,
		"want ErrServerClosed on server close frame, got: %v", got)
}
