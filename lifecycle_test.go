package client_test

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestClose_WaitsForDisconnectCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	gate := make(chan struct{})
	c, _, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			// Block until the test releases the gate, proving Close()
			// waits for the callback to complete.
			<-gate
			callbackDone.Store(true)
		}),
	)

	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	close(gate)
	requireReceive(t, closeDone)
	require.True(t, callbackDone.Load(), "Close() returned before onDisconnect callback finished — orphaned callback goroutine")
}

func TestClose_WaitsForTransportDropCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	gate := make(chan struct{})
	c, _, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			<-gate
			callbackDone.Store(true)
		}),
	)

	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	close(gate)
	requireReceive(t, closeDone)
	require.True(t, callbackDone.Load(), "Close() returned before onTransportDrop callback finished — orphaned callback goroutine")
}

func TestClose_WaitsForDisconnectCallback_AutoReconnect(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	gate := make(chan struct{})
	c, _, _ := dialWithMock(t,
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			<-gate
			callbackDone.Store(true)
		}),
	)

	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	close(gate)
	requireReceive(t, closeDone)
	require.True(t, callbackDone.Load(), "Close() returned before onDisconnect callback finished — orphaned callback goroutine")
}

func TestClose_WaitsForGoroutines(t *testing.T) {
	t.Parallel()
	const count = 20
	type entry struct {
		c  client.Client
		mt *mockTransport
	}
	entries := make([]entry, count)
	for i := range entries {
		mt := newMockTransport()
		fc := newFakeClock()
		md := newMockDialer(mockDialResult{transport: mt})
		c, err := client.Dial("ws://mock",
			client.WithDialer(md),
			client.WithClock(fc),
		)
		require.NoError(t, err, "Dial #%d failed", i)
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for _, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		requireReceive(t, closeDone)
		requireDone(t, e.c)
	}
}

func TestClose_WaitsForGoroutines_AutoReconnect(t *testing.T) {
	t.Parallel()
	const count = 20
	type entry struct {
		c  client.Client
		mt *mockTransport
	}
	entries := make([]entry, count)
	for i := range entries {
		mt := newMockTransport()
		fc := newFakeClock()
		md := newMockDialer(mockDialResult{transport: mt})
		c, err := client.Dial("ws://mock",
			client.WithDialer(md),
			client.WithClock(fc),
			client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		)
		require.NoError(t, err, "Dial #%d failed", i)
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for _, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		requireReceive(t, closeDone)
		requireDone(t, e.c)
	}
}

func TestDone_FiresOnServerDrop(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop.
	echoDone := make(chan struct{})
	go echoLoop(mt, echoDone)

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	requireReceive(t, received)

	// Stop echo and simulate server drop.
	close(echoDone)
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	requireDone(t, c)
	assert.ErrorIs(t, c.Send(wspulse.Frame{Event: "msg"}), wspulse.ErrConnectionClosed)
}

func TestConcurrentSendAndClose_NoRace(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)

	const senders = 8
	started := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			for j := 0; j < 50; j++ {
				_ = c.Send(wspulse.Frame{Event: "msg", Payload: []byte(`"x"`)})
			}
		}()
	}
	// Wait for all sender goroutines to start before closing.
	for i := 0; i < senders; i++ {
		<-started
	}
	_ = c.Close()
	wg.Wait()
}

func TestConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	var disconnectCount atomic.Int32
	disconnectDone := make(chan struct{}, 1)
	received := make(chan struct{}, 1)

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithOnDisconnect(func(err error) {
			disconnectCount.Add(1)
			select {
			case disconnectDone <- struct{}{}:
			default:
			}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	requireReceive(t, received)

	// Simultaneously close the client and drop the transport.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Close()
	}()
	go func() {
		defer wg.Done()
		mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	requireReceive(t, done)

	// Wait for onDisconnect to fire.
	requireReceive(t, disconnectDone)

	// Wait for Done() to close, which confirms all teardown is complete.
	requireDone(t, c)

	assert.Equal(t, int32(1), disconnectCount.Load(), "onDisconnect fired count")
}
