package client_test

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestAutoReconnect_ReconnectsAndDeliversMessages(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	mt2 := newMockTransport()
	spare := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
		mockDialResult{transport: spare},
	)

	restored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportRestore(func() {
			select {
			case restored <- struct{}{}:
			default:
			}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- f:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop on mt1.
	echo1Done := make(chan struct{})
	go echoLoop(mt1, echo1Done)

	// Verify initial connectivity.
	require.NoError(t, c.Send(wspulse.Frame{Event: "before", Payload: []byte(`"1"`)}), "Send before kick")
	f := <-received
	assert.Equal(t, "before", f.Event)

	// Stop echo on mt1, then kill the transport.
	close(echo1Done)
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire the backoff timer.
	<-fc.timerAdded
	fc.mu.Lock()
	fc.timers[len(fc.timers)-1].timer.Reset(0)
	fc.mu.Unlock()

	// Wait for onTransportRestore.
	<-restored

	// Fire any unexpected backoff timers so the client can close if a
	// secondary drop occurs under race detector scheduling.
	stopTimers := fireBackoffTimers(fc, 10)
	defer stopTimers()

	// Start echo loop on mt2.
	echo2Done := make(chan struct{})
	defer close(echo2Done)
	go echoLoop(mt2, echo2Done)

	// Verify post-reconnect message delivery.
	require.NoError(t, c.Send(wspulse.Frame{Event: "after", Payload: []byte(`"2"`)}), "Send after reconnect")
	select {
	case f2 := <-received:
		assert.Equal(t, "after", f2.Event)
	case <-c.Done():
		t.Fatal("client closed before echo received")
	}
}

func TestAutoReconnect_MaxRetriesExhausted_ClosesDone(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{err: errors.New("connection refused")},
		mockDialResult{err: errors.New("connection refused")},
	)

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(2, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers as they appear.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	<-c.Done()
}

func TestAutoReconnect_CloseDuringBackoff(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	mt2 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
	)

	transportDropped := make(chan struct{}, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		// Long backoff so Close() hits while the timer is still running.
		client.WithAutoReconnect(3, 10*time.Second, 30*time.Second),
		client.WithOnTransportDrop(func(err error) {
			select {
			case transportDropped <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	<-transportDropped

	// Wait for the backoff timer to be created.
	<-fc.timerAdded

	// Close while timer is still pending (we do NOT fire it).
	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	<-closeDone
}

func TestAutoReconnect_MultipleRapidCycles(t *testing.T) {
	t.Parallel()
	const cycles = 3
	// Allocate extra transports beyond the expected cycles to absorb
	// any spurious secondary drops under race detector scheduling.
	const spares = 3
	transports := make([]*mockTransport, cycles+1+spares)
	results := make([]mockDialResult, cycles+1+spares)
	for i := range transports {
		transports[i] = newMockTransport()
		results[i] = mockDialResult{transport: transports[i]}
	}

	fc := newFakeClock()
	md := newMockDialer(results...)

	var dropCount atomic.Int32
	var restoreCount atomic.Int32
	received := make(chan wspulse.Frame, 10)
	restored := make(chan struct{}, cycles)

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(10, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			dropCount.Add(1)
		}),
		client.WithOnTransportRestore(func() {
			restoreCount.Add(1)
			restored <- struct{}{}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- f:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Drain the initial dial signal from client.Dial().
	<-md.dialCalled

	for i := 0; i < cycles; i++ {
		// Kill the current transport.
		transports[i].InjectError(&net.OpError{Op: "read", Err: errors.New("reset")})

		// Fire the backoff timer.
		<-fc.timerAdded
		fc.mu.Lock()
		fc.timers[len(fc.timers)-1].timer.Reset(0)
		fc.mu.Unlock()

		// Wait for dial to happen.
		<-md.dialCalled

		// Wait for restore callback, confirming pumps are running.
		<-restored
	}

	// Fire any unexpected backoff timers so the client can exhaust retries
	// and close if a secondary drop occurs under race detector scheduling.
	stopTimers := fireBackoffTimers(fc, 10)
	defer stopTimers()

	// After cycles reconnects, verify echo on the final transport.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(transports[cycles], echoDone)

	require.NoError(t, c.Send(wspulse.Frame{Event: "final", Payload: []byte(`"ok"`)}), "Send after cycles")
	select {
	case f := <-received:
		assert.Equal(t, "final", f.Event)
	case <-c.Done():
		t.Fatal("client closed before echo received")
	}

	assert.GreaterOrEqual(t, dropCount.Load(), int32(cycles), "transport drop count")
	assert.GreaterOrEqual(t, restoreCount.Load(), int32(cycles), "transport restore count")
}

func TestAutoReconnect_Close_FiresOnDisconnect(t *testing.T) {
	t.Parallel()
	disconnected := make(chan struct{}, 1)
	received := make(chan struct{}, 1)
	c, mt, _ := dialWithMock(t,
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			select {
			case disconnected <- struct{}{}:
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

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	<-received

	_ = c.Close()

	<-disconnected
}
