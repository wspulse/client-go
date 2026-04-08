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

func TestOnDisconnect_CallbackFires(t *testing.T) {
	t.Parallel()
	disconnected := make(chan error, 1)
	c, _, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)

	_ = c.Close()

	_ = requireReceive(t, disconnected)
}

func TestOnDisconnect_NilOnNormalClose(t *testing.T) {
	t.Parallel()
	disconnectErr := make(chan error, 1)
	c, _, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)

	_ = c.Close()

	got := requireReceive(t, disconnectErr)
	assert.NoError(t, got, "want nil error on normal Close()")
}

func TestOnDisconnect_NonNilOnServerDrop(t *testing.T) {
	t.Parallel()
	disconnectErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Simulate server drop.
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	got := requireReceive(t, disconnectErr)
	assert.Error(t, got, "want non-nil error on server drop")
}

func TestOnDisconnect_IsErrConnectionLostOnServerDrop(t *testing.T) {
	t.Parallel()
	disconnectErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Simulate server drop.
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	got := requireReceive(t, disconnectErr)
	assert.ErrorIs(t, got, client.ErrConnectionLost)
}

func TestOnDisconnect_NonNilOnMaxRetries(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{err: errors.New("connection refused")},
		mockDialResult{err: errors.New("connection refused")},
	)

	disconnectErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(2, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	got := requireReceive(t, disconnectErr)
	assert.Error(t, got, "want non-nil error on max retries exhausted")
}

func TestClose_OnDisconnectFiresExactlyOnce(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	disconnectCount := 0

	received := make(chan struct{}, 1)
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithOnDisconnect(func(err error) {
			mu.Lock()
			disconnectCount++
			mu.Unlock()
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Confirm connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	requireReceive(t, received)

	_ = c.Close()

	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	assert.Equal(t, 1, dc, "onDisconnect fired count")
}

// ── onTransportDrop error value ──────────────────────────────────────────────

func TestOnTransportDrop_NilOnClose(t *testing.T) {
	t.Parallel()
	transportDropErr := make(chan error, 1)
	c, _, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)

	_ = c.Close()

	got := requireReceive(t, transportDropErr)
	assert.NoError(t, got, "onTransportDrop should receive nil on user-initiated Close()")
}

func TestOnTransportDrop_NilOnClose_AutoReconnect(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	transportDropErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")

	_ = c.Close()

	got := requireReceive(t, transportDropErr)
	assert.NoError(t, got, "onTransportDrop should receive nil on user-initiated Close() with auto-reconnect")
}

func TestOnTransportDrop_NonNilOnServerDrop(t *testing.T) {
	t.Parallel()
	transportDropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Simulate server dropping the connection.
	injectedErr := &net.OpError{Op: "read", Err: errors.New("connection reset")}
	mt.InjectError(injectedErr)

	got := requireReceive(t, transportDropErr)
	assert.Error(t, got, "onTransportDrop should receive non-nil error on server-initiated drop")
}

func TestOnTransportDrop_WritePumpDataWriteError(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("i/o timeout")
	transportDropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Inject write error, then send a frame to trigger writePump's data path.
	mt.SetWriteError(writeErr)
	_ = c.Send(wspulse.Frame{Event: "ping"})

	got := requireReceive(t, transportDropErr)
	assert.ErrorIs(t, got, writeErr, "onTransportDrop should receive the write error, not a read-side error")
}

func TestOnTransportDrop_WritePumpPingWriteError(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("i/o timeout")
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	transportDropErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for the ticker to be registered, then inject write error and fire tick.
	<-fc.tickerAdded
	mt.SetWriteError(writeErr)
	fc.fireTicker(0)

	got := requireReceive(t, transportDropErr)
	fc.stopTicker(0)
	assert.ErrorIs(t, got, writeErr, "onTransportDrop should receive the ping write error, not a read-side error")
}

func TestOnTransportDrop_ReadError_NoWriteError(t *testing.T) {
	t.Parallel()
	readErr := &net.OpError{Op: "read", Err: errors.New("connection reset")}
	transportDropErr := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Inject a read error — no write error is set.
	mt.InjectError(readErr)

	got := requireReceive(t, transportDropErr)
	assert.ErrorIs(t, got, readErr, "onTransportDrop should receive readPump's own error when no write error exists")
}

func TestOnTransportDrop_ReadError_NotOverriddenByCloseInducedWriteError(t *testing.T) {
	t.Parallel()
	injectedReadErr := errors.New("server connection reset")

	mt := newMockTransport()
	mt.writeEntered = make(chan struct{}, 1)
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	transportDropErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithOnTransportDrop(func(err error) {
			transportDropErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Block writes and send a frame so writePump enters WriteMessage and blocks.
	rawUnblock := mt.BlockWrites()
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(rawUnblock) }
	defer unblock()
	require.NoError(t, c.Send(wspulse.Frame{Event: "trigger"}))
	<-mt.writeEntered // writePump is now blocked mid-WriteMessage

	closeStarted := make(chan struct{})
	writeReleased := make(chan struct{})

	// closeHook runs inside Close() after closeCh is closed but before
	// Close() returns. It signals closeStarted, then waits for the blocked
	// write to be released — ensuring writePump's close-induced error lands
	// on writeErrCh before Close() returns. Zero real-time sleeps.
	mt.closeHook = func() {
		close(closeStarted)
		<-writeReleased
	}
	go func() {
		<-closeStarted
		unblock() // release blocked WriteMessage → returns net.ErrClosed → sends on writeErrCh
		close(writeReleased)
	}()

	// Inject read error — readPump fails first.
	// readPump's defer calls Close() → closeCh closes → closeHook fires →
	// helper goroutine unblocks writePump → writePump sends close-induced
	// error on writeErrCh → closeHook returns → Close() returns.
	mt.InjectError(injectedReadErr)

	got := requireReceive(t, transportDropErr)
	assert.ErrorIs(t, got, injectedReadErr,
		"onTransportDrop should receive the original read error, not a close-induced write error")
}

func TestOnTransportDrop_WriteError_Reconnect_CleanCycle(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("i/o timeout")
	readErr := &net.OpError{Op: "read", Err: errors.New("connection reset")}

	mt1 := newMockTransport()
	mt2 := newMockTransport()
	spare := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
		mockDialResult{transport: spare},
	)

	transportDropErrs := make(chan error, 5)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			transportDropErrs <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// First cycle: trigger write error on mt1.
	mt1.SetWriteError(writeErr)
	_ = c.Send(wspulse.Frame{Event: "trigger"})

	got1 := requireReceive(t, transportDropErrs)
	assert.ErrorIs(t, got1, writeErr, "first drop should report write error")

	// Fire backoff timer for reconnect.
	<-fc.timerAdded
	fc.mu.Lock()
	fc.timers[len(fc.timers)-1].timer.Reset(0)
	fc.mu.Unlock()

	// Second cycle: trigger read error on mt2 (no write error set).
	mt2.InjectError(readErr)

	got2 := requireReceive(t, transportDropErrs)
	assert.ErrorIs(t, got2, readErr, "second drop should report read error, not stale write error from first cycle")
}

func TestOnTransportDrop_FiresOnReconnect(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	mt2 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
	)

	transportDropped := make(chan struct{}, 5)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			select {
			case transportDropped <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	requireReceive(t, transportDropped)
}

func TestOnTransportRestore_FiresAfterReconnect(t *testing.T) {
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

	transportDropped := make(chan struct{}, 5)
	transportRestored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			select {
			case transportDropped <- struct{}{}:
			default:
			}
		}),
		client.WithOnTransportRestore(func() {
			select {
			case transportRestored <- struct{}{}:
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

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Wait for transport drop.
	requireReceive(t, transportDropped)

	// Fire the backoff timer so reconnect proceeds.
	<-fc.timerAdded
	fc.mu.Lock()
	fc.timers[len(fc.timers)-1].timer.Reset(0)
	fc.mu.Unlock()

	// Wait for transport restore.
	requireReceive(t, transportRestored)

	// Fire any unexpected backoff timers so the client can close if a
	// secondary drop occurs under race detector scheduling.
	stopTimers := fireBackoffTimers(fc, 10)
	defer stopTimers()

	// Start echo loop on the second transport.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt2, echoDone)

	// Verify message delivery works after restore.
	require.NoError(t, c.Send(wspulse.Frame{Event: "post-restore", Payload: []byte(`"ok"`)}), "Send after restore")
	select {
	case f := <-received:
		assert.Equal(t, "post-restore", f.Event)
	case <-c.Done():
		t.Fatal("client closed before echo received")
	}
}

func TestOnTransportRestore_DoesNotFireOnInitialConnect(t *testing.T) {
	t.Parallel()
	var restoreCount atomic.Int32
	received := make(chan wspulse.Frame, 1)

	c, mt, _ := dialWithMock(t,
		client.WithOnTransportRestore(func() {
			restoreCount.Add(1)
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- f:
			default:
			}
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Round-trip a frame to prove all pumps are fully operational.
	require.NoError(t, c.Send(wspulse.Frame{Event: "probe"}), "Send failed")
	requireReceive(t, received)

	assert.Equal(t, int32(0), restoreCount.Load(), "onTransportRestore should not fire on initial connect")
}

func TestOnTransportRestore_NotFiredOnFailedReconnect(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		// All subsequent dials fail.
		mockDialResult{err: errors.New("connection refused")},
		mockDialResult{err: errors.New("connection refused")},
	)

	var restoreCount atomic.Int32
	disconnectErr := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(2, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportRestore(func() {
			restoreCount.Add(1)
		}),
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers as they appear so retries proceed.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	// Wait for onDisconnect with ErrRetriesExhausted.
	got := requireReceive(t, disconnectErr)
	assert.ErrorIs(t, got, client.ErrRetriesExhausted)

	assert.Equal(t, int32(0), restoreCount.Load(), "onTransportRestore should not fire on failed reconnect")
}
