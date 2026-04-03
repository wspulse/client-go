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

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnDisconnect callback")
	}
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

	select {
	case got := <-disconnectErr:
		assert.NoError(t, got, "want nil error on normal Close()")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
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

	select {
	case got := <-disconnectErr:
		assert.Error(t, got, "want non-nil error on server drop")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
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

	select {
	case got := <-disconnectErr:
		assert.ErrorIs(t, got, client.ErrConnectionLost)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
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

	select {
	case got := <-disconnectErr:
		assert.Error(t, got, "want non-nil error on max retries exhausted")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
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
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	_ = c.Close()

	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	assert.Equal(t, 1, dc, "onDisconnect fired count")
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

	select {
	case <-transportDropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnTransportDrop")
	}
}

func TestOnTransportRestore_FiresAfterReconnect(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	mt2 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
	)

	transportDropped := make(chan struct{}, 5)
	transportRestored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
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
	select {
	case <-transportDropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onTransportDrop")
	}

	// Fire the backoff timer so reconnect proceeds.
	deadline := time.Now().Add(time.Second)
	for fc.TimerCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.Greater(t, fc.TimerCount(), 0, "no backoff timer created")
	fc.mu.Lock()
	fc.timers[len(fc.timers)-1].timer.Reset(0)
	fc.mu.Unlock()

	// Wait for transport restore.
	select {
	case <-transportRestored:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onTransportRestore")
	}

	// Start echo loop on the second transport.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt2, echoDone)

	// Verify message delivery works after restore.
	require.NoError(t, c.Send(wspulse.Frame{Event: "post-restore", Payload: []byte(`"ok"`)}), "Send after restore")
	select {
	case f := <-received:
		assert.Equal(t, "post-restore", f.Event)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo after restore")
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
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for probe echo")
	}

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
	select {
	case got := <-disconnectErr:
		assert.ErrorIs(t, got, client.ErrRetriesExhausted)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	assert.Equal(t, int32(0), restoreCount.Load(), "onTransportRestore should not fire on failed reconnect")
}
