package client_test

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		if got != nil {
			t.Errorf("want nil error on normal Close(), got %v", got)
		}
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
		if got == nil {
			t.Error("want non-nil error on server drop, got nil")
		}
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
		if !errors.Is(got, client.ErrConnectionLost) {
			t.Errorf("want errors.Is(err, ErrConnectionLost), got %v", got)
		}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	select {
	case got := <-disconnectErr:
		if got == nil {
			t.Error("want non-nil error on max retries exhausted, got nil")
		}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Confirm connection is established by round-tripping a frame.
	if err := c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	_ = c.Close()

	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	if dc != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", dc)
	}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
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
	if fc.TimerCount() == 0 {
		t.Fatal("no backoff timer created")
	}
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
	if err := c.Send(wspulse.Frame{Event: "post-restore", Payload: []byte(`"ok"`)}); err != nil {
		t.Fatalf("Send after restore: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "post-restore" {
			t.Fatalf("want event %q, got %q", "post-restore", f.Event)
		}
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
	if err := c.Send(wspulse.Frame{Event: "probe"}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for probe echo")
	}

	if count := restoreCount.Load(); count != 0 {
		t.Errorf("onTransportRestore fired %d times on initial connect, want 0", count)
	}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers as they appear so retries proceed.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	// Wait for onDisconnect with ErrRetriesExhausted.
	select {
	case got := <-disconnectErr:
		if !errors.Is(got, client.ErrRetriesExhausted) {
			t.Errorf("want ErrRetriesExhausted, got %v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	if count := restoreCount.Load(); count != 0 {
		t.Errorf("onTransportRestore fired %d times, want 0", count)
	}
}
