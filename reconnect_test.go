package client_test

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestAutoReconnect_ReconnectsAndDeliversMessages(t *testing.T) {
	t.Parallel()
	mt1 := newMockTransport()
	mt2 := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(
		mockDialResult{transport: mt1},
		mockDialResult{transport: mt2},
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop on mt1.
	echo1Done := make(chan struct{})
	go echoLoop(mt1, echo1Done)

	// Verify initial connectivity.
	if err := c.Send(wspulse.Frame{Event: "before", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send before kick: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "before" {
			t.Fatalf("want event %q, got %q", "before", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo before kick")
	}

	// Stop echo on mt1, then kill the transport.
	close(echo1Done)
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire the backoff timer.
	deadline := time.Now().Add(time.Second)
	for fc.TimerCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	fc.mu.Lock()
	fc.timers[len(fc.timers)-1].timer.Reset(0)
	fc.mu.Unlock()

	// Wait for onTransportRestore.
	select {
	case <-restored:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onTransportRestore")
	}

	// Start echo loop on mt2.
	echo2Done := make(chan struct{})
	defer close(echo2Done)
	go echoLoop(mt2, echo2Done)

	// Verify post-reconnect message delivery.
	if err := c.Send(wspulse.Frame{Event: "after", Payload: []byte(`"2"`)}); err != nil {
		t.Fatalf("Send after reconnect: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "after" {
			t.Fatalf("want event %q, got %q", "after", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo after reconnect")
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	// Fire backoff timers as they appear.
	stopTimers := fireBackoffTimers(fc, 5)
	defer stopTimers()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: Done() did not close after max retries exhausted")
	}
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// Kill the first transport.
	mt1.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	select {
	case <-transportDropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transport drop")
	}

	// Wait for the backoff timer to be created.
	deadline := time.Now().Add(time.Second)
	for fc.TimerCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// Close while timer is still pending (we do NOT fire it).
	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close() hung during backoff — timer not stopped")
	}
}

func TestAutoReconnect_MultipleRapidCycles(t *testing.T) {
	t.Parallel()
	const cycles = 3
	transports := make([]*mockTransport, cycles+1)
	results := make([]mockDialResult, cycles+1)
	for i := range transports {
		transports[i] = newMockTransport()
		results[i] = mockDialResult{transport: transports[i]}
	}

	fc := newFakeClock()
	md := newMockDialer(results...)

	var dropCount atomic.Int32
	var restoreCount atomic.Int32
	received := make(chan wspulse.Frame, 10)

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithAutoReconnect(10, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			dropCount.Add(1)
		}),
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	for i := 0; i < cycles; i++ {
		// Kill the current transport.
		transports[i].InjectError(&net.OpError{Op: "read", Err: errors.New("reset")})

		// Fire the backoff timer.
		dl := time.Now().Add(time.Second)
		prevTimerCount := fc.TimerCount()
		for fc.TimerCount() <= prevTimerCount && time.Now().Before(dl) {
			time.Sleep(time.Millisecond)
		}
		fc.mu.Lock()
		fc.timers[len(fc.timers)-1].timer.Reset(0)
		fc.mu.Unlock()

		// Wait for dial to happen.
		select {
		case <-md.dialCalled:
		case <-time.After(time.Second):
			t.Fatalf("cycle %d: timed out waiting for dial", i)
		}

		// After dialCalled fires, pumps start asynchronously.
		// Brief yield is sufficient in mock transport (no real TCP).
		time.Sleep(10 * time.Millisecond)
	}

	// After cycles reconnects, verify echo on the final transport.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(transports[cycles], echoDone)

	if err := c.Send(wspulse.Frame{Event: "final", Payload: []byte(`"ok"`)}); err != nil {
		t.Fatalf("Send after cycles: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "final" {
			t.Fatalf("want event %q, got %q", "final", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo after rapid cycles")
	}

	if dc := dropCount.Load(); dc < int32(cycles) {
		t.Errorf("transport drop count = %d, want >= %d", dc, cycles)
	}
	if rc := restoreCount.Load(); rc < int32(cycles) {
		t.Errorf("transport restore count = %d, want >= %d", rc, cycles)
	}
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
	if err := c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("timed out: onDisconnect did not fire after Close() with auto-reconnect")
	}
}
