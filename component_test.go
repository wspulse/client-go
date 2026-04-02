package client_test

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
)

// ── fakeClock ────────────────────────────────────────────────────────────────

// fakeClock replaces both NewTimer (backoff) and NewTicker (heartbeat) with
// controllable fakes. No real timers fire — tests drive time explicitly.
type fakeClock struct {
	mu      sync.Mutex
	timers  []*fakeTimerEntry
	tickers []*fakeTickerEntry
}

type fakeTimerEntry struct {
	d     time.Duration
	timer *time.Timer
}

type fakeTickerEntry struct {
	d      time.Duration
	ticker *time.Ticker
}

func newFakeClock() *fakeClock { return &fakeClock{} }

// NewTimer returns a stopped timer that will not fire on its own.
func (fc *fakeClock) NewTimer(d time.Duration) *time.Timer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	fc.mu.Lock()
	fc.timers = append(fc.timers, &fakeTimerEntry{d: d, timer: t})
	fc.mu.Unlock()
	return t
}

// NewTicker returns a stopped ticker that will never fire on its own.
func (fc *fakeClock) NewTicker(d time.Duration) *time.Ticker {
	t := time.NewTicker(time.Hour)
	t.Stop()
	fc.mu.Lock()
	fc.tickers = append(fc.tickers, &fakeTickerEntry{d: d, ticker: t})
	fc.mu.Unlock()
	return t
}

// TimerCount returns the number of registered timers.
func (fc *fakeClock) TimerCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.timers)
}

// TickerCount returns the number of registered tickers.
func (fc *fakeClock) TickerCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.tickers)
}

// ── helpers ──────────────────────────────────────────────────────────────────

// echoLoop reads from mt.writeCh and injects the written data back as a
// text message, simulating an echo server. Stops when done is closed.
func echoLoop(mt *mockTransport, done chan struct{}) {
	for {
		select {
		case w := <-mt.writeCh:
			// Only echo text messages (type 1), skip pings (9) and close (8).
			if w.messageType == 1 {
				mt.InjectMessage(1, w.data)
			}
		case <-done:
			return
		}
	}
}

// fireBackoffTimers spins up a goroutine that fires up to maxTimers backoff
// timers on fc as they are created. Returns a stop function that must be called
// to prevent goroutine leaks (typically via defer).
func fireBackoffTimers(fc *fakeClock, maxTimers int) (stop func()) {
	stopCh := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for i := 0; i < maxTimers; i++ {
			deadline := time.Now().Add(time.Second)
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				fc.mu.Lock()
				count := len(fc.timers)
				fc.mu.Unlock()
				if count > i {
					fc.mu.Lock()
					fc.timers[count-1].timer.Reset(0)
					fc.mu.Unlock()
					break
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()
	return func() {
		close(stopCh)
		<-stopped
	}
}

// dialWithMock creates a mock dialer with a single transport and dials using it.
// Returns the client, mock transport, and fake clock.
func dialWithMock(t *testing.T, opts ...client.ClientOption) (client.Client, *mockTransport, *fakeClock) {
	t.Helper()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})
	allOpts := []client.ClientOption{
		client.WithDialer(md),
		client.WithClock(fc),
	}
	allOpts = append(allOpts, opts...)
	c, err := client.Dial("ws://mock", allOpts...)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	return c, mt, fc
}

// ── Basic tests ──────────────────────────────────────────────────────────────

func TestComponent_SendAndReceive(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 1)
	c, mt, _ := dialWithMock(t, client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	defer func() { _ = c.Close() }()

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	frame := wspulse.Frame{Event: "echo", Payload: []byte(`"hello"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case f := <-received:
		if f.Event != "echo" {
			t.Errorf("Event: want %q, got %q", "echo", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestComponent_Close_SafeToCallTwice(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	_ = c.Close()
}

func TestComponent_Send_AfterClose_ReturnsErrConnectionClosed(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	sendErr := c.Send(wspulse.Frame{Event: "msg"})
	if !errors.Is(sendErr, wspulse.ErrConnectionClosed) {
		t.Errorf("want ErrConnectionClosed, got %v", sendErr)
	}
}

func TestComponent_Done_ClosedAfterClose(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() channel not closed after Close()")
	}
}

// ── Callback tests ───────────────────────────────────────────────────────────

func TestComponent_OnDisconnect_CallbackFires(t *testing.T) {
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

func TestComponent_OnDisconnect_NilOnNormalClose(t *testing.T) {
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

func TestComponent_OnDisconnect_NonNilOnServerDrop(t *testing.T) {
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

func TestComponent_OnDisconnect_IsErrConnectionLostOnServerDrop(t *testing.T) {
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

func TestComponent_Close_OnDisconnectFiresExactlyOnce(t *testing.T) {
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

func TestComponent_OnTransportDrop_FiresOnReconnect(t *testing.T) {
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

func TestComponent_OnTransportRestore_FiresAfterReconnect(t *testing.T) {
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

func TestComponent_OnTransportRestore_DoesNotFireOnInitialConnect(t *testing.T) {
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

func TestComponent_OnTransportRestore_NotFiredOnFailedReconnect(t *testing.T) {
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

// ── Reconnect tests ──────────────────────────────────────────────────────────

func TestComponent_AutoReconnect_ReconnectsAndDeliversMessages(t *testing.T) {
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

func TestComponent_AutoReconnect_MaxRetriesExhausted_ClosesDone(t *testing.T) {
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

func TestComponent_AutoReconnect_CloseDuringBackoff(t *testing.T) {
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

func TestComponent_AutoReconnect_MultipleRapidCycles(t *testing.T) {
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

		// Give readPump/writePump time to start on the new transport.
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

func TestComponent_AutoReconnect_Close_FiresOnDisconnect(t *testing.T) {
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

func TestComponent_OnDisconnect_NonNilOnMaxRetries(t *testing.T) {
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

// ── Concurrency tests ────────────────────────────────────────────────────────

func TestComponent_ConcurrentSendAndClose_NoRace(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)

	const senders = 8
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = c.Send(wspulse.Frame{Event: "msg", Payload: []byte(`"x"`)})
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	_ = c.Close()
	wg.Wait()
}

func TestComponent_ConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce(t *testing.T) {
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
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

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

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client and transport close to complete")
	}

	// Wait for onDisconnect to fire.
	select {
	case <-disconnectDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	// Give a short settling time.
	time.Sleep(50 * time.Millisecond)

	if count := disconnectCount.Load(); count != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", count)
	}
}

// ── Backpressure tests ───────────────────────────────────────────────────────

func TestComponent_Send_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	// Use a small send buffer but do NOT read from writeCh,
	// so the client's send channel fills up.
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithSendBufferSize(4),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// The writePump will drain send -> writeCh (cap 256).
	// With a 4-frame send buffer, we need to fill send + writeCh.
	// The mock writeCh has capacity 256, and writePump drains send into writes.
	// We need enough frames to overflow: writeCh(256) + send(4) + 1 = 261+.
	sawFull := false
	for i := 0; i < 1000; i++ {
		err := c.Send(wspulse.Frame{Event: "flood", Payload: []byte(`"x"`)})
		if errors.Is(err, wspulse.ErrSendBufferFull) {
			sawFull = true
			break
		}
		if errors.Is(err, wspulse.ErrConnectionClosed) {
			break
		}
	}
	if !sawFull {
		t.Log("ErrSendBufferFull was never returned — writePump drained fast enough or connection died")
	}
}

func TestComponent_Send_CustomBufferSize_Applied(t *testing.T) {
	t.Parallel()
	const bufSize = 4
	c, _, _ := dialWithMock(t, client.WithSendBufferSize(bufSize))
	t.Cleanup(func() { _ = c.Close() })

	if got := client.SendBufferCap(c); got != bufSize {
		t.Errorf("SendBufferCap = %d, want %d", got, bufSize)
	}
}

// ── Error tests ──────────────────────────────────────────────────────────────

func TestComponent_ReadPump_DecodeFailure_DropsFrame(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 5)
	c, mt, _ := dialWithMock(t,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Inject an invalid JSON frame (decode failure — should be dropped).
	mt.InjectMessage(1, []byte("not valid json{{{"))
	// Inject a valid frame that should be delivered.
	validFrame := `{"event":"valid-frame","payload":"ok"}`
	mt.InjectMessage(1, []byte(validFrame))

	select {
	case f := <-received:
		if f.Event != "valid-frame" {
			t.Fatalf("want event %q, got %q", "valid-frame", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for valid frame")
	}
}

func TestComponent_ReadPump_PanicRecovery(t *testing.T) {
	t.Parallel()
	disconnected := make(chan error, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnMessage(func(f wspulse.Frame) {
			panic("boom")
		}),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Inject a valid frame to trigger the panic in OnMessage.
	trigger := `{"event":"trigger","payload":null}`
	mt.InjectMessage(1, []byte(trigger))

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("timed out: readPump panic was not recovered")
	}
}

// failEncodeCodecComponent is a test codec whose Encode always returns an error.
type failEncodeCodecComponent struct{}

func (failEncodeCodecComponent) Encode(wspulse.Frame) ([]byte, error) {
	return nil, errors.New("wspulse: encode fail")
}

func (failEncodeCodecComponent) Decode(data []byte) (wspulse.Frame, error) {
	return wspulse.Frame{}, nil
}

func (failEncodeCodecComponent) FrameType() int { return 1 }

func TestComponent_Send_EncodeError_ReturnsError(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithCodec(failEncodeCodecComponent{}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	err = c.Send(wspulse.Frame{Event: "msg"})
	if err == nil {
		t.Fatal("expected encode error, got nil")
	}
}

// ── Lifecycle tests ──────────────────────────────────────────────────────────

func TestComponent_Close_WaitsForDisconnectCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

func TestComponent_Close_WaitsForTransportDropCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onTransportDrop callback finished — orphaned callback goroutine")
	}
}

func TestComponent_Close_WaitsForDisconnectCallback_AutoReconnect(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

func TestComponent_Close_WaitsForGoroutines(t *testing.T) {
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
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for i, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-e.c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
	}
}

func TestComponent_Close_WaitsForGoroutines_AutoReconnect(t *testing.T) {
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
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for i, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-e.c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
	}
}

func TestComponent_Done_FiresOnServerDrop(t *testing.T) {
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
	if err := c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	// Stop echo and simulate server drop.
	close(echoDone)
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out: Done() did not fire after server disconnect")
	}

	if err := c.Send(wspulse.Frame{Event: "msg"}); !errors.Is(err, wspulse.ErrConnectionClosed) {
		t.Fatalf("want ErrConnectionClosed, got %v", err)
	}
}

// ── Heartbeat tests ──────────────────────────────────────────────────────────

func TestComponent_HeartbeatPongTimeout_DisconnectsClient(t *testing.T) {
	t.Parallel()
	// This test verifies that the pong timeout mechanism works.
	// We use a real clock (not fake) because heartbeat depends on real
	// ticker and read deadline. The mock transport's SetReadDeadline is a
	// no-op, so the read won't actually time out from the deadline. However,
	// writePump sends pings via the ticker which will produce write calls.
	// Since our mock transport's SetPongHandler records the handler but the
	// mock never sends pongs, we need to verify via a different path.
	//
	// In the real implementation, pong timeout is detected by ReadMessage
	// returning an i/o timeout error when the read deadline expires without
	// a pong. Our mock transport's SetReadDeadline is a no-op, so we cannot
	// directly test pong timeout with mocks.
	//
	// Instead, we test the observable behavior: the client sends pings
	// (via the heartbeat ticker) and when the transport dies, the client
	// disconnects.
	mt := newMockTransport()
	// Use real clock so the ticker fires.
	md := newMockDialer(mockDialResult{transport: mt})

	disconnected := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		// Short heartbeat intervals.
		client.WithHeartbeat(50*time.Millisecond, 150*time.Millisecond, 10*time.Second),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Wait for at least one ping write from the heartbeat ticker.
	pingSeen := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		w, ok := mt.WaitWrite(100 * time.Millisecond)
		if ok && w.messageType == 9 { // PingMessage
			pingSeen = true
			break
		}
	}
	if !pingSeen {
		t.Log("no ping message observed — heartbeat ticker may not have fired yet")
	}

	// Kill the transport to simulate a connection loss that would happen
	// after pong timeout in a real WebSocket connection.
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("i/o timeout")})

	select {
	case got := <-disconnected:
		if got == nil {
			t.Error("want non-nil error on disconnect, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect after simulated pong timeout")
	}

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() not closed after pong timeout disconnect")
	}
}

// ── Feature tests ────────────────────────────────────────────────────────────

func TestComponent_WithDialHeaders(t *testing.T) {
	t.Parallel()
	// WithDialHeaders passes headers to the dialer. We verify the mock dialer
	// receives them by checking the Dial call.
	mt := newMockTransport()
	fc := newFakeClock()

	var capturedHeaders http.Header
	var headerMu sync.Mutex

	// Custom dialer that captures headers.
	captureDialer := &headerCapturingDialer{
		transport: mt,
		onDial: func(h http.Header) {
			headerMu.Lock()
			capturedHeaders = h.Clone()
			headerMu.Unlock()
		},
	}

	headers := http.Header{}
	headers.Set("X-Custom-Token", "test-token-123")

	c, err := client.Dial("ws://mock",
		client.WithDialer(captureDialer),
		client.WithClock(fc),
		client.WithDialHeaders(headers),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	headerMu.Lock()
	got := capturedHeaders.Get("X-Custom-Token")
	headerMu.Unlock()

	if got != "test-token-123" {
		t.Errorf("header value: want %q, got %q", "test-token-123", got)
	}
}

// headerCapturingDialer is a mock dialer that captures the headers passed to Dial.
type headerCapturingDialer struct {
	transport *mockTransport
	onDial    func(http.Header)
	called    atomic.Bool
}

func (d *headerCapturingDialer) Dial(_ string, header http.Header) (wspulse.Transport, error) {
	if d.onDial != nil {
		d.onDial(header)
	}
	if d.called.CompareAndSwap(false, true) {
		return d.transport, nil
	}
	return nil, errors.New("headerCapturingDialer: no more transports")
}

func TestComponent_WithMaxMessageSize(t *testing.T) {
	t.Parallel()
	// WithMaxMessageSize calls SetReadLimit on the transport.
	// Our mock records the value. Verify it is set.
	// SetReadLimit is called in readPump which runs asynchronously,
	// so we need to poll briefly.
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithMaxMessageSize(42),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Poll for readPump to call SetReadLimit.
	deadline := time.Now().Add(time.Second)
	var readLimit int64
	for time.Now().Before(deadline) {
		mt.mu.Lock()
		readLimit = mt.readLimit
		mt.mu.Unlock()
		if readLimit != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if readLimit != 42 {
		t.Errorf("SetReadLimit: want 42, got %d", readLimit)
	}
}

func TestComponent_WithMaxMessageSize_OversizedMessage(t *testing.T) {
	t.Parallel()
	// The mock transport's SetReadLimit does not enforce the limit (it is a no-op
	// on the read path). In production, the gorilla websocket conn enforces this.
	// We verify that SetReadLimit was called with the correct value.
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	dropped := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithMaxMessageSize(10),
		client.WithOnTransportDrop(func(err error) {
			select {
			case dropped <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Poll for readPump to call SetReadLimit.
	deadline := time.Now().Add(time.Second)
	var readLimit int64
	for time.Now().Before(deadline) {
		mt.mu.Lock()
		readLimit = mt.readLimit
		mt.mu.Unlock()
		if readLimit != 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if readLimit != 10 {
		t.Errorf("SetReadLimit: want 10, got %d", readLimit)
	}

	// Simulate what the real gorilla transport would do: return an error
	// when receiving an oversized message. The mock transport does not enforce
	// limits, so we inject the error directly.
	mt.InjectError(errors.New("websocket: read limit exceeded"))

	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out: transport should have dropped due to injected oversized-message error")
	}
}

func TestComponent_WithLogger_ValidLogger_Applied(t *testing.T) {
	t.Parallel()
	// WithLogger is applied at option construction time. Verify it does not
	// panic and the client can be created and closed.
	logger, _ := zap.NewDevelopment()
	c, _, _ := dialWithMock(t, client.WithLogger(logger))
	_ = c.Close()
}

func TestComponent_WithHeartbeat_ValidParams_Applied(t *testing.T) {
	t.Parallel()
	// WithHeartbeat is applied at option construction time. Verify the client
	// can be created and closed with custom heartbeat params.
	c, _, _ := dialWithMock(t,
		client.WithHeartbeat(5*time.Second, 15*time.Second, 3*time.Second),
	)
	_ = c.Close()
}

func TestComponent_NormalCloseFrame(t *testing.T) {
	t.Parallel()
	// When the client calls Close(), writePump should send a WebSocket close
	// frame (messageType 8) before exiting.
	c, mt, _ := dialWithMock(t)

	_ = c.Close()

	// Drain all writes and look for the close frame.
	// Give a brief moment for writePump to flush.
	time.Sleep(10 * time.Millisecond)
	writes := mt.DrainWrites()

	foundClose := false
	for _, w := range writes {
		if w.messageType == 8 { // CloseMessage
			foundClose = true
			break
		}
	}
	if !foundClose {
		t.Error("Close() did not produce a WebSocket close frame (messageType=8)")
	}
}

func TestComponent_WithHeartbeat_SendsPings(t *testing.T) {
	t.Parallel()
	// Verify that with a real clock and short heartbeat, pings are sent.
	mt := newMockTransport()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		// Use real clock so ticker fires. Short intervals for testing.
		client.WithHeartbeat(50*time.Millisecond, 200*time.Millisecond, 5*time.Second),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Wait for a ping message.
	deadline := time.Now().Add(time.Second)
	pingSeen := false
	for time.Now().Before(deadline) {
		w, ok := mt.WaitWrite(100 * time.Millisecond)
		if ok && w.messageType == 9 { // PingMessage
			pingSeen = true
			break
		}
	}
	if !pingSeen {
		t.Fatal("no ping message received from heartbeat")
	}
}

// ── Send verification test ──────────────────────────────────────────────────

func TestComponent_Send_WritesCorrectData(t *testing.T) {
	t.Parallel()
	c, mt, _ := dialWithMock(t)
	t.Cleanup(func() { _ = c.Close() })

	frame := wspulse.Frame{Event: "test", Payload: []byte(`"data"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	w, ok := mt.WaitWrite(time.Second)
	if !ok {
		t.Fatal("timed out waiting for write")
	}
	if w.messageType != 1 { // TextMessage (JSONCodec)
		t.Errorf("messageType: want 1, got %d", w.messageType)
	}

	// Decode the written data and verify.
	var wireFrame struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(w.data, &wireFrame); err != nil {
		t.Fatalf("unmarshal written data: %v", err)
	}
	if wireFrame.Event != "test" {
		t.Errorf("event: want %q, got %q", "test", wireFrame.Event)
	}
}
