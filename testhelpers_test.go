package client_test

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

// ── fakeClock ────────────────────────────────────────────────────────────────

// fakeClock replaces both NewTimer (backoff) and NewTicker (heartbeat) with
// controllable fakes. No real timers fire — tests drive time explicitly.
type fakeClock struct {
	mu          sync.Mutex
	timers      []*fakeTimerEntry
	tickers     []*fakeTickerEntry
	timerAdded  chan struct{}
	tickerAdded chan struct{}
}

type fakeTimerEntry struct {
	d     time.Duration
	timer *time.Timer
}

type fakeTickerEntry struct {
	d      time.Duration
	ticker *time.Ticker
}

func newFakeClock() *fakeClock {
	return &fakeClock{timerAdded: make(chan struct{}, 16), tickerAdded: make(chan struct{}, 16)}
}

// NewTimer returns a stopped timer that will not fire on its own.
func (fc *fakeClock) NewTimer(d time.Duration) *time.Timer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	fc.mu.Lock()
	fc.timers = append(fc.timers, &fakeTimerEntry{d: d, timer: t})
	select {
	case fc.timerAdded <- struct{}{}:
	default:
	}
	fc.mu.Unlock()
	return t
}

// NewTicker returns a stopped ticker that will never fire on its own.
func (fc *fakeClock) NewTicker(d time.Duration) *time.Ticker {
	t := time.NewTicker(time.Hour)
	t.Stop()
	fc.mu.Lock()
	fc.tickers = append(fc.tickers, &fakeTickerEntry{d: d, ticker: t})
	select {
	case fc.tickerAdded <- struct{}{}:
	default:
	}
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

// fireTicker fires the i-th registered ticker by resetting it to 1ns.
// The consumer (writePump) reads from ticker.C and acts on the tick.
// Call stopTicker(i) after verifying the side effect to prevent repeated ticks.
func (fc *fakeClock) fireTicker(i int) {
	fc.mu.Lock()
	t := fc.tickers[i].ticker
	fc.mu.Unlock()
	t.Reset(time.Nanosecond)
}

// stopTicker stops the i-th registered ticker.
func (fc *fakeClock) stopTicker(i int) {
	fc.mu.Lock()
	t := fc.tickers[i].ticker
	fc.mu.Unlock()
	t.Stop()
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
			select {
			case <-fc.timerAdded:
			case <-stopCh:
				return
			}
			fc.mu.Lock()
			fc.timers[len(fc.timers)-1].timer.Reset(0)
			fc.mu.Unlock()
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
	require.NoError(t, err, "Dial failed")
	return c, mt, fc
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

// requireReceive waits for a value on ch, failing the test if ch is not ready
// within a generous safety timeout. The timeout exists only to prevent infinite
// hangs in broken code — it should never fire in a passing test.
func requireReceive[T any](t *testing.T, ch <-chan T, msgAndArgs ...any) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for channel receive", msgAndArgs...)
		return *new(T) // unreachable
	}
}

// requireDone waits for the client's Done channel to close.
func requireDone(t *testing.T, c client.Client) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for Done()")
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
