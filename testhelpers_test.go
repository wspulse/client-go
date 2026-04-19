package client_test

import (
	"context"
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

// fakeClock replaces NewTimer (backoff) with a controllable fake.
// No real timers fire — tests drive time explicitly.
type fakeClock struct {
	mu         sync.Mutex
	timers     []*fakeTimerEntry
	timerAdded chan struct{}
}

type fakeTimerEntry struct {
	d     time.Duration
	timer *time.Timer
}

func newFakeClock() *fakeClock {
	return &fakeClock{timerAdded: make(chan struct{}, 16)}
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

// TimerCount returns the number of registered timers.
func (fc *fakeClock) TimerCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.timers)
}

// ── helpers ──────────────────────────────────────────────────────────────────

// echoLoop reads from mt.writeCh and injects the written data back as a
// text message, simulating an echo server. Stops when done is closed.
func echoLoop(mt *mockTransport, done chan struct{}) {
	for {
		select {
		case w := <-mt.writeCh:
			// Only echo text messages, skip others.
			if w.messageType == wspulse.TextMessage {
				mt.InjectMessage(wspulse.TextMessage, w.data)
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

func (d *headerCapturingDialer) Dial(_ context.Context, _ string, header http.Header) (client.Transport, error) {
	if d.onDial != nil {
		d.onDial(header)
	}
	if d.called.CompareAndSwap(false, true) {
		return d.transport, nil
	}
	return nil, errors.New("headerCapturingDialer: no more transports")
}

// requireReceive waits for a value on ch. The test binary's -timeout flag
// is the only hang guard — no real-time deadlines here.
func requireReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	return <-ch
}

// requireDone waits for the client's Done channel to close.
func requireDone(t *testing.T, c client.Client) {
	t.Helper()
	<-c.Done()
}

// failEncodeCodecComponent is a test codec whose Encode always returns an error.
type failEncodeCodecComponent struct{}

func (failEncodeCodecComponent) Encode(wspulse.Message) ([]byte, error) {
	return nil, errors.New("wspulse: encode fail")
}

func (failEncodeCodecComponent) Decode(data []byte) (wspulse.Message, error) {
	return wspulse.Message{}, nil
}

func (failEncodeCodecComponent) WireType() wspulse.MessageType { return wspulse.TextMessage }
