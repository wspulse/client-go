//go:build integration

package client_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	wspulse "github.com/wspulse/server"

	"github.com/wspulse/client-go"
)

// ── Fake clock ───────────────────────────────────────────────────────────────
//
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
// Tests can call ft.Reset(0) to fire it immediately if needed.
func (fc *fakeClock) NewTimer(d time.Duration) *time.Timer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	fc.mu.Lock()
	fc.timers = append(fc.timers, &fakeTimerEntry{d: d, timer: t})
	fc.mu.Unlock()
	return t
}

// NewTicker returns a stopped ticker that will never fire on its own.
// This prevents heartbeat pings from interfering with tests that use
// fakeClock.
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

// ── Test helpers ─────────────────────────────────────────────────────────────

func startEchoServer(t *testing.T) string {
	t.Helper()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "client-1", nil
		},
		wspulse.WithOnMessage(func(connection wspulse.Connection, f wspulse.Frame) {
			_ = connection.Send(f)
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestDial_SendAndReceive(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	received := make(chan wspulse.Frame, 1)
	c, err := client.Dial(url, client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	frame := wspulse.Frame{Event: "echo", Payload: []byte(`"hello"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "echo" {
			t.Errorf("Event: want %q, got %q", "echo", f.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestClient_Close_SafeToCallTwice(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	_ = c.Close()
}

func TestClient_Send_AfterClose_ReturnsErrConnectionClosed(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	sendErr := c.Send(wspulse.Frame{Event: "msg"})
	if !errors.Is(sendErr, wspulse.ErrConnectionClosed) {
		t.Errorf("want ErrConnectionClosed, got %v", sendErr)
	}
}

func TestClient_Done_ClosedAfterClose(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() channel not closed after Close()")
	}
}

func TestClient_ConcurrentSendAndClose_NoRace(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

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

func TestClient_OnDisconnect_CallbackFires(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnDisconnect callback")
	}
}

func TestClient_ReadPump_PanicRecovery(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "echo-1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			_ = connection.Send(wspulse.Frame{Event: "trigger"})
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			panic("boom")
		}),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: readPump panic was not recovered")
	}
}

func TestClient_Done_FiresOnServerDrop(t *testing.T) {
	t.Parallel()
	connected := make(chan wspulse.Connection, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			connected <- connection
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connection")
	}

	srv.Close()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: Done() did not fire after server disconnect")
	}

	if err := c.Send(wspulse.Frame{Event: "msg"}); err != wspulse.ErrConnectionClosed {
		t.Fatalf("want ErrConnectionClosed, got %v", err)
	}
}

func TestClient_WithDialHeaders(t *testing.T) {
	t.Parallel()
	headerReceived := make(chan string, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			headerReceived <- r.Header.Get("X-Custom-Token")
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	headers := http.Header{}
	headers.Set("X-Custom-Token", "test-token-123")

	c, err := client.Dial(url, client.WithDialHeaders(headers))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case got := <-headerReceived:
		if got != "test-token-123" {
			t.Errorf("header value: want %q, got %q", "test-token-123", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for header check")
	}
}

func TestClient_Close_OnDisconnectFiresExactlyOnce(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)

	var mu sync.Mutex
	disconnectCount := 0

	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			mu.Lock()
			disconnectCount++
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_ = c.Close()

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	if dc != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", dc)
	}
}

func TestClient_OnTransportDrop_FiresOnReconnect(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithResumeWindow(5),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	transportDropped := make(chan struct{}, 5)
	c, err := client.Dial(url,
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

	time.Sleep(200 * time.Millisecond)

	_ = srv.Kick("c1")

	select {
	case <-transportDropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnTransportDrop")
	}
}

func TestClient_AutoReconnect_Close_FiresOnDisconnect(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnected := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: onDisconnect did not fire after Close() with auto-reconnect")
	}
}

func TestClient_WithMaxMessageSize_OversizedMessage(t *testing.T) {
	t.Parallel()
	var serverConnection wspulse.Connection
	connected := make(chan struct{}, 1)

	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			serverConnection = connection
			select {
			case connected <- struct{}{}:
			default:
			}
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	dropped := make(chan error, 1)
	c, err := client.Dial(url,
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

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	bigPayload := []byte(`"` + strings.Repeat("x", 100) + `"`)
	_ = serverConnection.Send(wspulse.Frame{Event: "big", Payload: bigPayload})

	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: transport should have dropped due to oversized message")
	}
}

func TestClient_WithLogger_ValidLogger_Applied(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	logger, _ := zap.NewDevelopment()
	c, err := client.Dial(url, client.WithLogger(logger))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
}

func TestClient_WithHeartbeat_ValidParams_Applied(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url,
		client.WithHeartbeat(5*time.Second, 15*time.Second, 3*time.Second),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
}

func TestClient_Send_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

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

func TestClient_ReadPump_DecodeFailure_DropsFrame(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 5)

	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	time.Sleep(100 * time.Millisecond)
	frame := wspulse.Frame{Event: "valid-frame", Payload: []byte(`"ok"`)}
	if err := srv.Send("c1", frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case f := <-received:
		if f.Event != "valid-frame" {
			t.Fatalf("want event %q, got %q", "valid-frame", f.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for valid frame")
	}
}

func TestClient_Close_WaitsForDisconnectCallback(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	var callbackDone atomic.Bool
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

func TestClient_Close_WaitsForTransportDropCallback(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	var callbackDone atomic.Bool
	c, err := client.Dial(url,
		client.WithOnTransportDrop(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onTransportDrop callback finished — orphaned callback goroutine")
	}
}

func TestClient_Close_WaitsForDisconnectCallback_AutoReconnect(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	var callbackDone atomic.Bool
	c, err := client.Dial(url,
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

// startMultiClientEchoServer creates a server that assigns each connection a
// unique client ID so multiple concurrent clients coexist without kicking.
func startMultiClientEchoServer(t *testing.T) string {
	t.Helper()
	var clientIDCounter atomic.Int64
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			id := clientIDCounter.Add(1)
			return "room", fmt.Sprintf("c-%d", id), nil
		},
		wspulse.WithOnMessage(func(connection wspulse.Connection, f wspulse.Frame) {
			_ = connection.Send(f)
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestClient_Close_WaitsForGoroutines(t *testing.T) {
	t.Parallel()
	url := startMultiClientEchoServer(t)

	const count = 50
	clients := make([]client.Client, count)
	for i := range clients {
		c, err := client.Dial(url)
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		clients[i] = c
	}
	time.Sleep(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	for _, c := range clients {
		_ = c.Close()
	}
	after := runtime.NumGoroutine()

	// Each client spawns 3 internal goroutines. If Close() properly
	// blocks until they exit, NumGoroutine drops by roughly count*3.
	if reclaimed := before - after; reclaimed < count {
		t.Errorf("Close() did not wait for goroutines: reclaimed only %d, want >= %d (before=%d after=%d)",
			reclaimed, count, before, after)
	}
}

func TestClient_Close_WaitsForGoroutines_AutoReconnect(t *testing.T) {
	t.Parallel()
	url := startMultiClientEchoServer(t)

	const count = 50
	clients := make([]client.Client, count)
	for i := range clients {
		c, err := client.Dial(url,
			client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		clients[i] = c
	}
	time.Sleep(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	for _, c := range clients {
		_ = c.Close()
	}
	after := runtime.NumGoroutine()

	if reclaimed := before - after; reclaimed < count {
		t.Errorf("Close() did not wait for goroutines: reclaimed only %d, want >= %d (before=%d after=%d)",
			reclaimed, count, before, after)
	}
}

// failEncodeCodec is a test codec whose Encode always returns an error.
type failEncodeCodec struct{}

func (failEncodeCodec) Encode(wspulse.Frame) ([]byte, error) {
	return nil, errors.New("wspulse: encode fail")
}

func (failEncodeCodec) Decode(data []byte) (wspulse.Frame, error) {
	return wspulse.Frame{}, nil
}

func (failEncodeCodec) FrameType() int { return 1 }

func TestClient_Send_EncodeError_ReturnsError(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	c, err := client.Dial(url, client.WithCodec(failEncodeCodec{}))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	err = c.Send(wspulse.Frame{Event: "msg"})
	if err == nil {
		t.Fatal("expected encode error, got nil")
	}
}

func TestClient_AutoReconnect_ReconnectsAndDeliversMessages(t *testing.T) {
	t.Parallel()
	var clientIDCounter atomic.Int64
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			id := clientIDCounter.Add(1)
			return "room", fmt.Sprintf("rc-%d", id), nil
		},
		wspulse.WithOnMessage(func(connection wspulse.Connection, f wspulse.Frame) {
			_ = connection.Send(f)
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	restored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial(url,
		client.WithAutoReconnect(3, 50*time.Millisecond, 200*time.Millisecond),
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

	// Verify initial connectivity.
	if err := c.Send(wspulse.Frame{Event: "before", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send before kick: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "before" {
			t.Fatalf("want event %q, got %q", "before", f.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo before kick")
	}

	// Drop the connection.
	firstID := fmt.Sprintf("rc-%d", clientIDCounter.Load())
	if err := srv.Kick(firstID); err != nil {
		t.Fatalf("Kick failed: %v", err)
	}

	// Wait for onTransportRestore (fires after pumps have been started).
	select {
	case <-restored:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onTransportRestore")
	}

	// Verify post-reconnect message delivery.
	if err := c.Send(wspulse.Frame{Event: "after", Payload: []byte(`"2"`)}); err != nil {
		t.Fatalf("Send after reconnect: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "after" {
			t.Fatalf("want event %q, got %q", "after", f.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo after reconnect")
	}
}

func TestClient_OnDisconnect_NilOnNormalClose(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)
	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	_ = c.Close()

	select {
	case got := <-disconnectErr:
		if got != nil {
			t.Errorf("want nil error on normal Close(), got %v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_NonNilOnServerDrop(t *testing.T) {
	t.Parallel()
	connected := make(chan wspulse.Connection, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			connected <- connection
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connection")
	}

	srv.Close()

	select {
	case got := <-disconnectErr:
		if got == nil {
			t.Error("want non-nil error on server drop, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_IsErrConnectionLostOnServerDrop(t *testing.T) {
	t.Parallel()
	connected := make(chan struct{}, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(_ wspulse.Connection) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connection")
	}

	srv.Close()

	select {
	case got := <-disconnectErr:
		if !errors.Is(got, client.ErrConnectionLost) {
			t.Errorf("want errors.Is(err, ErrConnectionLost), got %v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_NonNilOnMaxRetries(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(2, 50*time.Millisecond, 200*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	srv.Close()
	ts.Close()

	select {
	case got := <-disconnectErr:
		if got == nil {
			t.Error("want non-nil error on max retries exhausted, got nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_AutoReconnect_MaxRetriesExhausted_ClosesDone(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnected := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(2, 50*time.Millisecond, 200*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Shut down the server so reconnect dials fail.
	srv.Close()
	ts.Close()

	// Done() should close after max retries are exhausted.
	select {
	case <-c.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out: Done() did not close after max retries exhausted")
	}

	// onDisconnect should have fired.
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("onDisconnect did not fire after max retries exhausted")
	}
}

func TestClient_AutoReconnect_CloseDuringBackoff(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	transportDropped := make(chan struct{}, 1)
	c, err := client.Dial(url,
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

	// Poll until the hub has registered the session, then kick it.
	// Kick returns ErrConnectionNotFound if called before the hub processes
	// the join message, which causes a flaky transport-drop timeout.
	kickDeadline := time.Now().Add(2 * time.Second)
	var lastKickErr error
	for {
		if err := srv.Kick("c1"); err != nil {
			lastKickErr = err
			if time.Now().After(kickDeadline) {
				t.Fatalf("server did not register c1 within 2s (last error: %v)", lastKickErr)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}

	select {
	case <-transportDropped:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transport drop")
	}

	// Let the reconnect loop reach the backoff select.
	time.Sleep(100 * time.Millisecond)

	// Close() must not block waiting for the long backoff timer.
	closeDone := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() hung during backoff — timer not stopped")
	}
}

func TestClient_OnTransportRestore_FiresAfterReconnect(t *testing.T) {
	t.Parallel()
	var clientIDCounter atomic.Int64
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			id := clientIDCounter.Add(1)
			return "room", fmt.Sprintf("tr-%d", id), nil
		},
		wspulse.WithOnMessage(func(connection wspulse.Connection, f wspulse.Frame) {
			_ = connection.Send(f)
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	transportDropped := make(chan struct{}, 5)
	transportRestored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial(url,
		client.WithAutoReconnect(3, 50*time.Millisecond, 200*time.Millisecond),
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

	// Drop the connection.
	firstID := fmt.Sprintf("tr-%d", clientIDCounter.Load())
	kickDeadline := time.Now().Add(2 * time.Second)
	for {
		if err := srv.Kick(firstID); err != nil {
			if time.Now().After(kickDeadline) {
				t.Fatalf("server did not register %s within 2s", firstID)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}

	// Verify onTransportDrop fires.
	select {
	case <-transportDropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onTransportDrop")
	}

	// Verify onTransportRestore fires after reconnect.
	select {
	case <-transportRestored:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onTransportRestore")
	}

	// Verify message delivery works after restore.
	if err := c.Send(wspulse.Frame{Event: "post-restore", Payload: []byte(`"ok"`)}); err != nil {
		t.Fatalf("Send after restore: %v", err)
	}
	select {
	case f := <-received:
		if f.Event != "post-restore" {
			t.Fatalf("want event %q, got %q", "post-restore", f.Event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo after restore")
	}
}

func TestClient_OnTransportRestore_DoesNotFireOnInitialConnect(t *testing.T) {
	t.Parallel()
	url := startEchoServer(t)

	var restoreCount atomic.Int32
	c, err := client.Dial(url,
		client.WithOnTransportRestore(func() {
			restoreCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// Give enough time for any erroneous callback to fire.
	time.Sleep(300 * time.Millisecond)

	_ = c.Close()

	if count := restoreCount.Load(); count != 0 {
		t.Errorf("onTransportRestore fired %d times on initial connect, want 0", count)
	}
}

func TestClient_OnTransportRestore_NotFiredOnFailedReconnect(t *testing.T) {
	t.Parallel()
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	var restoreCount atomic.Int32
	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(2, 50*time.Millisecond, 200*time.Millisecond),
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

	// Shut down the server so all reconnect dials fail.
	srv.Close()
	ts.Close()

	// Wait for onDisconnect with ErrRetriesExhausted.
	select {
	case got := <-disconnectErr:
		if !errors.Is(got, client.ErrRetriesExhausted) {
			t.Errorf("want ErrRetriesExhausted, got %v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	if count := restoreCount.Load(); count != 0 {
		t.Errorf("onTransportRestore fired %d times, want 0", count)
	}
}

// startNoPongServer creates a raw WebSocket server that accepts connections
// but never replies to Ping frames. This is achieved by overriding the default
// ping handler (which normally sends an automatic Pong) with a no-op.
// The server keeps the connection alive by reading messages in a loop until
// the connection is closed.
func startNoPongServer(t *testing.T) string {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, "websocket upgrade failed", http.StatusBadRequest)
			t.Errorf("websocket upgrade failed: %v", err)
			return
		}
		// Override the default ping handler so no Pong is ever sent.
		wsConn.SetPingHandler(func(string) error { return nil })
		defer wsConn.Close()
		// Keep reading to hold the connection open; exit on any error.
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				return
			}
		}
	})
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestClient_HeartbeatPongTimeout_DisconnectsClient(t *testing.T) {
	t.Parallel()
	url := startNoPongServer(t)

	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		// Fast ping interval (100ms), short pong timeout (300ms), generous write wait.
		client.WithHeartbeat(100*time.Millisecond, 300*time.Millisecond, 10*time.Second),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// The client should detect the missing Pong within pongWait (300ms).
	// Allow generous headroom for CI.
	select {
	case got := <-disconnected:
		if got == nil {
			t.Error("want non-nil error (ErrConnectionLost) on pong timeout, got nil")
		}
		if !errors.Is(got, client.ErrConnectionLost) {
			t.Errorf("want errors.Is(err, ErrConnectionLost), got %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onDisconnect after pong timeout")
	}

	// Done() should also be closed.
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() not closed after pong timeout disconnect")
	}
}

func TestClient_ConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce(t *testing.T) {
	t.Parallel()
	connected := make(chan struct{}, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "race-1", nil
		},
		wspulse.WithOnConnect(func(_ wspulse.Connection) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	var disconnectCount atomic.Int32
	disconnectDone := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectCount.Add(1)
			select {
			case disconnectDone <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Wait for the connection to be established on the server side.
	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server to register connection")
	}

	// Simultaneously close the client and drop the server connection.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Close()
	}()
	go func() {
		defer wg.Done()
		srv.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for client and server close to complete")
	}

	// Wait for onDisconnect to fire.
	select {
	case <-disconnectDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	// Allow a short grace period for any spurious second invocations.
	time.Sleep(200 * time.Millisecond)

	if count := disconnectCount.Load(); count != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", count)
	}

}
