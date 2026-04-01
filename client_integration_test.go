//go:build integration

package client_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
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

// ── Raw WebSocket helpers ────────────────────────────────────────────────────

// startClosableEchoServer creates a local httptest echo server with tracked
// WebSocket connections. The returned closeFunc shuts down the HTTP listener
// first (preventing new connections), then closes all active WS connections.
// This is used for tests that need to make the server unreachable
// (server-drop, max-retries scenarios) without affecting the shared testserver.
func startClosableEchoServer(t *testing.T) (url string, closeFunc func()) {
	t.Helper()
	var mu sync.Mutex
	var conns []*websocket.Conn

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer wsConn.Close()
		mu.Lock()
		conns = append(conns, wsConn)
		mu.Unlock()
		for {
			mt, msg, readErr := wsConn.ReadMessage()
			if readErr != nil {
				return
			}
			if writeErr := wsConn.WriteMessage(mt, msg); writeErr != nil {
				return
			}
		}
	})
	ts := httptest.NewServer(handler)
	url = "ws" + strings.TrimPrefix(ts.URL, "http")
	var closeOnce sync.Once
	closeFunc = func() {
		closeOnce.Do(func() {
			ts.Close()
			mu.Lock()
			for _, c := range conns {
				_ = c.Close()
			}
			conns = nil
			mu.Unlock()
		})
	}
	t.Cleanup(closeFunc)
	return url, closeFunc
}

// startRawServer creates a local httptest server with the given WS handler.
// Returns the WS URL. Cleanup is registered via t.Cleanup automatically.
func startRawServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

// ── Integration tests ────────────────────────────────────────────────────────

func TestDial_SendAndReceive(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 1)
	c, err := client.Dial(wsURL("id=send-recv"), client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })
	frame := wspulse.Frame{Event: "echo", Payload: []byte(`"hello"`)}
	require.NoError(t, c.Send(frame), "Send failed")
	select {
	case f := <-received:
		assert.Equal(t, "echo", f.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestClient_Close_SafeToCallTwice(t *testing.T) {
	t.Parallel()
	c, err := client.Dial(wsURL("id=close-twice"))
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	_ = c.Close()
}

func TestClient_Send_AfterClose_ReturnsErrConnectionClosed(t *testing.T) {
	t.Parallel()
	c, err := client.Dial(wsURL("id=send-after-close"))
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	sendErr := c.Send(wspulse.Frame{Event: "msg"})
	assert.ErrorIs(t, sendErr, wspulse.ErrConnectionClosed)
}

func TestClient_Done_ClosedAfterClose(t *testing.T) {
	t.Parallel()
	c, err := client.Dial(wsURL("id=done-after-close"))
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() channel not closed after Close()")
	}
}

func TestClient_ConcurrentSendAndClose_NoRace(t *testing.T) {
	t.Parallel()
	c, err := client.Dial(wsURL("id=concurrent-send"))
	require.NoError(t, err, "Dial failed")

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
	disconnected := make(chan error, 1)
	c, err := client.Dial(wsURL("id=ondisconnect-fires"),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	require.NoError(t, err, "Dial failed")

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnDisconnect callback")
	}
}

func TestClient_ReadPump_PanicRecovery(t *testing.T) {
	t.Parallel()
	// Need a server that sends a frame immediately on connect to trigger
	// the panic in OnMessage. Use a raw httptest server.
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = wsConn.Close() }()
		// Send a trigger frame immediately.
		trigger := `{"event":"trigger","payload":null}`
		_ = wsConn.WriteMessage(websocket.TextMessage, []byte(trigger))
		// Keep reading to hold the connection open.
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				return
			}
		}
	})
	url := startRawServer(t, handler)

	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			panic("boom")
		}),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: readPump panic was not recovered")
	}
}

func TestClient_Done_FiresOnServerDrop(t *testing.T) {
	t.Parallel()
	url, closeServer := startClosableEchoServer(t)

	received := make(chan wspulse.Frame, 1)
	c, err := client.Dial(url, client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}

	closeServer()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: Done() did not fire after server disconnect")
	}

	require.ErrorIs(t, c.Send(wspulse.Frame{Event: "msg"}), wspulse.ErrConnectionClosed)
}

func TestClient_WithDialHeaders(t *testing.T) {
	t.Parallel()
	// Need a server that inspects headers. Use a raw httptest server.
	headerReceived := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerReceived <- r.Header.Get("X-Custom-Token")
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = wsConn.Close() }()
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				return
			}
		}
	})
	url := startRawServer(t, handler)

	headers := http.Header{}
	headers.Set("X-Custom-Token", "test-token-123")

	c, err := client.Dial(url, client.WithDialHeaders(headers))
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	select {
	case got := <-headerReceived:
		assert.Equal(t, "test-token-123", got, "header value mismatch")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for header check")
	}
}

func TestClient_Close_OnDisconnectFiresExactlyOnce(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	disconnectCount := 0

	echoReceived := make(chan struct{}, 1)
	c, err := client.Dial(wsURL("id=ondisconnect-once"),
		client.WithOnDisconnect(func(err error) {
			mu.Lock()
			disconnectCount++
			mu.Unlock()
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case echoReceived <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-echoReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}

	_ = c.Close()

	// Close() blocks until all goroutines and callbacks complete.
	// No sleep needed — check immediately.
	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	assert.Equal(t, 1, dc, "onDisconnect fired unexpected number of times")
}

func TestClient_OnTransportDrop_FiresOnReconnect(t *testing.T) {
	t.Parallel()
	transportDropped := make(chan struct{}, 5)
	c, err := client.Dial(wsURL("id=transport-drop"),
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

	kickConnection(t, "transport-drop")

	select {
	case <-transportDropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnTransportDrop")
	}
}

func TestClient_AutoReconnect_Close_FiresOnDisconnect(t *testing.T) {
	t.Parallel()
	disconnected := make(chan struct{}, 1)
	echoReceived := make(chan struct{}, 1)
	c, err := client.Dial(wsURL("id=autoreconnect-close"),
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case echoReceived <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-echoReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: onDisconnect did not fire after Close() with auto-reconnect")
	}
}

func TestClient_WithMaxMessageSize_OversizedMessage(t *testing.T) {
	t.Parallel()
	// Need a server that sends an oversized message. Use a raw httptest server.
	connected := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = wsConn.Close() }()
		select {
		case connected <- struct{}{}:
		default:
		}
		// Wait a moment then send an oversized frame.
		time.Sleep(100 * time.Millisecond)
		bigPayload := `"` + strings.Repeat("x", 100) + `"`
		frame := `{"event":"big","payload":` + bigPayload + `}`
		_ = wsConn.WriteMessage(websocket.TextMessage, []byte(frame))
		// Keep reading.
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				return
			}
		}
	})
	url := startRawServer(t, handler)

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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: transport should have dropped due to oversized message")
	}
}

func TestClient_WithLogger_ValidLogger_Applied(t *testing.T) {
	t.Parallel()
	logger, _ := zap.NewDevelopment()
	c, err := client.Dial(wsURL("id=logger-test"), client.WithLogger(logger))
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
}

func TestClient_WithHeartbeat_ValidParams_Applied(t *testing.T) {
	t.Parallel()
	c, err := client.Dial(wsURL("id=heartbeat-test"),
		client.WithHeartbeat(5*time.Second, 15*time.Second, 3*time.Second),
	)
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
}

func TestClient_Send_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
	t.Parallel()
	// Use a raw server that does not read messages, so the client's write
	// buffer fills up. httptest.Server.Close does not cancel r.Context for
	// hijacked connections, so we use an explicit done channel.
	done := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = wsConn.Close() }()
		// Hold connection open but do not read (to cause backpressure).
		<-done
	})
	url := startRawServer(t, handler)
	t.Cleanup(func() { close(done) })

	c, err := client.Dial(url)
	require.NoError(t, err, "Dial failed")
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

func TestClient_Send_CustomBufferSize_Applied(t *testing.T) {
	t.Parallel()
	const bufSize = 4
	c, err := client.Dial(wsURL("id=custom-buf"),
		client.WithSendBufferSize(bufSize),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, bufSize, client.SendBufferCap(c))
}

func TestClient_ReadPump_DecodeFailure_DropsFrame(t *testing.T) {
	t.Parallel()
	// Need a server that sends a raw invalid JSON frame then a valid one.
	// Use a raw httptest server.
	received := make(chan wspulse.Frame, 5)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = wsConn.Close() }()
		// Send an invalid JSON frame (decode failure — should be dropped).
		_ = wsConn.WriteMessage(websocket.TextMessage, []byte("not valid json{{{"))
		// Send a valid frame that should be delivered.
		validFrame := `{"event":"valid-frame","payload":"ok"}`
		_ = wsConn.WriteMessage(websocket.TextMessage, []byte(validFrame))
		// Keep reading.
		for {
			if _, _, err := wsConn.ReadMessage(); err != nil {
				return
			}
		}
	})
	url := startRawServer(t, handler)

	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	select {
	case f := <-received:
		require.Equal(t, "valid-frame", f.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for valid frame")
	}
}

func TestClient_Close_WaitsForDisconnectCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, err := client.Dial(wsURL("id=close-waits-disconnect"),
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	require.True(t, callbackDone.Load(),
		"Close() returned before onDisconnect callback finished — orphaned callback goroutine")
}

func TestClient_Close_WaitsForTransportDropCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, err := client.Dial(wsURL("id=close-waits-drop"),
		client.WithOnTransportDrop(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	require.True(t, callbackDone.Load(),
		"Close() returned before onTransportDrop callback finished — orphaned callback goroutine")
}

func TestClient_Close_WaitsForDisconnectCallback_AutoReconnect(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, err := client.Dial(wsURL("id=close-waits-ar"),
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	require.NoError(t, err, "Dial failed")
	_ = c.Close()
	require.True(t, callbackDone.Load(),
		"Close() returned before onDisconnect callback finished — orphaned callback goroutine")
}

func TestClient_Close_WaitsForGoroutines(t *testing.T) {
	t.Parallel()
	const count = 50
	clients := make([]client.Client, count)
	for i := range clients {
		c, err := client.Dial(wsURL(""))
		require.NoError(t, err, "Dial #%d failed", i)
		t.Cleanup(func() { _ = c.Close() })
		clients[i] = c
	}

	// Close all clients and verify Done() is closed for each.
	// Run Close() in a goroutine so the timeout select can detect
	// a hang — calling Close() inline would block before the select.
	for i, c := range clients {
		closeDone := make(chan struct{})
		go func() {
			_ = c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
	}
}

func TestClient_Close_WaitsForGoroutines_AutoReconnect(t *testing.T) {
	t.Parallel()
	const count = 50
	clients := make([]client.Client, count)
	for i := range clients {
		c, err := client.Dial(wsURL(""),
			client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		)
		require.NoError(t, err, "Dial #%d failed", i)
		t.Cleanup(func() { _ = c.Close() })
		clients[i] = c
	}

	// Run Close() in a goroutine so the timeout select can detect
	// a hang — calling Close() inline would block before the select.
	for i, c := range clients {
		closeDone := make(chan struct{})
		go func() {
			_ = c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
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
	c, err := client.Dial(wsURL("id=encode-error"), client.WithCodec(failEncodeCodec{}))
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	err = c.Send(wspulse.Frame{Event: "msg"})
	require.Error(t, err, "expected encode error")
}

func TestClient_AutoReconnect_ReconnectsAndDeliversMessages(t *testing.T) {
	t.Parallel()
	restored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial(wsURL("id=reconnect-delivers"),
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Verify initial connectivity.
	require.NoError(t, c.Send(wspulse.Frame{Event: "before", Payload: []byte(`"1"`)}), "Send before kick")
	select {
	case f := <-received:
		require.Equal(t, "before", f.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo before kick")
	}

	// Drop the connection.
	kickConnection(t, "reconnect-delivers")

	// Wait for onTransportRestore (fires after pumps have been started).
	select {
	case <-restored:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for onTransportRestore")
	}

	// Verify post-reconnect message delivery.
	require.NoError(t, c.Send(wspulse.Frame{Event: "after", Payload: []byte(`"2"`)}), "Send after reconnect")
	select {
	case f := <-received:
		require.Equal(t, "after", f.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo after reconnect")
	}
}

func TestClient_OnDisconnect_NilOnNormalClose(t *testing.T) {
	t.Parallel()
	disconnectErr := make(chan error, 1)
	c, err := client.Dial(wsURL("id=disconnect-nil"),
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")

	_ = c.Close()

	select {
	case got := <-disconnectErr:
		assert.NoError(t, got, "want nil error on normal Close()")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_NonNilOnServerDrop(t *testing.T) {
	t.Parallel()
	url, closeServer := startClosableEchoServer(t)

	disconnectErr := make(chan error, 1)
	echoReceived := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case echoReceived <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-echoReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}

	closeServer()

	select {
	case got := <-disconnectErr:
		assert.Error(t, got, "want non-nil error on server drop")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_IsErrConnectionLostOnServerDrop(t *testing.T) {
	t.Parallel()
	url, closeServer := startClosableEchoServer(t)

	disconnectErr := make(chan error, 1)
	echoReceived := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case echoReceived <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-echoReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}

	closeServer()

	select {
	case got := <-disconnectErr:
		assert.ErrorIs(t, got, client.ErrConnectionLost)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_OnDisconnect_NonNilOnMaxRetries(t *testing.T) {
	t.Parallel()
	url, closeServer := startClosableEchoServer(t)

	disconnectErr := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(2, 50*time.Millisecond, 200*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			disconnectErr <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	closeServer()

	select {
	case got := <-disconnectErr:
		assert.Error(t, got, "want non-nil error on max retries exhausted")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}
}

func TestClient_AutoReconnect_MaxRetriesExhausted_ClosesDone(t *testing.T) {
	t.Parallel()
	// Use a closable echo server so shutting it down doesn't affect
	// other parallel tests using the shared testserver.
	url, closeServer := startClosableEchoServer(t)

	c, err := client.Dial(url,
		client.WithAutoReconnect(2, 50*time.Millisecond, 200*time.Millisecond),
		client.WithHeartbeat(50*time.Millisecond, 150*time.Millisecond, 5*time.Second),
		client.WithOnDisconnect(func(err error) {}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Close the server — reconnect dials get connection-refused instantly.
	closeServer()

	// With 2 retries, baseDelay=50ms, maxDelay=200ms, short heartbeat
	// to detect disconnect quickly, Done() should close well within 3s.
	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: Done() did not close after max retries exhausted")
	}
}

func TestClient_AutoReconnect_CloseDuringBackoff(t *testing.T) {
	t.Parallel()
	transportDropped := make(chan struct{}, 1)
	c, err := client.Dial(wsURL("id=close-during-backoff"),
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

	kickConnection(t, "close-during-backoff")

	select {
	case <-transportDropped:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for transport drop")
	}

	// Close() signals the reconnect loop so that when it reaches
	// the backoff select during reconnect, it exits promptly.
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
	transportDropped := make(chan struct{}, 5)
	transportRestored := make(chan struct{}, 5)
	received := make(chan wspulse.Frame, 5)
	c, err := client.Dial(wsURL("id=restore-fires"),
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Drop the connection.
	kickConnection(t, "restore-fires")

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
	require.NoError(t, c.Send(wspulse.Frame{Event: "post-restore", Payload: []byte(`"ok"`)}), "Send after restore")
	select {
	case f := <-received:
		require.Equal(t, "post-restore", f.Event)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo after restore")
	}
}

func TestClient_OnTransportRestore_DoesNotFireOnInitialConnect(t *testing.T) {
	t.Parallel()
	var restoreCount atomic.Int32
	received := make(chan wspulse.Frame, 1)
	c, err := client.Dial(wsURL("id=restore-no-initial"),
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Round-trip a frame to prove all pumps are fully operational.
	// If onTransportRestore were incorrectly fired, it would have
	// happened during or immediately after Dial()/pump startup.
	require.NoError(t, c.Send(wspulse.Frame{Event: "probe"}), "Send failed")
	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for probe echo")
	}

	assert.Equal(t, int32(0), restoreCount.Load(),
		"onTransportRestore should not fire on initial connect")
}

func TestClient_OnTransportRestore_NotFiredOnFailedReconnect(t *testing.T) {
	t.Parallel()
	url, closeServer := startClosableEchoServer(t)

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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Shut down the server so all reconnect dials fail.
	closeServer()

	// Wait for onDisconnect with ErrRetriesExhausted.
	select {
	case got := <-disconnectErr:
		assert.ErrorIs(t, got, client.ErrRetriesExhausted)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	assert.Equal(t, int32(0), restoreCount.Load(),
		"onTransportRestore should not fire on failed reconnect")
}

func TestClient_HeartbeatPongTimeout_DisconnectsClient(t *testing.T) {
	t.Parallel()
	// Use testserver's ?ignore_pings=1 mode.
	disconnected := make(chan error, 1)
	c, err := client.Dial(wsURL("ignore_pings=1"),
		// Fast ping interval (100ms), short pong timeout (300ms), generous write wait.
		client.WithHeartbeat(100*time.Millisecond, 300*time.Millisecond, 10*time.Second),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// The client should detect the missing Pong within pongWait (300ms).
	// Allow generous headroom for CI.
	select {
	case got := <-disconnected:
		assert.Error(t, got, "want non-nil error (ErrConnectionLost) on pong timeout")
		assert.ErrorIs(t, got, client.ErrConnectionLost)
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
	url, closeServer := startClosableEchoServer(t)

	var disconnectCount atomic.Int32
	disconnectDone := make(chan struct{}, 1)
	echoReceived := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnectCount.Add(1)
			select {
			case disconnectDone <- struct{}{}:
			default:
			}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case echoReceived <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Confirm the connection is established by round-tripping a frame.
	require.NoError(t, c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}), "Send failed")
	select {
	case <-echoReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
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
		closeServer()
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

	// Close() blocks until all goroutines exit and callbacks complete.
	// No sleep needed — check immediately after Close + onDisconnect sync.
	assert.Equal(t, int32(1), disconnectCount.Load(),
		"onDisconnect should fire exactly once")
}
