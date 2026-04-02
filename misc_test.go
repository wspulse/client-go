package client_test

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestSend_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
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
	require.True(t, sawFull, "ErrSendBufferFull was never returned in 1000 sends")
}

func TestSend_CustomBufferSize_Applied(t *testing.T) {
	t.Parallel()
	const bufSize = 4
	c, _, _ := dialWithMock(t, client.WithSendBufferSize(bufSize))
	t.Cleanup(func() { _ = c.Close() })

	if got := client.SendBufferCap(c); got != bufSize {
		t.Errorf("SendBufferCap = %d, want %d", got, bufSize)
	}
}

func TestReadPump_DecodeFailure_DropsFrame(t *testing.T) {
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

func TestReadPump_PanicRecovery(t *testing.T) {
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

func TestSend_EncodeError_ReturnsError(t *testing.T) {
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

func TestHeartbeatPongTimeout_DisconnectsClient(t *testing.T) {
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

func TestWithDialHeaders(t *testing.T) {
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

func TestWithMaxMessageSize(t *testing.T) {
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

func TestWithMaxMessageSize_OversizedMessage(t *testing.T) {
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

func TestWithLogger_ValidLogger_Applied(t *testing.T) {
	t.Parallel()
	// WithLogger is applied at option construction time. Verify it does not
	// panic and the client can be created and closed.
	logger, _ := zap.NewDevelopment()
	c, _, _ := dialWithMock(t, client.WithLogger(logger))
	_ = c.Close()
}

func TestWithHeartbeat_ValidParams_Applied(t *testing.T) {
	t.Parallel()
	// WithHeartbeat is applied at option construction time. Verify the client
	// can be created and closed with custom heartbeat params.
	c, _, _ := dialWithMock(t,
		client.WithHeartbeat(5*time.Second, 15*time.Second, 3*time.Second),
	)
	_ = c.Close()
}

func TestWithHeartbeat_SendsPings(t *testing.T) {
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
