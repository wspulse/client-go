package client_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestSend_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithSendBufferSize(4),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Block writes at the transport level so writePump stalls.
	// This guarantees c.send (cap 4) fills up deterministically.
	unblock := mt.BlockWrites()
	t.Cleanup(unblock)

	sawFull := false
	for i := 0; i < 100; i++ {
		err := c.Send(wspulse.Frame{Event: "flood", Payload: []byte(`"x"`)})
		if errors.Is(err, wspulse.ErrSendBufferFull) {
			sawFull = true
			break
		}
		if errors.Is(err, wspulse.ErrConnectionClosed) {
			break
		}
	}
	require.True(t, sawFull, "ErrSendBufferFull was never returned")
}

func TestSend_CustomBufferSize_Applied(t *testing.T) {
	t.Parallel()
	const bufSize = 4
	c, _, _ := dialWithMock(t, client.WithSendBufferSize(bufSize))
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, bufSize, client.SendBufferCap(c), "SendBufferCap")
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
	mt.InjectMessage(wspulse.TextMessage, []byte("not valid json{{{"))
	// Inject a valid frame that should be delivered.
	validFrame := `{"event":"valid-frame","payload":"ok"}`
	mt.InjectMessage(wspulse.TextMessage, []byte(validFrame))

	f := requireReceive(t, received)
	assert.Equal(t, "valid-frame", f.Event)
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
	mt.InjectMessage(wspulse.TextMessage, []byte(trigger))

	_ = requireReceive(t, disconnected)
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	err = c.Send(wspulse.Frame{Event: "msg"})
	require.Error(t, err, "expected encode error")
}

func TestPingInterval_SendsPings(t *testing.T) {
	t.Parallel()
	// Verify that with a fake clock, firing the heartbeat ticker causes a ping.
	mt := newMockTransport()
	mt.pingCh = make(chan struct{}, 16)
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithPingInterval(50*time.Millisecond),
		client.WithWriteTimeout(5*time.Second),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for pingPump to create the heartbeat ticker.
	requireReceive(t, fc.tickerAdded)

	// Fire the ticker to trigger a ping.
	fc.fireTicker(0)

	// Verify ping was called.
	requireReceive(t, mt.pingCh)
	fc.stopTicker(0)
}

func TestHeartbeat_ReadError_DisconnectsClient(t *testing.T) {
	t.Parallel()
	// Verifies the observable heartbeat behaviour: the client sends pings
	// via the heartbeat ticker, and when the transport dies the client disconnects.
	mt := newMockTransport()
	mt.pingCh = make(chan struct{}, 16)
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	disconnected := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithPingInterval(50*time.Millisecond),
		client.WithWriteTimeout(10*time.Second),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for pingPump to create the heartbeat ticker.
	requireReceive(t, fc.tickerAdded)

	// Fire the ticker to trigger a ping.
	fc.fireTicker(0)

	// Verify a ping was sent.
	requireReceive(t, mt.pingCh)
	fc.stopTicker(0)

	// Kill the transport to simulate a connection loss.
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("i/o timeout")})

	got := requireReceive(t, disconnected)
	assert.Error(t, got, "want non-nil error on disconnect")

	requireDone(t, c)
}

func TestPongTimeout_DisconnectsClient(t *testing.T) {
	t.Parallel()
	// When the server never responds to a ping, pingPump force-closes the
	// transport. pingFunc returns context.DeadlineExceeded immediately so
	// the test does not depend on any real wall-clock timeout.
	mt := newMockTransport()
	mt.pingFunc = func(ctx context.Context) error {
		return context.DeadlineExceeded
	}
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	transportDropped := make(chan error, 1)
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithPingInterval(50*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			transportDropped <- err
		}),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for pingPump to create the ticker.
	requireReceive(t, fc.tickerAdded)

	// Fire the ticker — pingFunc returns DeadlineExceeded immediately,
	// doPing calls CloseNow(), readPump signals onTransportDrop.
	fc.fireTicker(0)

	got := requireReceive(t, transportDropped)
	assert.Error(t, got, "want non-nil error on pong timeout")
	fc.stopTicker(0)
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	headerMu.Lock()
	got := capturedHeaders.Get("X-Custom-Token")
	headerMu.Unlock()

	assert.Equal(t, "test-token-123", got, "header value")
}

func TestWithMaxMessageSize(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithMaxMessageSize(42),
	)
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for readPump to call SetReadLimit.
	requireReceive(t, mt.readLimitSet)

	mt.mu.Lock()
	readLimit := mt.readLimit
	mt.mu.Unlock()
	assert.Equal(t, int64(42), readLimit, "SetReadLimit")
}

func TestWithMaxMessageSize_OversizedMessage(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err, "Dial failed")
	t.Cleanup(func() { _ = c.Close() })

	// Wait for readPump to call SetReadLimit.
	requireReceive(t, mt.readLimitSet)

	mt.mu.Lock()
	readLimit := mt.readLimit
	mt.mu.Unlock()
	assert.Equal(t, int64(10), readLimit, "SetReadLimit")

	// Simulate what the real transport would do: return an error
	// when receiving an oversized message.
	mt.InjectError(errors.New("websocket: read limit exceeded"))

	_ = requireReceive(t, dropped)
}

func TestWithLogger_ValidLogger_Applied(t *testing.T) {
	t.Parallel()
	// WithLogger is applied at option construction time. Verify it does not
	// panic and the client can be created and closed.
	logger, _ := zap.NewDevelopment()
	c, _, _ := dialWithMock(t, client.WithLogger(logger))
	_ = c.Close()
}

func TestWithPingInterval_ValidParams_Applied(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t,
		client.WithPingInterval(5*time.Second),
		client.WithWriteTimeout(3*time.Second),
	)
	_ = c.Close()
}
