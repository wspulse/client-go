package client

import (
	"net/http"
	"time"

	wspulse "github.com/wspulse/server"
	"go.uber.org/zap"
)

// ClientOption configures a Client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	onMessage       func(wspulse.Frame)
	onReconnect     func(attempt int)
	onDisconnect    func(err error)
	onTransportDrop func(err error)
	codec           wspulse.Codec
	dialHeaders     http.Header
	logger          *zap.Logger
	autoReconnect   bool
	maxRetries      int // ≤ 0 means retry indefinitely
	baseDelay       time.Duration
	maxDelay        time.Duration
	pongWait        time.Duration
	pingPeriod      time.Duration
	writeWait       time.Duration
	maxMessageSize  int64 // max inbound message size in bytes; 0 = use gorilla default
}

func defaultClientConfig() *clientConfig {
	return &clientConfig{
		codec:          wspulse.JSONCodec,
		logger:         zap.NewNop(),
		autoReconnect:  false,
		maxRetries:     10,
		baseDelay:      1 * time.Second,
		maxDelay:       30 * time.Second,
		pongWait:       60 * time.Second,
		pingPeriod:     20 * time.Second,
		writeWait:      10 * time.Second,
		maxMessageSize: 1 << 20, // 1 MiB
	}
}

// WithOnMessage registers a callback invoked for every inbound Frame.
func WithOnMessage(fn func(wspulse.Frame)) ClientOption {
	return func(c *clientConfig) { c.onMessage = fn }
}

// WithOnReconnect registers a callback invoked at each reconnection attempt.
// attempt is zero-based.
func WithOnReconnect(fn func(attempt int)) ClientOption {
	return func(c *clientConfig) { c.onReconnect = fn }
}

// WithOnDisconnect registers a callback invoked when the client permanently
// disconnects. When auto-reconnect is enabled this fires only after all
// retries are exhausted or Close() is called — not on every transport drop.
// When auto-reconnect is disabled it fires once when the connection dies.
// err is nil for a normal closure.
func WithOnDisconnect(fn func(err error)) ClientOption {
	return func(c *clientConfig) { c.onDisconnect = fn }
}

// WithOnTransportDrop registers a callback invoked each time the underlying
// WebSocket transport dies. Unlike OnDisconnect, this fires on every
// transport failure including those followed by automatic reconnection.
func WithOnTransportDrop(fn func(err error)) ClientOption {
	return func(c *clientConfig) { c.onTransportDrop = fn }
}

// WithCodec replaces the default JSONCodec.
// Panics if codec is nil (fail-fast at construction rather than at first Send).
func WithCodec(codec wspulse.Codec) ClientOption {
	if codec == nil {
		panic("wspulse/client: WithCodec: codec must not be nil")
	}
	return func(c *clientConfig) { c.codec = codec }
}

// WithAutoReconnect enables automatic reconnection with exponential backoff.
// maxRetries ≤ 0 retries indefinitely. baseDelay is the initial backoff; it
// doubles each attempt up to maxDelay.
func WithAutoReconnect(maxRetries int, baseDelay, maxDelay time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.autoReconnect = true
		c.maxRetries = maxRetries
		c.baseDelay = baseDelay
		c.maxDelay = maxDelay
	}
}

// WithDialHeaders sets custom HTTP headers sent during the WebSocket handshake.
func WithDialHeaders(h http.Header) ClientOption {
	return func(c *clientConfig) { c.dialHeaders = h }
}

// WithLogger sets a zap logger for the client.
func WithLogger(logger *zap.Logger) ClientOption {
	if logger == nil {
		panic("wspulse/client: WithLogger: logger must not be nil")
	}
	return func(c *clientConfig) { c.logger = logger }
}

// WithHeartbeat configures client-side Ping/Pong heartbeat intervals.
// pingPeriod must be positive and strictly less than pongWait.
func WithHeartbeat(pingPeriod, pongWait, writeWait time.Duration) ClientOption {
	if pingPeriod <= 0 || pongWait <= 0 || writeWait <= 0 || pingPeriod >= pongWait {
		panic("wspulse/client: WithHeartbeat: pingPeriod must be positive and strictly less than pongWait, writeWait must be positive")
	}
	return func(c *clientConfig) {
		c.pingPeriod = pingPeriod
		c.pongWait = pongWait
		c.writeWait = writeWait
	}
}

// WithMaxMessageSize sets the maximum size in bytes for inbound messages.
// n must be at least 1.
func WithMaxMessageSize(n int64) ClientOption {
	if n < 1 {
		panic("wspulse/client: WithMaxMessageSize: n must be at least 1")
	}
	return func(c *clientConfig) { c.maxMessageSize = n }
}
