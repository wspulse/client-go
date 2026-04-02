package client

import (
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	wspulse "github.com/wspulse/core"
)

// Configuration upper bounds — option functions panic if these ceilings are exceeded.
const (
	maxPingPeriod    = 1 * time.Minute  // WithHeartbeat: pingPeriod upper bound
	maxPongWait      = 2 * time.Minute  // WithHeartbeat: pongWait upper bound
	maxWriteWait     = 30 * time.Second // WithHeartbeat: writeWait upper bound
	maxMsgSizeBytes  = 64 << 20         // WithMaxMessageSize upper bound — 64 MiB
	maxBaseDelay     = 1 * time.Minute  // WithAutoReconnect: baseDelay upper bound
	maxDelayLimit    = 5 * time.Minute  // WithAutoReconnect: maxDelay upper bound
	maxRetriesLimit  = 32               // WithAutoReconnect: maxRetries upper bound (0 = unlimited)
	maxSendBufFrames = 4096             // WithSendBufferSize upper bound
)

// ClientOption configures a Client.
type ClientOption func(*clientConfig) //nolint:revive

type clientConfig struct {
	onMessage          func(wspulse.Frame)
	onDisconnect       func(err error)
	onTransportDrop    func(err error)
	onTransportRestore func()
	codec              wspulse.Codec
	dialHeaders        http.Header
	logger             *zap.Logger
	autoReconnect      bool
	maxRetries         int // 0 means retry indefinitely
	baseDelay          time.Duration
	maxDelay           time.Duration
	pongWait           time.Duration
	pingPeriod         time.Duration
	writeWait          time.Duration
	maxMessageSize     int64 // max inbound message size in bytes; 0 = no size enforcement
	sendBufferSize     int   // outbound channel capacity (number of frames)
	clock              clock
	dialer             dialer
}

func defaultClientConfig() *clientConfig {
	return &clientConfig{
		codec:          wspulse.JSONCodec,
		logger:         zap.Must(zap.NewProduction()),
		autoReconnect:  false,
		maxRetries:     10,
		baseDelay:      1 * time.Second,
		maxDelay:       30 * time.Second,
		pongWait:       60 * time.Second,
		pingPeriod:     20 * time.Second,
		writeWait:      10 * time.Second,
		maxMessageSize: 1 << 20, // 1 MiB
		sendBufferSize: 256,
		clock:          realClock{},
		dialer:         gorillaDialer{},
	}
}

// WithOnMessage registers a callback invoked for every inbound Frame.
func WithOnMessage(fn func(wspulse.Frame)) ClientOption {
	return func(c *clientConfig) { c.onMessage = fn }
}

// WithOnDisconnect registers a callback invoked when the client permanently
// disconnects. When auto-reconnect is enabled this fires only after all
// retries are exhausted or Close() is called — not on every transport drop.
// When auto-reconnect is disabled it fires once when the client permanently
// disconnects, whether triggered by a server-side drop or an explicit Close().
// err is nil for a normal closure (Close() was called).
func WithOnDisconnect(fn func(err error)) ClientOption {
	return func(c *clientConfig) { c.onDisconnect = fn }
}

// WithOnTransportDrop registers a callback invoked each time the underlying
// WebSocket transport dies. Unlike OnDisconnect, this fires on every
// transport failure including those followed by automatic reconnection.
func WithOnTransportDrop(fn func(err error)) ClientOption {
	return func(c *clientConfig) { c.onTransportDrop = fn }
}

// WithOnTransportRestore registers a callback invoked after a successful
// reconnect when the new transport is ready and the internal pumps have been started.
// Does not fire on the initial connection.
func WithOnTransportRestore(fn func()) ClientOption {
	return func(c *clientConfig) { c.onTransportRestore = fn }
}

// WithCodec replaces the default JSONCodec.
// Panics if codec is nil (fail-fast at construction rather than at first Send).
func WithCodec(codec wspulse.Codec) ClientOption {
	if codec == nil {
		panic("wspulse: codec must not be nil")
	}
	return func(c *clientConfig) { c.codec = codec }
}

// WithAutoReconnect enables automatic reconnection with exponential backoff.
// maxRetries 0 retries indefinitely; positive values must be in [1, 32].
// baseDelay must be in (0, 1m] and maxDelay must be in [baseDelay, 5m].
func WithAutoReconnect(maxRetries int, baseDelay, maxDelay time.Duration) ClientOption {
	if maxRetries < 0 {
		panic("wspulse: autoReconnect.maxRetries must be non-negative")
	}
	if baseDelay <= 0 {
		panic("wspulse: autoReconnect.baseDelay must be positive")
	}
	if baseDelay > maxBaseDelay {
		panic("wspulse: autoReconnect.baseDelay exceeds maximum (1m)")
	}
	if maxDelay < baseDelay {
		panic("wspulse: autoReconnect.maxDelay must be >= autoReconnect.baseDelay")
	}
	if maxDelay > maxDelayLimit {
		panic("wspulse: autoReconnect.maxDelay exceeds maximum (5m)")
	}
	if maxRetries > maxRetriesLimit {
		panic("wspulse: autoReconnect.maxRetries exceeds maximum (32)")
	}
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
		panic("wspulse: logger must not be nil")
	}
	return func(c *clientConfig) { c.logger = logger }
}

// WithHeartbeat configures client-side Ping/Pong heartbeat intervals.
// pingPeriod must be in (0, 1m], pongWait in (pingPeriod, 2m], writeWait in (0, 30s].
func WithHeartbeat(pingPeriod, pongWait, writeWait time.Duration) ClientOption {
	if pingPeriod <= 0 {
		panic("wspulse: heartbeat.pingPeriod must be positive")
	}
	if pongWait <= 0 {
		panic("wspulse: heartbeat.pongWait must be positive")
	}
	if writeWait <= 0 {
		panic("wspulse: writeWait must be positive")
	}
	if pingPeriod >= pongWait {
		panic("wspulse: heartbeat.pingPeriod must be strictly less than heartbeat.pongWait")
	}
	if pingPeriod > maxPingPeriod {
		panic("wspulse: heartbeat.pingPeriod exceeds maximum (1m)")
	}
	if pongWait > maxPongWait {
		panic("wspulse: heartbeat.pongWait exceeds maximum (2m)")
	}
	if writeWait > maxWriteWait {
		panic("wspulse: writeWait exceeds maximum (30s)")
	}
	return func(c *clientConfig) {
		c.pingPeriod = pingPeriod
		c.pongWait = pongWait
		c.writeWait = writeWait
	}
}

// WithSendBufferSize sets the outbound channel capacity (number of frames).
// n must be in [1, 4096]. Default is 256.
func WithSendBufferSize(n int) ClientOption {
	if n < 1 {
		panic("wspulse: sendBufferSize must be at least 1")
	}
	if n > maxSendBufFrames {
		panic(fmt.Sprintf("wspulse: sendBufferSize exceeds maximum (%d)", maxSendBufFrames))
	}
	return func(c *clientConfig) { c.sendBufferSize = n }
}

// WithMaxMessageSize sets the maximum size in bytes for inbound messages.
// n must be in [0, 67108864] (64 MiB). 0 disables size enforcement.
func WithMaxMessageSize(n int64) ClientOption {
	if n < 0 {
		panic("wspulse: maxMessageSize must be non-negative")
	}
	if n > maxMsgSizeBytes {
		panic("wspulse: maxMessageSize exceeds maximum (64 MiB)")
	}
	return func(c *clientConfig) { c.maxMessageSize = n }
}
