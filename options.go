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
	maxPingInterval  = 1 * time.Minute  // WithPingInterval upper bound
	maxWriteTimeout  = 30 * time.Second // WithWriteTimeout upper bound
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
	pingInterval       time.Duration
	writeTimeout       time.Duration
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
		pingInterval:   20 * time.Second,
		writeTimeout:   10 * time.Second,
		maxMessageSize: 1 << 20, // 1 MiB
		sendBufferSize: 256,
		clock:          realClock{},
		dialer:         coderDialer{},
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

// WithPingInterval sets the interval between heartbeat pings.
// d must be in (0, 1m]. Default is 20s.
func WithPingInterval(d time.Duration) ClientOption {
	if d <= 0 {
		panic("wspulse: pingInterval must be positive")
	}
	if d > maxPingInterval {
		panic("wspulse: pingInterval exceeds maximum (1m)")
	}
	return func(c *clientConfig) { c.pingInterval = d }
}

// WithWriteTimeout sets the timeout for all write operations: data frames,
// ping/pong, and the close handshake. Pong must arrive within this duration
// or the connection is considered dead. d must be in (0, 30s]. Default is 10s.
func WithWriteTimeout(d time.Duration) ClientOption {
	if d <= 0 {
		panic("wspulse: writeTimeout must be positive")
	}
	if d > maxWriteTimeout {
		panic("wspulse: writeTimeout exceeds maximum (30s)")
	}
	return func(c *clientConfig) { c.writeTimeout = d }
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
