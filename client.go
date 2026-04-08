package client

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	wspulse "github.com/wspulse/core"
)

// ErrRetriesExhausted is returned to OnDisconnect when all reconnect
// attempts have been exhausted without establishing a connection.
var ErrRetriesExhausted = errors.New("wspulse: max reconnect retries exhausted")

// ErrConnectionLost is returned to OnDisconnect when the server drops the
// connection and auto-reconnect is disabled.
var ErrConnectionLost = errors.New("wspulse: connection lost")

// Client is the public interface for the WebSocket client.
type Client interface {
	// Send enqueues f for delivery to the server.
	Send(f wspulse.Frame) error

	// Close terminates the connection and stops any reconnect loop.
	Close() error

	// Done returns a channel closed when the client permanently disconnects.
	// This includes an explicit Close() call, a server-side drop when
	// auto-reconnect is disabled, or max reconnect retries being exhausted.
	Done() <-chan struct{}
}

// internalClient is the unexported, concrete implementation of Client.
//
// Signal channels:
//   - done            : closed via once.Do on any permanent disconnect (explicit
//     Close(), server drop without auto-reconnect, or max retries exhausted);
//     signals Send() and writePump to stop.
//   - quit            : closed together with done (same once.Do); signals
//     reconnectLoop to stop.
//   - connectionQuit  : closed by reconnectLoop when it successfully reconnects,
//     telling the OLD writePump to yield so the NEW one can take over.
//     Swapped (replaced with a fresh channel) on each reconnect.
type internalClient struct {
	url                string
	config             *clientConfig
	logger             *zap.Logger
	dialer             dialer
	clock              clock
	connection         wspulse.Transport
	send               chan []byte
	done               chan struct{}  // closed via once.Do on permanent disconnect
	quit               chan struct{}  // closed together with done via once.Do
	connectionQuit     chan struct{}  // closed to stop the current writePump; swapped on each reconnect
	pumpDone           chan struct{}  // closed by writePump on exit; used by reconnectLoop to wait for the old pump
	mu                 sync.Mutex     // guards connection, connectionQuit, and pumpDone across goroutines
	once               sync.Once      // ensures Close() logic runs only once
	goroutineWaitGroup sync.WaitGroup // tracks all internal goroutines so Close() can wait for their exit
}

// Dial connects to urlStr and returns a Client.
// If WithAutoReconnect is configured, reconnection runs in the background.
//
// Accepted URL schemes: ws://, wss://, http://, https://.
// HTTP schemes are automatically converted to their WebSocket equivalents
// (http → ws, https → wss). Invalid or unsupported schemes are passed
// through and will be rejected by the underlying WebSocket dialer.
func Dial(urlStr string, opts ...ClientOption) (Client, error) {
	urlStr = normalizeScheme(urlStr)
	config := defaultClientConfig()
	for _, o := range opts {
		o(config)
	}
	connectionQuit := make(chan struct{})
	pumpDone := make(chan struct{})
	c := &internalClient{
		url:            urlStr,
		config:         config,
		logger:         config.logger,
		dialer:         config.dialer,
		clock:          config.clock,
		send:           make(chan []byte, config.sendBufferSize),
		done:           make(chan struct{}),
		quit:           make(chan struct{}),
		connectionQuit: connectionQuit,
		pumpDone:       pumpDone,
	}
	if err := c.dialOnce(); err != nil {
		return nil, fmt.Errorf("wspulse: dial: %w", err)
	}
	c.logger.Debug("wspulse: connected",
		zap.String("url", urlStr),
	)
	dropped := make(chan struct{})
	writeErrCh := make(chan error, 1)
	c.goroutineWaitGroup.Add(3)
	conn := c.connection
	go func() { defer c.goroutineWaitGroup.Done(); c.writePump(conn, connectionQuit, pumpDone, writeErrCh) }()
	go func() { defer c.goroutineWaitGroup.Done(); c.readPump(conn, dropped, writeErrCh) }()
	if config.autoReconnect {
		go func() { defer c.goroutineWaitGroup.Done(); c.reconnectLoop(dropped) }()
	} else {
		go func() {
			defer c.goroutineWaitGroup.Done()
			<-dropped
			c.logger.Debug("wspulse: connection dropped permanently (no reconnect)")

			// If done is already closed, Close() was called first — normal closure.
			// Otherwise the server dropped the connection — abnormal.
			var disconnectErr error
			select {
			case <-c.done:
			default:
				disconnectErr = ErrConnectionLost
			}

			c.once.Do(func() {
				close(c.done)
				close(c.quit)
			})
			if fn := c.config.onDisconnect; fn != nil {
				fn(disconnectErr)
			}
		}()
	}
	return c, nil
}

var _ Client = (*internalClient)(nil)

// Send enqueues f for delivery to the server.
func (c *internalClient) Send(f wspulse.Frame) error {
	select {
	case <-c.done:
		return wspulse.ErrConnectionClosed
	default:
	}

	data, err := c.config.codec.Encode(f)
	if err != nil {
		return err
	}

	select {
	case c.send <- data:
		return nil
	case <-c.done:
		return wspulse.ErrConnectionClosed
	default:
		return wspulse.ErrSendBufferFull
	}
}

// Close terminates the connection and stops any reconnect loop.
// It blocks until all internal goroutines have exited, so after Close
// returns the client holds no background resources.
// Safe to call multiple times.
//
// Do not call Close synchronously from within any callback (OnMessage,
// OnDisconnect, OnTransportDrop, OnTransportRestore); the callback runs inside
// a tracked goroutine, and waiting for it to exit would deadlock.
// Use go c.Close() instead if closing from a callback is required.
func (c *internalClient) Close() error {
	c.once.Do(func() {
		c.logger.Info("wspulse: closing",
			zap.String("url", c.url),
		)
		close(c.done)
		close(c.quit)
	})
	c.goroutineWaitGroup.Wait()
	return nil
}

// Done returns a channel closed when the client permanently disconnects.
func (c *internalClient) Done() <-chan struct{} { return c.done }

// ── internal ──────────────────────────────────────────────────────────────────

func (c *internalClient) dialOnce() error {
	transport, err := c.dialer.Dial(c.url, c.config.dialHeaders)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.connection = transport
	c.mu.Unlock()
	return nil
}

func (c *internalClient) readPump(wsConnection wspulse.Transport, dropped chan struct{}, writeErrCh <-chan error) {

	var readErr error

	defer func() {
		if r := recover(); r != nil {
			readErr = fmt.Errorf("wspulse: readPump panic: %v", r)
			c.logger.Error("wspulse: readPump panic recovered",
				zap.Any("panic", r),
			)
		}
		_ = wsConnection.Close()

		// Determine the root-cause error for onTransportDrop:
		//   1. User-initiated close → nil (behaviour contract).
		//   2. writePump reported an error → use it (root cause).
		//   3. Otherwise → readPump's own readErr.
		select {
		case <-c.done:
			readErr = nil
		default:
			select {
			case writeErr := <-writeErrCh:
				readErr = writeErr
			default:
			}
		}

		c.logger.Debug("wspulse: connection lost",
			zap.Error(readErr),
		)

		if fn := c.config.onTransportDrop; fn != nil {
			fn(readErr)
		}

		close(dropped)
	}()

	pongWait := c.config.pongWait
	if c.config.maxMessageSize > 0 {
		wsConnection.SetReadLimit(c.config.maxMessageSize)
	}
	_ = wsConnection.SetReadDeadline(time.Now().Add(pongWait))
	wsConnection.SetPongHandler(func(string) error {
		return wsConnection.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, data, err := wsConnection.ReadMessage()
		if err != nil {
			readErr = err
			return
		}
		if fn := c.config.onMessage; fn != nil {
			frame, decodeErr := c.config.codec.Decode(data)
			if decodeErr == nil {
				fn(frame)
			} else {
				c.logger.Warn("wspulse: decode failed, frame dropped",
					zap.Error(decodeErr),
				)
			}
		}
	}
}

func (c *internalClient) writePump(wsConnection wspulse.Transport, connectionQuit chan struct{}, pumpDone chan struct{}, writeErrCh chan<- error) {

	writeWait := c.config.writeWait
	pingPeriod := c.config.pingPeriod

	sendClose := func() {
		_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
		_ = wsConnection.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
	}

	ticker := c.clock.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = wsConnection.Close()
		close(pumpDone)
	}()

	for {
		// Reconnect priority check — yield immediately so the new
		// writePump can take over on a fresh connection.
		select {
		case <-connectionQuit:
			c.logger.Debug("wspulse: writePump yielding for reconnect (priority)")
			return
		default:
		}

		// Close priority check — discard buffered frames on shutdown.
		select {
		case <-c.done:
			c.logger.Debug("wspulse: writePump stopping (client closed)")
			sendClose()
			return
		default:
		}

		select {
		case data := <-c.send:
			_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsConnection.WriteMessage(c.config.codec.FrameType(), data); err != nil {
				c.logger.Warn("wspulse: write failed",
					zap.Error(err),
				)
				select {
				case writeErrCh <- err:
				default:
				}
				return
			}

		case <-ticker.C:
			_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsConnection.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Warn("wspulse: ping write failed",
					zap.Error(err),
				)
				select {
				case writeErrCh <- err:
				default:
				}
				return
			}

		case <-c.done:
			c.logger.Debug("wspulse: writePump stopping (client closed)")
			sendClose()
			return

		case <-connectionQuit:
			c.logger.Debug("wspulse: writePump yielding for reconnect")
			return
		}
	}
}

func (c *internalClient) reconnectLoop(dropped chan struct{}) {
	var disconnectErr error
	defer func() {
		if fn := c.config.onDisconnect; fn != nil {
			fn(disconnectErr)
		}
	}()

	attempt := 0
	for {
		select {
		case <-c.quit:
			// Close() was called — normal closure; disconnectErr stays nil.
			return
		case <-dropped:
		}

		if c.config.maxRetries > 0 && attempt >= c.config.maxRetries {
			c.logger.Warn("wspulse: max retries exhausted, closing client",
				zap.Int("max_retries", c.config.maxRetries),
			)
			disconnectErr = ErrRetriesExhausted
			c.once.Do(func() {
				close(c.done)
				close(c.quit)
			})
			return
		}

		delay := backoff(attempt, c.config.baseDelay, c.config.maxDelay)
		c.logger.Debug("wspulse: connection dropped, backoff before retry",
			zap.Int("attempt", attempt),
			zap.Duration("delay", delay),
		)
		backoffTimer := c.clock.NewTimer(delay)
		select {
		case <-c.quit:
			backoffTimer.Stop()
			return
		case <-backoffTimer.C:
		}

		c.logger.Debug("wspulse: reconnect attempt",
			zap.Int("attempt", attempt),
			zap.String("url", c.url),
		)
		if err := c.dialOnce(); err != nil {
			c.logger.Debug("wspulse: dial failed",
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			attempt++
			dropped = make(chan struct{})
			close(dropped)
			continue
		}

		select {
		case <-c.quit:
			c.logger.Debug("wspulse: quit during dial, closing fresh connection")
			c.mu.Lock()
			_ = c.connection.Close()
			c.mu.Unlock()
			return
		default:
		}

		dropped = make(chan struct{})
		c.mu.Lock()
		oldQuit := c.connectionQuit
		oldPumpDone := c.pumpDone
		newQuit := make(chan struct{})
		newPumpDone := make(chan struct{})
		c.connectionQuit = newQuit
		c.pumpDone = newPumpDone
		conn := c.connection
		c.mu.Unlock()

		close(oldQuit)
		<-oldPumpDone

		// Guard: if Close() was called while we were waiting for the old
		// pumps to drain, skip launching new ones to avoid wasted work.
		// Note: a panic from Add-concurrent-with-Wait is impossible here
		// because reconnectLoop itself holds one WaitGroup count, keeping
		// the counter ≥ 1 until this function returns.
		select {
		case <-c.quit:
			c.logger.Debug("wspulse: quit before starting fresh pumps, closing fresh connection")
			_ = conn.Close()
			return
		default:
		}

		newWriteErrCh := make(chan error, 1)
		c.goroutineWaitGroup.Add(2)
		go func() { defer c.goroutineWaitGroup.Done(); c.writePump(conn, newQuit, newPumpDone, newWriteErrCh) }()
		go func() { defer c.goroutineWaitGroup.Done(); c.readPump(conn, dropped, newWriteErrCh) }()
		c.logger.Info("wspulse: reconnected",
			zap.Int("attempt", attempt),
			zap.String("url", c.url),
		)
		attempt = 0
		if fn := c.config.onTransportRestore; fn != nil {
			fn()
		}
	}
}

// normalizeScheme converts http/https URL schemes to their WebSocket
// equivalents (http → ws, https → wss). All other URLs are returned
// unchanged — gorilla/websocket already validates schemes at dial
// time and returns a catchable error, so we avoid duplicating that.
func normalizeScheme(rawURL string) string {
	if len(rawURL) < 8 {
		return rawURL
	}
	lower := strings.ToLower(rawURL[:8])
	switch {
	case strings.HasPrefix(lower, "https://"):
		return "wss://" + rawURL[len("https://"):]
	case strings.HasPrefix(lower, "http://"):
		return "ws://" + rawURL[len("http://"):]
	default:
		return rawURL
	}
}

// backoff returns the delay before the next reconnect attempt.
// It uses equal jitter: the result is uniformly distributed in
// [fullDelay/2, fullDelay], where fullDelay = base * 2^attempt
// (capped at maxDelay). The shift is capped at 62 bits to prevent
// integer overflow.
func backoff(attempt int, base, max time.Duration) time.Duration {
	const maxShift = 62
	if attempt > maxShift {
		attempt = maxShift
	}
	d := base * time.Duration(1<<uint(attempt))
	if d > max || d <= 0 {
		d = max
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half+1)))
}
