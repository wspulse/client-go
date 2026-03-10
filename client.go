package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	wspulse "github.com/wspulse/server"
)

// Client is the public interface for the WebSocket client.
type Client interface {
	// Send enqueues f for delivery to the server.
	Send(f wspulse.Frame) error

	// Close terminates the connection and stops any reconnect loop.
	Close() error

	// Done returns a channel closed when Close() is called.
	Done() <-chan struct{}
}

// internalClient is the unexported, concrete implementation of Client.
//
// Signal channels:
//   - done            : closed by Close(); signals Send() and writePump to stop.
//   - quit            : closed by Close(); signals reconnectLoop to stop.
//   - connectionQuit  : closed by reconnectLoop when it successfully reconnects,
//     telling the OLD writePump to yield so the NEW one can take over.
//     Swapped (replaced with a fresh channel) on each reconnect.
type internalClient struct {
	url                string
	config             *clientConfig
	logger             *zap.Logger
	connection         *websocket.Conn
	send               chan []byte
	done               chan struct{}  // closed only by Close()
	quit               chan struct{}  // closed by Close() to stop reconnect loop
	connectionQuit     chan struct{}  // closed to stop the current writePump; swapped on each reconnect
	pumpDone           chan struct{}  // closed by writePump on exit; used by reconnectLoop to wait for the old pump
	mu                 sync.Mutex     // guards connection, connectionQuit, and pumpDone across goroutines
	once               sync.Once      // ensures Close() logic runs only once
	goroutineWaitGroup sync.WaitGroup // tracks all internal goroutines so Close() can wait for their exit
}

// Dial connects to urlStr and returns a Client.
// If WithAutoReconnect is configured, reconnection runs in the background.
func Dial(urlStr string, opts ...ClientOption) (Client, error) {
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
		send:           make(chan []byte, 256),
		done:           make(chan struct{}),
		quit:           make(chan struct{}),
		connectionQuit: connectionQuit,
		pumpDone:       pumpDone,
	}
	if err := c.dialOnce(); err != nil {
		return nil, fmt.Errorf("wspulse/client: %w", err)
	}
	c.logger.Debug("wspulse/client: connected",
		zap.String("url", urlStr),
	)
	dropped := make(chan struct{})
	c.goroutineWaitGroup.Add(3)
	go func() { defer c.goroutineWaitGroup.Done(); c.writePump(connectionQuit, pumpDone) }()
	go func() { defer c.goroutineWaitGroup.Done(); c.readPump(dropped) }()
	if config.autoReconnect {
		go func() { defer c.goroutineWaitGroup.Done(); c.reconnectLoop(dropped) }()
	} else {
		go func() {
			defer c.goroutineWaitGroup.Done()
			<-dropped
			c.logger.Debug("wspulse/client: connection dropped permanently (no reconnect)")
			c.once.Do(func() {
				close(c.done)
				close(c.quit)
			})
			if fn := c.config.onDisconnect; fn != nil {
				fn(nil)
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
// OnDisconnect, OnTransportDrop, OnReconnect); the callback runs inside
// a tracked goroutine, and waiting for it to exit would deadlock.
// Use go c.Close() instead if closing from a callback is required.
func (c *internalClient) Close() error {
	c.once.Do(func() {
		c.logger.Info("wspulse/client: closing",
			zap.String("url", c.url),
		)
		close(c.done)
		close(c.quit)
	})
	c.goroutineWaitGroup.Wait()
	return nil
}

// Done returns a channel closed when Close() is called.
func (c *internalClient) Done() <-chan struct{} { return c.done }

// ── internal ──────────────────────────────────────────────────────────────────

func (c *internalClient) dialOnce() error {
	wsConnection, _, err := websocket.DefaultDialer.Dial(c.url, c.config.dialHeaders)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.connection = wsConnection
	c.mu.Unlock()
	return nil
}

func (c *internalClient) readPump(dropped chan struct{}) {
	c.mu.Lock()
	wsConnection := c.connection
	c.mu.Unlock()

	var readErr error

	defer func() {
		if r := recover(); r != nil {
			readErr = fmt.Errorf("wspulse/client: readPump panic: %v", r)
			c.logger.Error("wspulse/client: readPump panic recovered",
				zap.Any("panic", r),
			)
		}
		_ = wsConnection.Close()
		close(dropped)

		c.logger.Debug("wspulse/client: connection lost",
			zap.Error(readErr),
		)

		if fn := c.config.onTransportDrop; fn != nil {
			fn(readErr)
		}
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
				c.logger.Warn("wspulse/client: decode failed, frame dropped",
					zap.Error(decodeErr),
				)
			}
		}
	}
}

func (c *internalClient) writePump(connectionQuit chan struct{}, pumpDone chan struct{}) {
	c.mu.Lock()
	wsConnection := c.connection
	c.mu.Unlock()

	writeWait := c.config.writeWait
	pingPeriod := c.config.pingPeriod

	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = wsConnection.Close()
		close(pumpDone)
	}()

	for {
		select {
		case <-connectionQuit:
			c.logger.Debug("wspulse/client: writePump yielding for reconnect (priority)")
			return
		default:
		}

		select {
		case data := <-c.send:
			_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsConnection.WriteMessage(c.config.codec.FrameType(), data); err != nil {
				c.logger.Warn("wspulse/client: write failed",
					zap.Error(err),
				)
				return
			}

		case <-ticker.C:
			_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
			if err := wsConnection.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Warn("wspulse/client: ping write failed",
					zap.Error(err),
				)
				return
			}

		case <-c.done:
			c.logger.Debug("wspulse/client: writePump stopping (client closed)")
			_ = wsConnection.SetWriteDeadline(time.Now().Add(writeWait))
			_ = wsConnection.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			return

		case <-connectionQuit:
			c.logger.Debug("wspulse/client: writePump yielding for reconnect")
			return
		}
	}
}

func (c *internalClient) reconnectLoop(dropped chan struct{}) {
	defer func() {
		if fn := c.config.onDisconnect; fn != nil {
			fn(nil)
		}
	}()

	attempt := 0
	for {
		select {
		case <-c.quit:
			return
		case <-dropped:
		}

		if c.config.maxRetries > 0 && attempt >= c.config.maxRetries {
			c.logger.Warn("wspulse/client: max retries exhausted, closing client",
				zap.Int("max_retries", c.config.maxRetries),
			)
			c.once.Do(func() {
				close(c.done)
				close(c.quit)
			})
			return
		}

		delay := backoff(attempt, c.config.baseDelay, c.config.maxDelay)
		c.logger.Debug("wspulse/client: connection dropped, backoff before retry",
			zap.Int("attempt", attempt),
			zap.Duration("delay", delay),
		)
		backoffTimer := time.NewTimer(delay)
		select {
		case <-c.quit:
			backoffTimer.Stop()
			return
		case <-backoffTimer.C:
		}

		if fn := c.config.onReconnect; fn != nil {
			fn(attempt)
		}

		c.logger.Debug("wspulse/client: reconnect attempt",
			zap.Int("attempt", attempt),
			zap.String("url", c.url),
		)
		if err := c.dialOnce(); err != nil {
			c.logger.Debug("wspulse/client: dial failed",
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
			c.logger.Debug("wspulse/client: quit during dial, closing fresh connection")
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
		c.mu.Unlock()

		close(oldQuit)
		<-oldPumpDone

		c.goroutineWaitGroup.Add(2)
		go func() { defer c.goroutineWaitGroup.Done(); c.writePump(newQuit, newPumpDone) }()
		go func() { defer c.goroutineWaitGroup.Done(); c.readPump(dropped) }()
		c.logger.Info("wspulse/client: reconnected",
			zap.Int("attempt", attempt),
			zap.String("url", c.url),
		)
		attempt = 0
	}
}

// backoff returns the delay before the next reconnect attempt.
// It doubles on each attempt (capped at maxDelay).
// The shift is capped at 62 bits to prevent integer overflow.
func backoff(attempt int, base, max time.Duration) time.Duration {
	const maxShift = 62
	if attempt > maxShift {
		attempt = maxShift
	}
	d := base * time.Duration(1<<uint(attempt))
	if d > max || d <= 0 {
		d = max
	}
	return d
}
