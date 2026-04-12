package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

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
//   - done       : closed via once.Do on any permanent disconnect (explicit
//     Close(), server drop without auto-reconnect, or max retries exhausted);
//     signals Send() and writePump to stop.
//   - quit       : closed together with done (same once.Do); signals
//     reconnectLoop to stop.
//
// Pump lifecycle:
//   - pumpCancel : cancels the pump context, causing readPump, writePump,
//     and pingPump to exit. Called on reconnect (to swap pumps) and on
//     Close() (to shut down permanently).
//   - pumpDone   : closed by writePump on exit; used by reconnectLoop
//     to wait for the old pumps before starting new ones.
type internalClient struct {
	url                string
	config             *clientConfig
	logger             *zap.Logger
	dialer             dialer
	clock              clock
	connection         wspulse.Transport
	send               chan []byte
	done               chan struct{}      // closed via once.Do on permanent disconnect
	quit               chan struct{}      // closed together with done via once.Do
	pumpDone           chan struct{}      // closed by writePump on exit
	pumpCancel         context.CancelFunc // cancels pump context to stop all pumps
	mu                 sync.Mutex         // guards connection, pumpDone, and pumpCancel
	once               sync.Once          // ensures Close() logic runs only once
	goroutineWaitGroup sync.WaitGroup     // tracks all internal goroutines so Close() can wait
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
	c := &internalClient{
		url:    urlStr,
		config: config,
		logger: config.logger,
		dialer: config.dialer,
		clock:  config.clock,
		send:   make(chan []byte, config.sendBufferSize),
		done:   make(chan struct{}),
		quit:   make(chan struct{}),
	}
	if err := c.dialOnce(context.Background()); err != nil {
		return nil, fmt.Errorf("wspulse: dial: %w", err)
	}
	c.logger.Debug("wspulse: connected",
		zap.String("url", urlStr),
	)

	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pumpDone := make(chan struct{})
	dropped := make(chan struct{})
	writeErrCh := make(chan error, 1)
	conn := c.connection

	c.pumpCancel = pumpCancel
	c.pumpDone = pumpDone

	c.goroutineWaitGroup.Add(4)
	go func() { defer c.goroutineWaitGroup.Done(); c.readPump(pumpCtx, conn, dropped, writeErrCh) }()
	go func() { defer c.goroutineWaitGroup.Done(); c.writePump(pumpCtx, conn, pumpDone, writeErrCh) }()
	go func() { defer c.goroutineWaitGroup.Done(); c.pingPump(pumpCtx, conn) }()
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
				c.mu.Lock()
				c.pumpCancel()
				c.mu.Unlock()
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
		c.mu.Lock()
		c.pumpCancel()
		c.mu.Unlock()
	})
	c.goroutineWaitGroup.Wait()
	return nil
}

// Done returns a channel closed when the client permanently disconnects.
func (c *internalClient) Done() <-chan struct{} { return c.done }

// ── internal ──────────────────────────────────────────────────────────────────

func (c *internalClient) dialOnce(ctx context.Context) error {
	transport, err := c.dialer.Dial(ctx, c.url, c.config.dialHeaders)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.connection = transport
	c.mu.Unlock()
	return nil
}

func (c *internalClient) readPump(ctx context.Context, transport wspulse.Transport, dropped chan struct{}, writeErrCh <-chan error) {

	var readErr error

	defer func() {
		if r := recover(); r != nil {
			readErr = fmt.Errorf("wspulse: readPump panic: %v", r)
			c.logger.Error("wspulse: readPump panic recovered",
				zap.Any("panic", r),
			)
		}
		// Capture any write error BEFORE closing the transport.
		// If writePump already failed, its error is on the channel.
		// Reading before CloseNow() prevents a spurious close-induced
		// write error from overriding the original readErr.
		var writeErr error
		select {
		case writeErr = <-writeErrCh:
		default:
		}

		_ = transport.CloseNow()

		// Determine the root-cause error for onTransportDrop:
		//   1. User-initiated close → nil (behaviour contract).
		//   2. writePump reported an error → use it (root cause).
		//   3. Otherwise → readPump's own readErr.
		select {
		case <-c.done:
			readErr = nil
		default:
			if writeErr != nil {
				readErr = writeErr
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

	if c.config.maxMessageSize > 0 {
		transport.SetReadLimit(c.config.maxMessageSize)
	}

	for {
		_, data, err := transport.Read(ctx)
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

func (c *internalClient) writePump(ctx context.Context, transport wspulse.Transport, pumpDone chan struct{}, writeErrCh chan<- error) {

	defer func() {
		_ = transport.CloseNow()
		close(pumpDone)
	}()

	for {
		// Reconnect priority check — yield immediately so the new
		// writePump can take over on a fresh connection.
		select {
		case <-ctx.Done():
			c.closeOrForce(transport)
			return
		default:
		}

		// Close priority check — discard buffered frames on shutdown.
		select {
		case <-c.done:
			c.logger.Debug("wspulse: writePump stopping (client closed)")
			_ = transport.Close(wspulse.StatusNormalClosure, "")
			return
		default:
		}

		select {
		case data := <-c.send:
			writeCtx, cancel := context.WithTimeout(ctx, c.config.writeTimeout)
			err := transport.Write(writeCtx, c.config.codec.FrameType(), data)
			cancel()
			if err != nil {
				c.logger.Warn("wspulse: write failed",
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
			_ = transport.Close(wspulse.StatusNormalClosure, "")
			return

		case <-ctx.Done():
			c.closeOrForce(transport)
			return
		}
	}
}

// closeOrForce sends a close frame if the client is shutting down, or
// force-closes if yielding for reconnect.
func (c *internalClient) closeOrForce(transport wspulse.Transport) {
	select {
	case <-c.done:
		c.logger.Debug("wspulse: writePump stopping (client closed)")
		_ = transport.Close(wspulse.StatusNormalClosure, "")
	default:
		c.logger.Debug("wspulse: writePump yielding for reconnect")
	}
}

func (c *internalClient) pingPump(ctx context.Context, transport wspulse.Transport) {
	ticker := c.clock.NewTicker(c.config.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.doPing(ctx, transport); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// doPing sends a ping and waits for the pong within writeTimeout.
// If the parent context is cancelled (reconnect or close), returns
// without logging or killing the transport — that is a normal exit.
// If the pong times out, force-closes the transport so readPump detects
// the error.
func (c *internalClient) doPing(ctx context.Context, transport wspulse.Transport) error {
	pingCtx, cancel := context.WithTimeout(ctx, c.config.writeTimeout)
	defer cancel()
	if err := transport.Ping(pingCtx); err != nil {
		// Parent context cancelled — normal shutdown or reconnect.
		if ctx.Err() != nil {
			return err
		}
		// Pong timeout — force-close the transport to trigger readPump error.
		c.logger.Warn("wspulse: pong timeout, closing transport",
			zap.Error(err),
		)
		_ = transport.CloseNow()
		return err
	}
	return nil
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
				c.mu.Lock()
				c.pumpCancel()
				c.mu.Unlock()
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
		if err := c.dialOnce(context.Background()); err != nil {
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
			_ = c.connection.CloseNow()
			c.mu.Unlock()
			return
		default:
		}

		dropped = make(chan struct{})

		// Cancel old pumps and wait for writePump to exit.
		c.mu.Lock()
		c.pumpCancel()
		oldPumpDone := c.pumpDone
		c.mu.Unlock()

		<-oldPumpDone

		// Guard: if Close() was called while we were waiting for the old
		// pumps to drain, skip launching new ones to avoid wasted work.
		// Note: a panic from Add-concurrent-with-Wait is impossible here
		// because reconnectLoop itself holds one WaitGroup count, keeping
		// the counter >= 1 until this function returns.
		select {
		case <-c.quit:
			c.logger.Debug("wspulse: quit before starting fresh pumps, closing fresh connection")
			c.mu.Lock()
			_ = c.connection.CloseNow()
			c.mu.Unlock()
			return
		default:
		}

		newPumpCtx, newPumpCancel := context.WithCancel(context.Background())
		newPumpDone := make(chan struct{})
		newWriteErrCh := make(chan error, 1)

		c.mu.Lock()
		c.pumpCancel = newPumpCancel
		c.pumpDone = newPumpDone
		conn := c.connection
		c.mu.Unlock()

		c.goroutineWaitGroup.Add(3)
		go func() { defer c.goroutineWaitGroup.Done(); c.readPump(newPumpCtx, conn, dropped, newWriteErrCh) }()
		go func() { defer c.goroutineWaitGroup.Done(); c.writePump(newPumpCtx, conn, newPumpDone, newWriteErrCh) }()
		go func() { defer c.goroutineWaitGroup.Done(); c.pingPump(newPumpCtx, conn) }()
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
// unchanged — the underlying WebSocket dialer validates schemes at dial
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
