package client_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

// ── mockTransport ───────────────────────────────────────────────────────────

// mockTransport is a channel-based, deterministic Transport for component tests.
type mockTransport struct {
	readCh    chan readResult
	writeCh   chan writeCall
	closeCh   chan struct{}
	closeOnce sync.Once
	blockCh   chan struct{} // when non-nil, Write blocks until blockCh or closeCh is closed

	mu           sync.Mutex
	readLimit    int64
	readLimitSet chan struct{}                   // signaled once when SetReadLimit is first called with a non-zero value
	writeEntered chan struct{}                   // when non-nil, signaled each time Write is entered (before blocking)
	pingCh       chan struct{}                   // when non-nil, signaled each time Ping is called
	pingFunc     func(ctx context.Context) error // when non-nil, Ping delegates to this function
	closeCalled  chan closeCall                  // when non-nil, signaled on Close(code, reason)
	writeErr     error                           // when non-nil, Write returns this error immediately
	closeHook    func()                          // when non-nil, runs inside doClose after closeCh is closed
	closed       bool
}

type readResult struct {
	messageType wspulse.MessageType
	data        []byte
	err         error
}

type writeCall struct {
	messageType wspulse.MessageType
	data        []byte
}

type closeCall struct {
	code   wspulse.StatusCode
	reason string
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		readCh:       make(chan readResult, 16),
		writeCh:      make(chan writeCall, 256),
		closeCh:      make(chan struct{}),
		readLimitSet: make(chan struct{}, 1),
	}
}

func (m *mockTransport) Read(ctx context.Context) (wspulse.MessageType, []byte, error) {
	select {
	case r := <-m.readCh:
		return r.messageType, r.data, r.err
	case <-m.closeCh:
		return 0, nil, &net.OpError{Op: "read", Err: net.ErrClosed}
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (m *mockTransport) Write(ctx context.Context, typ wspulse.MessageType, data []byte) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return &net.OpError{Op: "write", Err: net.ErrClosed}
	}
	if m.writeErr != nil {
		err := m.writeErr
		m.mu.Unlock()
		return err
	}
	blockCh := m.blockCh
	writeEntered := m.writeEntered
	m.mu.Unlock()

	// Signal entry before blocking so tests can synchronize.
	if writeEntered != nil {
		select {
		case writeEntered <- struct{}{}:
		default:
		}
	}

	// When blockCh is set, block until unblock is called or the transport
	// is closed. After unblock, blockCh is cleared so writes resume normally.
	if blockCh != nil {
		select {
		case <-blockCh:
			// Unblocked — fall through to normal write path.
		case <-m.closeCh:
			return &net.OpError{Op: "write", Err: net.ErrClosed}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	select {
	case m.writeCh <- writeCall{messageType: typ, data: copied}:
		return nil
	case <-m.closeCh:
		return &net.OpError{Op: "write", Err: net.ErrClosed}
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel full — drop silently. Backpressure tests use blockCh instead.
		return nil
	}
}

func (m *mockTransport) Ping(ctx context.Context) error {
	if m.pingCh != nil {
		select {
		case m.pingCh <- struct{}{}:
		default:
		}
	}
	m.mu.Lock()
	fn := m.pingFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (m *mockTransport) SetReadLimit(limit int64) {
	m.mu.Lock()
	m.readLimit = limit
	m.mu.Unlock()
	if limit != 0 {
		select {
		case m.readLimitSet <- struct{}{}:
		default:
		}
	}
}

func (m *mockTransport) Close(code wspulse.StatusCode, reason string) error {
	if m.closeCalled != nil {
		select {
		case m.closeCalled <- closeCall{code: code, reason: reason}:
		default:
		}
	}
	return m.doClose()
}

func (m *mockTransport) CloseNow() error {
	return m.doClose()
}

func (m *mockTransport) doClose() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		hook := m.closeHook
		m.mu.Unlock()
		close(m.closeCh)
		if hook != nil {
			hook()
		}
	})
	return nil
}

// BlockWrites causes Write to block until unblock is called or the
// transport is closed. Used by backpressure tests to deterministically stall
// writePump. The returned function unblocks all pending and future writes.
func (m *mockTransport) BlockWrites() (unblock func()) {
	ch := make(chan struct{})
	m.mu.Lock()
	m.blockCh = ch
	m.mu.Unlock()
	once := sync.Once{}
	return func() {
		once.Do(func() {
			m.mu.Lock()
			m.blockCh = nil
			m.mu.Unlock()
			close(ch)
		})
	}
}

// SetWriteError causes all subsequent Write calls to return err.
func (m *mockTransport) SetWriteError(err error) {
	m.mu.Lock()
	m.writeErr = err
	m.mu.Unlock()
}

// InjectMessage simulates a message from the server.
func (m *mockTransport) InjectMessage(typ wspulse.MessageType, data []byte) {
	m.readCh <- readResult{messageType: typ, data: data}
}

// InjectError simulates a read error (e.g. connection drop).
func (m *mockTransport) InjectError(err error) {
	m.readCh <- readResult{err: err}
}

// DrainWrites reads all pending writes.
func (m *mockTransport) DrainWrites() []writeCall {
	var calls []writeCall
	for {
		select {
		case c := <-m.writeCh:
			calls = append(calls, c)
		default:
			return calls
		}
	}
}

// ── mockDialer ──────────────────────────────────────────────────────────────

// mockDialer returns pre-configured transports on successive Dial calls.
type mockDialer struct {
	mu         sync.Mutex
	results    []mockDialResult
	callCount  int
	dialCalled chan struct{} // signaled on each Dial call
}

type mockDialResult struct {
	transport *mockTransport
	err       error
}

func newMockDialer(results ...mockDialResult) *mockDialer {
	return &mockDialer{
		results:    results,
		dialCalled: make(chan struct{}, 16),
	}
}

func (d *mockDialer) Dial(_ context.Context, _ string, _ http.Header) (wspulse.Transport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.callCount >= len(d.results) {
		return nil, errors.New("mockDialer: no more transports configured")
	}
	r := d.results[d.callCount]
	d.callCount++
	select {
	case d.dialCalled <- struct{}{}:
	default:
	}
	return r.transport, r.err
}

// Transport returns the i-th configured transport.
func (d *mockDialer) Transport(i int) *mockTransport {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.results[i].transport
}

// CallCount returns how many times Dial was called.
func (d *mockDialer) CallCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.callCount
}

// Ensure mockDialer satisfies the exported Dialer type alias.
var _ client.Dialer = (*mockDialer)(nil)
