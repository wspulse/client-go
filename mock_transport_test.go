package client_test

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

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
	blockCh   chan struct{} // when non-nil, WriteMessage blocks until closeCh

	mu          sync.Mutex
	readLimit   int64
	pongHandler func(string) error
	closed      bool
}

type readResult struct {
	messageType int
	data        []byte
	err         error
}

type writeCall struct {
	messageType int
	data        []byte
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		readCh:  make(chan readResult, 16),
		writeCh: make(chan writeCall, 256),
		closeCh: make(chan struct{}),
	}
}

func (m *mockTransport) ReadMessage() (int, []byte, error) {
	select {
	case r := <-m.readCh:
		return r.messageType, r.data, r.err
	case <-m.closeCh:
		return 0, nil, &net.OpError{Op: "read", Err: net.ErrClosed}
	}
}

func (m *mockTransport) WriteMessage(messageType int, data []byte) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return &net.OpError{Op: "write", Err: net.ErrClosed}
	}
	blockCh := m.blockCh
	m.mu.Unlock()

	// When blockCh is set, block until the transport is closed.
	// This lets tests deterministically stall writePump to fill c.send.
	if blockCh != nil {
		select {
		case <-blockCh:
			return &net.OpError{Op: "write", Err: net.ErrClosed}
		case <-m.closeCh:
			return &net.OpError{Op: "write", Err: net.ErrClosed}
		}
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	select {
	case m.writeCh <- writeCall{messageType: messageType, data: copied}:
		return nil
	case <-m.closeCh:
		return &net.OpError{Op: "write", Err: net.ErrClosed}
	default:
		// Channel full — drop silently. Backpressure tests use blockCh instead.
		return nil
	}
}

func (m *mockTransport) SetReadLimit(limit int64) {
	m.mu.Lock()
	m.readLimit = limit
	m.mu.Unlock()
}

func (m *mockTransport) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockTransport) SetWriteDeadline(_ time.Time) error { return nil }

func (m *mockTransport) SetPongHandler(h func(string) error) {
	m.mu.Lock()
	m.pongHandler = h
	m.mu.Unlock()
}

func (m *mockTransport) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		close(m.closeCh)
	})
	return nil
}

// BlockWrites causes WriteMessage to block until unblock is called or the
// transport is closed. Used by backpressure tests to deterministically stall
// writePump. The returned function unblocks all pending and future writes.
func (m *mockTransport) BlockWrites() (unblock func()) {
	ch := make(chan struct{})
	m.mu.Lock()
	m.blockCh = ch
	m.mu.Unlock()
	once := sync.Once{}
	return func() { once.Do(func() { close(ch) }) }
}

// InjectMessage simulates a message from the server.
func (m *mockTransport) InjectMessage(messageType int, data []byte) {
	m.readCh <- readResult{messageType: messageType, data: data}
}

// InjectError simulates a read error (e.g. connection drop).
func (m *mockTransport) InjectError(err error) {
	m.readCh <- readResult{err: err}
}

// WaitWrite waits for a single write with timeout.
func (m *mockTransport) WaitWrite(timeout time.Duration) (writeCall, bool) {
	select {
	case c := <-m.writeCh:
		return c, true
	case <-time.After(timeout):
		return writeCall{}, false
	}
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

func (d *mockDialer) Dial(url string, header http.Header) (wspulse.Transport, error) {
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
