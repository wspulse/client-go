package client

// Clock exports the internal clock interface for testing only.
type Clock = clock

// SendBufferCap returns the capacity of the client's send channel. Test-only.
func SendBufferCap(c Client) int {
	return cap(c.(*internalClient).send)
}

// NormalizeScheme exports normalizeScheme for testing only.
func NormalizeScheme(rawURL string) string {
	return normalizeScheme(rawURL)
}

// WithClock returns a ClientOption that sets the clock. Test-only.
func WithClock(c Clock) ClientOption {
	if c == nil {
		panic("wspulse: WithClock: clock must not be nil")
	}
	return func(cfg *clientConfig) { cfg.clock = c }
}

// Transport exports the internal transport interface for testing only.
type Transport = transport

// Dialer exports the internal dialer interface for testing only.
type Dialer = dialer

// WithDialer returns a ClientOption that sets the dialer. Test-only.
func WithDialer(d Dialer) ClientOption {
	if d == nil {
		panic("wspulse: WithDialer: dialer must not be nil")
	}
	return func(cfg *clientConfig) { cfg.dialer = d }
}
