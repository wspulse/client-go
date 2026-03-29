package client

// Clock exports the internal clock interface for testing only.
type Clock = clock

// SendBufferCap returns the capacity of the client's send channel. Test-only.
func SendBufferCap(c Client) int {
	return cap(c.(*internalClient).send)
}

// NormalizeScheme exports normalizeScheme for testing only.
var NormalizeScheme = normalizeScheme

// WithClock returns a ClientOption that sets the clock. Test-only.
func WithClock(c Clock) ClientOption {
	if c == nil {
		panic("wspulse: WithClock: clock must not be nil")
	}
	return func(cfg *clientConfig) { cfg.clock = c }
}
