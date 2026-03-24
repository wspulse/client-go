package client

// Clock exports the internal clock interface for testing only.
type Clock = clock

// WithClock returns a ClientOption that sets the clock. Test-only.
func WithClock(c Clock) ClientOption {
	if c == nil {
		panic("wspulse: WithClock: clock must not be nil")
	}
	return func(cfg *clientConfig) { cfg.clock = c }
}
