package client

// Clock exports the internal clock interface for testing only.
type Clock = clock

// WithClock returns a ClientOption that sets the clock. Test-only.
func WithClock(c Clock) ClientOption {
	return func(cfg *clientConfig) { cfg.clock = c }
}
