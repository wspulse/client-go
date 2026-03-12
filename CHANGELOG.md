# Changelog

## [Unreleased]

### Changed

- Bump `github.com/wspulse/core` to v0.2.0 (via `github.com/wspulse/server`)
- `Frame.Event` (renamed from `Frame.Type`) and wire key `"event"` (renamed from `"type"`) — follows core v0.2.0 breaking change (**breaking**)
- Added frame routing section to README

---

## [0.1.0] - 2026-03-10

### Added

- `Client` with `Dial(url string, opts ...ClientOption) (Client, error)`
- `Client.Send(frame wspulse.Frame) error`
- `Client.Close() error` — waits for all internal goroutines to exit
- `Client.Done() <-chan struct{}`
- Automatic reconnect with exponential backoff and jitter
- `WithOnMessage(fn func(wspulse.Frame))`, `WithOnReconnect(fn func(attempt int))`
- `WithOnDisconnect(fn func(err error))`, `WithOnTransportDrop(fn func(err error))`
- `WithAutoReconnect(maxRetries int, baseDelay, maxDelay time.Duration)`
- `WithHeartbeat(pingPeriod, pongWait, writeWait time.Duration)`, `WithMaxMessageSize`
- `WithCodec(codec)`, `WithDialHeaders(h http.Header)`, `WithLogger(l *zap.Logger)` options
- Upper bound validation on all configurable options (panics on construction)

### Fixed

- Orphaned callback goroutines on disconnect — all goroutines cleaned up on `Close`
- `Close()` waits for all internal goroutines to exit before returning

[Unreleased]: https://github.com/wspulse/client-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/wspulse/client-go/releases/tag/v0.1.0
