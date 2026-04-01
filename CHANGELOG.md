# Changelog

## [Unreleased]

### Added

- `WithSendBufferSize(n int)` option — configurable outbound channel capacity [1, 4096], default 256
- `Dial` auto-converts `http://` to `ws://` and `https://` to `wss://` (case-insensitive per RFC 3986). Other schemes are passed through to the underlying WebSocket dialer.

### Removed

- **BREAKING**: `Frame.ID` field removed — transport layer does not use it. Applications needing message IDs should use Payload.

---

## [0.4.1] - 2026-03-28

### Changed

- Migrate integration tests from in-process `wspulse/server` to out-of-process `testserver` binary
- Remove direct `github.com/wspulse/server` dependency — only `core` remains
- Unify log message prefix from `wspulse/client:` to `wspulse:` across all internal logging

---

## [0.4.0] - 2026-03-24

### Added

- `WithOnTransportRestore` callback option, fired after a successful reconnect

### Removed

- `WithOnReconnect` callback option (replaced by `WithOnTransportRestore`)

---

## [0.3.0] - 2026-03-22

### Changed

- **BREAKING**: negative `maxRetries` now panics instead of being treated as
  unlimited. Use `0` for unlimited retries.
- Validation error messages use fully-qualified field names (`heartbeat.pongWait`,
  `autoReconnect.baseDelay`) to match the config validation contract.

---

## [0.2.2] - 2026-03-21

### Changed

- Default logger changed from `zap.NewNop()` to `zap.NewProduction()`. Internal
  diagnostics (decode failures, reconnect lifecycle, transport drops) are now
  visible by default. Use `WithLogger(zap.NewNop())` to disable.

### Added

- Integration tests: heartbeat pong timeout (scenario 7), concurrent
  close/transport-drop race (scenario 9).
- CI/CD: auto-label on PR opened, tag-triggered GitHub Release, `release.yml`
  changelog categories, CD workflow.

---

## [0.2.1] - 2026-03-13

### Changed

- Bump `github.com/wspulse/server` to v0.3.0 (test-only dependency; server package renamed to `wspulse`)

---

## [0.2.0] - 2026-03-12

### Added

- `ErrRetriesExhausted` sentinel error returned to `OnDisconnect` when max retries exhausted
- `ErrConnectionLost` sentinel error returned to `OnDisconnect` on server-side drop (no auto-reconnect)

### Changed

- Bump `github.com/wspulse/core` to v0.2.0 (direct) and `github.com/wspulse/server` to v0.2.0
- `Frame.Event` (renamed from `Frame.Type`) and wire key `"event"` (renamed from `"type"`) — follows core v0.2.0 breaking change (**breaking**)
- Added frame routing section to README

### Fixed

- `OnDisconnect` callback now receives a non-nil error on abnormal closure (server drop or retries exhausted); previously always received nil
- Backoff function now applies equal jitter (`[delay/2, delay]`) to prevent thundering herd on mass reconnect
- `Done()` GoDoc corrected: channel closes on any permanent disconnect, not only on `Close()`
- `Dial` error now uses correct `wspulse: dial:` prefix per project convention

---

## [0.1.0] - 2026-03-10

### Added

- `Client` with `Dial(url string, opts ...ClientOption) (Client, error)`
- `Client.Send(frame wspulse.Frame) error`
- `Client.Close() error` — waits for all internal goroutines to exit
- `Client.Done() <-chan struct{}`
- Automatic reconnect with exponential backoff
- `WithOnMessage(fn func(wspulse.Frame))`, `WithOnReconnect(fn func())`
- `WithOnDisconnect(fn func(err error))`, `WithOnTransportDrop(fn func(err error))`
- `WithAutoReconnect(maxRetries int, baseDelay, maxDelay time.Duration)`
- `WithHeartbeat(pingPeriod, pongWait, writeWait time.Duration)`, `WithMaxMessageSize`
- `WithCodec(codec)`, `WithDialHeaders(h http.Header)`, `WithLogger(l *zap.Logger)` options
- Upper bound validation on all configurable options (panics on construction)

### Fixed

- Orphaned callback goroutines on disconnect — all goroutines cleaned up on `Close`
- `Close()` waits for all internal goroutines to exit before returning

[Unreleased]: https://github.com/wspulse/client-go/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/wspulse/client-go/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/wspulse/client-go/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/wspulse/client-go/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/wspulse/client-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/wspulse/client-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/wspulse/client-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/wspulse/client-go/releases/tag/v0.1.0
