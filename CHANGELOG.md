# Changelog

## [Unreleased]

### Added

- Benchmark harness with `make bench-ci`, `make bench-sync`, and a CI
  workflow that uploads `bench.txt` as an artefact on every PR. See
  `doc/bench.md` for the baseline numbers. (#60)

## [0.10.0] - 2026-05-02

### Changed

- **BREAKING**: `ErrServerClosed` sentinel replaced by `ServerClosedError` struct carrying `Code wspulse.StatusCode` and `Reason string`. `coderTransport.Read` now extracts the exact close code and reason from the server's close frame. Use `errors.As(err, &sce)` to inspect the values; `errors.Is(err, &ServerClosedError{})` works as a type-check shortcut. See [wspulse/.github#37](https://github.com/wspulse/.github/issues/37).

## [0.9.0] - 2026-04-20

### Changed

- **BREAKING**: `Client.Send(f wspulse.Frame)` renamed to `Client.Send(m wspulse.Message)` — follows upstream core rename (`wspulse/.github#34`)
- **BREAKING**: `WithOnMessage(fn func(wspulse.Frame))` renamed to `WithOnMessage(fn func(wspulse.Message))` — follows upstream core rename
- `Codec.FrameType()` usage updated to `Codec.WireType()` — follows upstream core rename

## [0.8.1] - 2026-04-16

### Changed

- Replace `core.Transport` with a client-local `transport` interface; `Ping` is omitted
  since dead-connection detection is server-side only. Requires `wspulse/core` v0.5.0+.

## [0.8.0] - 2026-04-16

### Removed

- **BREAKING**: `WithPingInterval` option — client-side ping is removed; dead-connection detection is now handled exclusively by the Hub's server-side heartbeat.
- **BREAKING**: `ErrNetworkUnhealthy` sentinel error — no longer applicable without client-side ping.

## [0.7.0] - 2026-04-15

### Added

- `ErrNetworkUnhealthy`: returned to `OnTransportDrop` when the client sends a ping and the server does not reply within `writeTimeout`. Previously the callback received a generic `net.ErrClosed` with the root cause lost.
- `ErrServerClosed`: returned to `OnTransportDrop` when the server initiates a WebSocket close handshake (any close frame status code). Previously the callback received a library-specific error type from `coder/websocket`.

## [0.6.0] - 2026-04-13

### Changed

- **BREAKING**: Replace `gorilla/websocket` (archived) with `coder/websocket` as the underlying WebSocket transport
- **BREAKING**: Remove `WithHeartbeat(pingPeriod, pongWait, writeWait)` option; replaced by two independent options:
  - `WithPingInterval(d time.Duration)` — heartbeat interval (default 20s, max 1m)
  - `WithWriteTimeout(d time.Duration)` — write/pong deadline (default 10s, max 30s)
- Extract `pingPump` as a dedicated goroutine (was inlined in `writePump`), giving a 3-pump architecture: readPump + writePump + pingPump
- Internal `dialer` interface now accepts `context.Context`; reconnect dial is cancellable via `Close()`

## [0.5.1] - 2026-04-08

### Fixed

- `writePump` now checks `c.done` before draining `c.send`, ensuring buffered frames are discarded on `Close()` per the behaviour contract
- `onTransportDrop` now receives `nil` when triggered by `Close()`, matching the behaviour contract. Previously it received a misleading read-side error ("use of closed network connection").
- `onTransportDrop` now receives the original write error when `writePump` fails (e.g. write timeout). Previously the write error was discarded and `onTransportDrop` received a secondary read-side error.
- Fixed a race where a close-induced write error could override the original read error in `onTransportDrop`. The `writeErrCh` read now happens before `wsConnection.Close()` to prevent spurious override.

## [0.5.0] - 2026-04-04

### Added

- `WithSendBufferSize(n int)` option — configurable outbound channel capacity [1, 4096], default 256
- `Dial` auto-converts `http://` to `ws://` and `https://` to `wss://` (case-insensitive per RFC 3986). Other schemes are passed through to the underlying WebSocket dialer.

### Changed

- Extracted `Transport` interface and test-only `WithDialer` support for mock-based testing
- Migrated all tests to deterministic component tests using mock transport — zero network I/O
- Adopted `testify` for test assertions

### Removed

- **BREAKING**: `Frame.ID` field removed — transport layer does not use it. Applications needing message IDs should use Payload.

## [0.4.1] - 2026-03-28

### Changed

- Migrate integration tests from in-process `wspulse/server` to out-of-process `testserver` binary
- Remove direct `github.com/wspulse/server` dependency — only `core` remains
- Unify log message prefix from `wspulse/client:` to `wspulse:` across all internal logging

## [0.4.0] - 2026-03-24

### Added

- `WithOnTransportRestore` callback option, fired after a successful reconnect

### Removed

- `WithOnReconnect` callback option (replaced by `WithOnTransportRestore`)

## [0.3.0] - 2026-03-22

### Changed

- **BREAKING**: negative `maxRetries` now panics instead of being treated as
  unlimited. Use `0` for unlimited retries.
- Validation error messages use fully-qualified field names (`heartbeat.pongWait`,
  `autoReconnect.baseDelay`) to match the config validation contract.

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

## [0.2.1] - 2026-03-13

### Changed

- Bump `github.com/wspulse/server` to v0.3.0 (test-only dependency; server package renamed to `wspulse`)

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

[Unreleased]: https://github.com/wspulse/client-go/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/wspulse/client-go/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/wspulse/client-go/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/wspulse/client-go/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/wspulse/client-go/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/wspulse/client-go/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/wspulse/client-go/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/wspulse/client-go/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/wspulse/client-go/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/wspulse/client-go/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/wspulse/client-go/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/wspulse/client-go/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/wspulse/client-go/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/wspulse/client-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/wspulse/client-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/wspulse/client-go/releases/tag/v0.1.0
