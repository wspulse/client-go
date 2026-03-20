# Integration Test Coverage — client-go

> **Contract:** all scenarios defined in
> [`.github/doc/contracts/integration-test-scenarios.md`](https://github.com/wspulse/.github/blob/main/doc/contracts/integration-test-scenarios.md)

Integration tests run against a live `wspulse/server` embedded in-process via
`httptest.NewServer`. Because client-go can import the server package directly,
each test spins up its own server rather than using the shared testserver binary.
Server-side actions (kick, close) are performed via the `srv.*` API instead of
the HTTP control endpoint.

**Run:** `make test-integration` (or `go test -race -count=1 -tags integration ./...`)

## Scenario Matrix

| #   | Scenario                                                            | Test Name                                                         |
| --- | ------------------------------------------------------------------- | ----------------------------------------------------------------- |
| 1   | Connect → send → echo → close clean                                 | `TestDial_SendAndReceive`                                         |
| 2   | Server drops → onTransportDrop + onDisconnect (no reconnect)        | `TestClient_OnDisconnect_IsErrConnectionLostOnServerDrop`         |
| 3   | Auto-reconnect: server drops → reconnects within maxRetries         | `TestClient_AutoReconnect_ReconnectsAndDeliversMessages`          |
| 4   | Max retries exhausted → `onDisconnect(ErrRetriesExhausted)`         | `TestClient_AutoReconnect_MaxRetriesExhausted_ClosesDone`         |
| 5   | `Close()` during reconnect → loop stops, `onDisconnect(nil)`        | `TestClient_AutoReconnect_CloseDuringBackoff`                     |
| 6   | `Send()` on closed client → `ErrConnectionClosed`                   | `TestClient_Send_AfterClose_ReturnsErrConnectionClosed`           |
| 7   | Heartbeat pong timeout → `ErrConnectionLost`                        | `TestClient_HeartbeatPongTimeout_TriggersTransportDrop`            |
| 8   | Concurrent sends: no data race or interleaving                      | `TestClient_ConcurrentSendAndClose_NoRace`                        |
| 9   | Concurrent `Close()` + transport drop → `onDisconnect` exactly once | `TestClient_ConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce` |

> **Coverage notes:**
> - Scenario 2 asserts `onDisconnect(ErrConnectionLost)`; `onTransportDrop` without `autoReconnect` has no dedicated test.
> - Scenario 4 asserts `Done()` closes; the `ErrRetriesExhausted` error type is verified separately by `TestClient_OnDisconnect_NonNilOnMaxRetries` (non-nil only).
> - Scenario 5 verifies `Close()` unblocks the reconnect loop but does not assert `onDisconnect(nil)`.
> - Scenario 7 uses a raw WebSocket server (no-pong) to verify read-deadline timeout triggers `ErrConnectionLost`.
> - Scenario 9 races `Close()` against `srv.Close()` and asserts `onDisconnect` fires exactly once with no goroutine leak.

## Additional Tests

| Test Name                                              | What It Covers                                                              |
| ------------------------------------------------------ | --------------------------------------------------------------------------- |
| `TestClient_Close_SafeToCallTwice`                     | `Close()` is safe to call multiple times                                    |
| `TestClient_Done_ClosedAfterClose`                     | `Done()` channel closes after `Close()`                                     |
| `TestClient_OnDisconnect_CallbackFires`                | `onDisconnect` fires on normal close                                        |
| `TestClient_OnDisconnect_NilOnNormalClose`             | `onDisconnect` receives `nil` error on user-initiated close                 |
| `TestClient_OnDisconnect_NonNilOnServerDrop`           | `onDisconnect` receives non-nil error on server drop                        |
| `TestClient_Done_FiresOnServerDrop`                    | `Done()` fires and `Send()` returns `ErrConnectionClosed` after server drop |
| `TestClient_OnTransportDrop_FiresOnReconnect`          | `onTransportDrop` fires when a kick occurs (autoReconnect enabled)          |
| `TestClient_AutoReconnect_Close_FiresOnDisconnect`     | `Close()` with autoReconnect enabled still fires `onDisconnect`             |
| `TestClient_OnDisconnect_NonNilOnMaxRetries`           | `onDisconnect` receives non-nil error when retries are exhausted            |
| `TestClient_Close_OnDisconnectFiresExactlyOnce`        | `onDisconnect` fires exactly once on `Close()`                              |
| `TestClient_ReadPump_PanicRecovery`                    | Panic inside `onMessage` is recovered; `onDisconnect` fires gracefully      |
| `TestClient_WithDialHeaders`                           | Custom dial headers are forwarded to the server's `ConnectFunc`             |
| `TestClient_WithMaxMessageSize_OversizedMessage`       | Oversized inbound message closes the transport                              |
| `TestClient_Send_BufferFull_ReturnsErrSendBufferFull`  | `Send()` returns `ErrSendBufferFull` when the write buffer is saturated     |
| `TestClient_ReadPump_DecodeFailure_DropsFrame`         | Malformed frame is silently dropped; connection continues                   |
| `TestClient_Send_EncodeError_ReturnsError`             | `Send()` propagates codec encode errors                                     |
| `TestClient_Close_WaitsForDisconnectCallback`          | `Close()` blocks until `onDisconnect` callback completes                    |
| `TestClient_Close_WaitsForTransportDropCallback`       | `Close()` blocks until `onTransportDrop` callback completes                 |
| `TestClient_Close_WaitsForDisconnectCallback_AutoReconnect` | Same guarantee holds with autoReconnect enabled                        |
| `TestClient_Close_WaitsForGoroutines`                  | `Close()` joins all internal goroutines (no goroutine leak)                 |
| `TestClient_Close_WaitsForGoroutines_AutoReconnect`    | Same guarantee holds with autoReconnect enabled                             |
| `TestClient_WithLogger_ValidLogger_Applied`            | `WithLogger` option is accepted without error                               |
| `TestClient_WithHeartbeat_ValidParams_Applied`         | `WithHeartbeat` option is accepted without error                            |

**Total: 32 integration tests** (9 scenarios covered; 23 additional).
