# Component Test Coverage — client-go

> **Contract:** all scenarios defined in
> [`.github/doc/contracts/client/test-scenarios.md`](https://github.com/wspulse/.github/blob/main/doc/contracts/client/test-scenarios.md)

Component tests use a channel-based mock transport (`mockTransport`) and mock
dialer (`mockDialer`) — no real WebSocket server or TCP I/O. All tests are
deterministic and run with `make check`.

**Run:** `make test` (or `go test -race ./...`)

## Scenario Matrix

| #   | Scenario                                                            | Test Name                                                    | File                |
| --- | ------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------- |
| 1   | Connect, send, echo, close clean                                    | `TestSendAndReceive`                                         | basic_test.go       |
| 2   | Server drops, onTransportDrop + onDisconnect (no reconnect)         | `TestOnDisconnect_IsErrConnectionLostOnServerDrop`, `TestOnTransportDrop_NonNilOnServerDrop` | callback_test.go    |
| 3   | Auto-reconnect: server drops, reconnects within maxRetries          | `TestAutoReconnect_ReconnectsAndDeliversMessages`            | reconnect_test.go   |
| 4   | Max retries exhausted, `Done()` closes                              | `TestAutoReconnect_MaxRetriesExhausted_ClosesDone`           | reconnect_test.go   |
| 5   | `Close()` during reconnect/backoff, loop stops cleanly              | `TestAutoReconnect_CloseDuringBackoff`                       | reconnect_test.go   |
| 6   | `Send()` on closed client, `ErrConnectionClosed`                    | `TestSend_AfterClose_ReturnsErrConnectionClosed`             | basic_test.go       |
| 7   | Heartbeat ping then read error, client disconnects with non-nil error | `TestHeartbeat_ReadError_DisconnectsClient`                  | misc_test.go        |
| 8   | Concurrent sends: no data race or interleaving                      | `TestConcurrentSendAndClose_NoRace`                          | lifecycle_test.go   |
| 9   | Concurrent `Close()` + transport drop, `onDisconnect` exactly once  | `TestConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce`| lifecycle_test.go   |

## Additional Tests

### basic_test.go

| Test Name                                  | What It Covers                                         |
| ------------------------------------------ | ------------------------------------------------------ |
| `TestSend_WritesCorrectData`               | Encoded frame arrives at transport with correct payload |
| `TestClose_SafeToCallTwice`                | `Close()` is safe to call multiple times               |
| `TestDone_ClosedAfterClose`                | `Done()` channel closes after `Close()`                |
| `TestNormalCloseFrame`                     | `Close()` sends a WebSocket close frame                |

### callback_test.go

| Test Name                                          | What It Covers                                                     |
| -------------------------------------------------- | ------------------------------------------------------------------ |
| `TestOnDisconnect_CallbackFires`                   | `onDisconnect` fires on normal close                               |
| `TestOnDisconnect_NilOnNormalClose`                | `onDisconnect` receives `nil` error on user-initiated close        |
| `TestOnDisconnect_NonNilOnServerDrop`              | `onDisconnect` receives non-nil error on server drop               |
| `TestOnDisconnect_NonNilOnMaxRetries`              | `onDisconnect` receives non-nil error when retries exhausted       |
| `TestClose_OnDisconnectFiresExactlyOnce`           | `onDisconnect` fires exactly once on `Close()`                     |
| `TestOnTransportDrop_Fires_AutoReconnect`           | `onTransportDrop` fires when transport drops (autoReconnect)       |
| `TestOnTransportRestore_FiresAfterReconnect`       | `onTransportRestore` fires after successful reconnect              |
| `TestOnTransportRestore_DoesNotFireOnInitialConnect`| `onTransportRestore` does not fire on first connect               |
| `TestOnTransportRestore_NotFiredOnFailedReconnect` | `onTransportRestore` does not fire when reconnect fails            |

### lifecycle_test.go

| Test Name                                                  | What It Covers                                                |
| ---------------------------------------------------------- | ------------------------------------------------------------- |
| `TestDone_FiresOnServerDrop`                               | `Done()` fires and `Send()` returns error after server drop   |
| `TestClose_WaitsForDisconnectCallback`                     | `Close()` blocks until `onDisconnect` completes               |
| `TestClose_WaitsForTransportDropCallback`                  | `Close()` blocks until `onTransportDrop` completes            |
| `TestClose_WaitsForDisconnectCallback_AutoReconnect`       | Same guarantee with autoReconnect enabled                     |
| `TestClose_WaitsForGoroutines`                             | `Close()` joins all internal goroutines                       |
| `TestClose_WaitsForGoroutines_AutoReconnect`               | Same guarantee with autoReconnect enabled                     |

### reconnect_test.go

| Test Name                                          | What It Covers                                        |
| -------------------------------------------------- | ----------------------------------------------------- |
| `TestAutoReconnect_MultipleRapidCycles`            | Survives 3 rapid drop/reconnect cycles                |
| `TestAutoReconnect_Close_FiresOnDisconnect`        | `Close()` with autoReconnect fires `onDisconnect`     |

### misc_test.go

| Test Name                                          | What It Covers                                                |
| -------------------------------------------------- | ------------------------------------------------------------- |
| `TestSend_BufferFull_ReturnsErrSendBufferFull`     | `Send()` returns `ErrSendBufferFull` when buffer is saturated |
| `TestSend_CustomBufferSize_Applied`                | `WithSendBufferSize` sets the channel capacity                |
| `TestReadPump_DecodeFailure_DropsFrame`            | Malformed frame is dropped; connection continues              |
| `TestSend_EncodeError_ReturnsError`                | `Send()` propagates codec encode errors                       |
| `TestReadPump_PanicRecovery`                       | Panic inside `onMessage` is recovered gracefully              |
| `TestWithDialHeaders`                              | Custom dial headers are forwarded to the dialer               |
| `TestWithMaxMessageSize`                           | `SetReadLimit` is called with configured size                 |
| `TestWithMaxMessageSize_OversizedMessage`          | Oversized message triggers read error                         |
| `TestWithHeartbeat_ValidParams_Applied`            | `WithHeartbeat` option is accepted                            |
| `TestWithHeartbeat_SendsPings`                     | Ping messages are sent at configured interval                 |
| `TestWithLogger_ValidLogger_Applied`               | `WithLogger` option is accepted                               |

**Total: 41 component tests** (9 contract scenarios + 32 additional).
