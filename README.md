# wspulse/client-go

[![CI](https://github.com/wspulse/client-go/actions/workflows/ci.yml/badge.svg)](https://github.com/wspulse/client-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/wspulse/client-go.svg)](https://pkg.go.dev/github.com/wspulse/client-go)
[![Go](https://img.shields.io/badge/Go-1.26-blue.svg?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Go WebSocket client with optional automatic reconnection, designed for use with [wspulse/server](https://github.com/wspulse/server).

**Status:** v0 — API is being stabilized. Module path: `github.com/wspulse/client-go`.

---

## Design Goals

- Thin client: connect, send, receive, auto-reconnect
- Matches server-side `Frame` and `Codec` types via [wspulse/core](https://github.com/wspulse/core)
- Exponential backoff with configurable retries
- Transport drop vs. permanent disconnect callbacks

---

## Install

```bash
go get github.com/wspulse/client-go
```

---

## Quick Start

```go
import (
    wspulse "github.com/wspulse/core"
    client  "github.com/wspulse/client-go"
)

c, err := client.Dial("ws://localhost:8080/ws?room=r1&token=xyz",
    client.WithOnMessage(func(f wspulse.Frame) {
        fmt.Printf("[%s] %s\n", f.Event, f.Payload)
    }),
    client.WithAutoReconnect(5, time.Second, 30*time.Second),
)
if err != nil {
    log.Fatal(err)
}
defer c.Close()

c.Send(wspulse.Frame{Event: "msg", Payload: []byte(`{"text":"hello"}`)})
<-c.Done()
```

---

## Frame Types and Server-Side Routing

Every frame sent or received is a JSON object on the wire:

```json
{
  "id": "msg-001",
  "event": "chat.message",
  "payload": { "text": "hello" }
}
```

The `"event"` field is the routing key on the server. Set `frame.Event` to match the handler registered with `r.On("chat.message", ...)` on the server side. The `"payload"` field carries arbitrary JSON — the library does not inspect it.

```go
// Send a typed frame — server routes by "event"
c.Send(wspulse.Frame{
    Event:   "chat.message",
    Payload: []byte(`{"text":"hello world"}`),
})

// Receive typed frames
client.WithOnMessage(func(f wspulse.Frame) {
    switch f.Event {
    case "chat.message":
        // handle message
    case "chat.ack":
        // handle acknowledgement
    case "pong":
        // heartbeat response
    }
})
```

## Event Routing with `core/router`

`core/router` provides Gin-style middleware and per-event handler dispatch. When using the client, pass `nil` as the connection (client frames have no server-side `Connection` concept).

```go
import (
    wspulse "github.com/wspulse/core"
    "github.com/wspulse/core/router"
    client  "github.com/wspulse/client-go"
)

rtr := router.New()
rtr.Use(router.Recovery())

rtr.On("chat.message", func(c *router.Context) {
    fmt.Printf("[msg] %s\n", c.Frame.Payload)
})
rtr.On("chat.welcome", func(c *router.Context) {
    fmt.Println("joined!")
})

c, _ := client.Dial("ws://localhost:8080/ws?room=r1&token=xyz",
    client.WithOnMessage(func(f wspulse.Frame) {
        rtr.Dispatch(nil, f) // no router.Connection on the client side
    }),
    client.WithAutoReconnect(5, time.Second, 30*time.Second),
)
defer c.Close()
```

See [wspulse/core](https://github.com/wspulse/core) for the full `router` API.

---

## Public API Surface

| Symbol           | Description                        |
| ---------------- | ---------------------------------- |
| `Client`         | Interface: `Send`, `Close`, `Done` |
| `Dial(url, ...)` | Connect and return a `Client`      |
| `ClientOption`   | Functional option type             |

### Client options

| Option                                   | Default         |
| ---------------------------------------- | --------------- |
| `WithOnMessage(fn)`                      | —               |
| `WithOnTransportRestore(fn)`             | —               |
| `WithOnDisconnect(fn)`                   | —               |
| `WithOnTransportDrop(fn)`                | —               |
| `WithAutoReconnect(max, base, maxDelay)` | disabled              |
| `WithHeartbeat(ping, pong, writeWait)`   | 20s / 60s / 10s       |
| `WithMaxMessageSize(n)`                  | 1 MiB                 |
| `WithCodec(c)`                           | JSONCodec             |
| `WithDialHeaders(h)`                     | —                     |
| `WithLogger(l)`                          | `zap.NewProduction()` |

---

## Logging

The client logs internal diagnostics via [zap](https://github.com/go-uber/zap). By default a production logger is used (JSON format, Info+ level).

**Replace the logger** with your own:

```go
c, _ := client.Dial(url, client.WithLogger(myZapLogger))
```

**Disable logging:**

```go
c, _ := client.Dial(url, client.WithLogger(zap.NewNop()))
```

---

## Features

- **Auto-reconnect** — exponential backoff with configurable max retries, base delay, and max delay.
- **Transport drop callback** — `WithOnTransportDrop` fires on every transport death, even when auto-reconnect follows. Useful for metrics.
- **Permanent disconnect callback** — `WithOnDisconnect` fires only when the client is truly done (Close() called or retries exhausted).
- **Panic recovery** — panics in `OnMessage` are recovered; the connection is dropped but the process survives.
- **Heartbeat** — client-side Ping/Pong keeps the connection alive and detects silently-dead servers.
- **Backpressure** — bounded send buffer; `ErrSendBufferFull` returned when full.
- **Swappable codec** — JSON by default; plug in any `Codec` implementation.

## Development

```bash
make fmt        # auto-format source files (gofmt + goimports)
make check      # validate format, lint, test with race detector (pre-commit gate)
make test       # go test -race -count=3 ./...
make test-cover # go test with coverage report → coverage.html
make bench      # run benchmarks with memory allocation stats
make tidy       # go mod tidy (GOWORK=off)
make clean      # remove build artifacts and test cache
```

---

## Related Modules             |
| --------------------------------------------------- | ---------------- |
| [wspulse/server](https://github.com/wspulse/server) | WebSocket server |
