# wspulse/client-go

A Go WebSocket client with optional automatic reconnection, designed for use with [wspulse/server](https://github.com/wspulse/server).

**Status:** v0 — API is being stabilized. Module path: `github.com/wspulse/client-go`.

---

## Design Goals

- Thin client: connect, send, receive, auto-reconnect
- Matches server-side `Frame` and `Codec` types via [wspulse/server](https://github.com/wspulse/server)
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
    wspulse "github.com/wspulse/server"
    client  "github.com/wspulse/client-go"
)

c, err := client.Dial("ws://localhost:8080/ws?room=r1&token=xyz",
    client.WithOnMessage(func(f wspulse.Frame) {
        fmt.Printf("[%s] %s\n", f.Type, f.Payload)
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
client.Send(server.Frame{
    Event:    "chat.message",
    Payload: []byte(`{"text":"hello world"}`),
})

// Receive typed frames
client.WithOnMessage(func(f server.Frame) {
    switch f.Type {
    case "chat.message":
        // handle message
    case "chat.ack":
        // handle acknowledgement
    case "pong":
        // heartbeat response
    }
})
```

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
| `WithOnReconnect(fn)`                    | —               |
| `WithOnDisconnect(fn)`                   | —               |
| `WithOnTransportDrop(fn)`                | —               |
| `WithAutoReconnect(max, base, maxDelay)` | disabled        |
| `WithHeartbeat(ping, pong, writeWait)`   | 20s / 60s / 10s |
| `WithMaxMessageSize(n)`                  | 1 MiB           |
| `WithCodec(c)`                           | JSONCodec       |
| `WithDialHeaders(h)`                     | —               |
| `WithLogger(l)`                          | zap.NewNop()    |

---

## Features

- **Auto-reconnect** — exponential backoff with configurable max retries, base delay, and max delay.
- **Transport drop callback** — `WithOnTransportDrop` fires on every transport death, even when auto-reconnect follows. Useful for metrics.
- **Permanent disconnect callback** — `WithOnDisconnect` fires only when the client is truly done (Close() called or retries exhausted).
- **Panic recovery** — panics in `OnMessage` are recovered; the connection is dropped but the process survives.
- **Heartbeat** — client-side Ping/Pong keeps the connection alive and detects silently-dead servers.
- **Backpressure** — bounded send buffer; `ErrSendBufferFull` returned when full.
- **Swappable codec** — JSON by default; plug in any `Codec` implementation.

---

## Related Modules

| Module                                              | Description             |
| --------------------------------------------------- | ----------------------- |
| [wspulse/core](https://github.com/wspulse/core)     | Shared types and codecs |
| [wspulse/server](https://github.com/wspulse/server) | WebSocket server        |
