package client

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	wspulse "github.com/wspulse/core"
)

// jsonPayload returns a valid JSON string payload with total byte length size.
// size includes the surrounding quotes; minimum is 2 (an empty JSON string).
func jsonPayload(size int) []byte {
	if size < 2 {
		size = 2
	}
	return []byte(`"` + strings.Repeat("x", size-2) + `"`)
}

// messageSizes is the standard payload size matrix for Send benchmarks.
// Values match the workspace bench-harness plan.
var messageSizes = []struct {
	label string
	size  int
}{
	{"64B", 64},
	{"1KiB", 1024},
	{"16KiB", 16 * 1024},
}

// startWSSink starts a minimal WebSocket sink that accepts and drains all
// incoming messages with no application-level processing. Used as the bench
// counterpart so the client-side Send path is the only thing measured (apart
// from network/transport overhead inherent to a real WebSocket loopback).
func startWSSink(b *testing.B) *httptest.Server {
	b.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		// Drain reads until the client disconnects.
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	})
	return httptest.NewServer(handler)
}

func BenchmarkSend(b *testing.B) {
	for _, shape := range []struct {
		label string
		loop  int
	}{
		{"single", 1},
		{"loop_10", 10},
		{"loop_100", 100},
	} {
		for _, ms := range messageSizes {
			name := fmt.Sprintf("shape=%s/messageSize=%s", shape.label, ms.label)
			b.Run(name, func(b *testing.B) {
				benchSend(b, shape.loop, ms.size)
			})
		}
	}
}

// benchSend measures b.N batches of `loop` Send calls each. Reported ns/op is
// per-batch cost; divide by `loop` to get per-message cost.
func benchSend(b *testing.B, loop, payloadSize int) {
	ts := startWSSink(b)
	b.Cleanup(ts.Close)

	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := Dial(url, WithLogger(zap.NewNop()))
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })

	msg := wspulse.Message{Event: "bench", Payload: jsonPayload(payloadSize)}
	// unexpectedErrs counts any Send errors that aren't the expected
	// ErrSendBufferFull. A non-zero count means the connection dropped or
	// the codec failed mid-bench, in which case the timing data is junk.
	// Direct sentinel comparison (not errors.Is) keeps the hot path cheap.
	var unexpectedErrs int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < loop; j++ {
			// ErrSendBufferFull is expected at benchmark speed when writePump
			// cannot keep up with enqueue rate. The bench measures Send call
			// cost (encode + enqueue) including the buffer-full path.
			if err := c.Send(msg); err != nil && err != wspulse.ErrSendBufferFull {
				unexpectedErrs++
			}
		}
	}
	b.StopTimer()
	if unexpectedErrs > 0 {
		b.Fatalf("got %d unexpected Send errors during bench", unexpectedErrs)
	}
}

// BenchmarkReconnectBackoff measures the cost of one backoff() call. The
// formula must match all other client-* SDKs (workspace critical rule 5);
// regressions in cost or distribution are worth catching early.
//
// `attempt` cycles through 0..29 to exercise the doubling path including
// saturation at maxDelay.
func BenchmarkReconnectBackoff(b *testing.B) {
	const (
		base = 100 * time.Millisecond
		max  = 30 * time.Second
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = backoff(i%30, base, max)
	}
}
