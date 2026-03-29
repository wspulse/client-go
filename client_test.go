package client_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
)

func TestNormalizeScheme(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"ws passthrough", "ws://host/ws", "ws://host/ws"},
		{"wss passthrough", "wss://host/ws", "wss://host/ws"},
		{"http to ws", "http://host/ws", "ws://host/ws"},
		{"https to wss", "https://host/ws", "wss://host/ws"},
		{"http with port", "http://host:8080/ws", "ws://host:8080/ws"},
		{"https with port and query", "https://host:443/ws?token=abc", "wss://host:443/ws?token=abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := client.NormalizeScheme(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeScheme(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeScheme_MissingScheme_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for missing scheme")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "wspulse: url must include scheme") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client.NormalizeScheme("host/ws")
}

func TestNormalizeScheme_UnsupportedScheme_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for unsupported scheme")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "wspulse: unsupported url scheme") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	client.NormalizeScheme("ftp://host/ws")
}

func TestDial_MissingScheme_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for missing scheme")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "wspulse: url must include scheme") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	_, _ = client.Dial("host/ws")
}

func TestDial_UnsupportedScheme_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for unsupported scheme")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "wspulse: unsupported url scheme") {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	_, _ = client.Dial("ftp://host/ws")
}

func TestDial_ReturnsErrorOnBadURL(t *testing.T) {
	t.Parallel()
	_, err := client.Dial("ws://localhost:0/no-such-server")
	if err == nil {
		t.Error("expected error dialing unreachable server, got nil")
	}
}

func TestDial_ErrorFormat(t *testing.T) {
	t.Parallel()
	_, err := client.Dial("ws://localhost:0/no-such-server")
	if err == nil {
		t.Fatal("expected error dialing unreachable server, got nil")
	}
	const wantPrefix = "wspulse: dial: "
	if !strings.HasPrefix(err.Error(), wantPrefix) {
		t.Errorf("error format: want prefix %q, got %q", wantPrefix, err.Error())
	}
	if errors.Unwrap(err) == nil {
		t.Error("Dial error must wrap the underlying dial error")
	}
}

func TestClient_WithCodec_Nil_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil codec, got none")
		}
		if msg, ok := r.(string); !ok || msg != "wspulse: codec must not be nil" {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()
	_ = client.WithCodec(nil)
}

func TestClient_WithHeartbeat_InvalidParams_Panics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name               string
		ping, pong, writeW time.Duration
		wantMsg            string
	}{
		{"ping zero", 0, 10 * time.Second, 5 * time.Second, "wspulse: heartbeat.pingPeriod must be positive"},
		{"pong zero", 5 * time.Second, 0, 5 * time.Second, "wspulse: heartbeat.pongWait must be positive"},
		{"writeWait zero", 5 * time.Second, 10 * time.Second, 0, "wspulse: writeWait must be positive"},
		{"ping == pong", 10 * time.Second, 10 * time.Second, 5 * time.Second, "wspulse: heartbeat.pingPeriod must be strictly less than heartbeat.pongWait"},
		{"ping > pong", 30 * time.Second, 10 * time.Second, 5 * time.Second, "wspulse: heartbeat.pingPeriod must be strictly less than heartbeat.pongWait"},
		{"ping exceeds max", 2 * time.Minute, 3 * time.Minute, 5 * time.Second, "wspulse: heartbeat.pingPeriod exceeds maximum (1m)"},
		{"pong exceeds max", 1 * time.Second, 3 * time.Minute, 5 * time.Second, "wspulse: heartbeat.pongWait exceeds maximum (2m)"},
		{"writeWait exceeds max", 1 * time.Second, 5 * time.Second, 31 * time.Second, "wspulse: writeWait exceeds maximum (30s)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if msg, ok := r.(string); !ok || msg != tc.wantMsg {
					t.Fatalf("panic message = %v, want %q", r, tc.wantMsg)
				}
			}()
			_ = client.WithHeartbeat(tc.ping, tc.pong, tc.writeW)
		})
	}
}

func TestClient_WithSendBufferSize_InvalidParam_Panics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		n       int
		wantMsg string
	}{
		{"zero", 0, "wspulse: sendBufferSize must be at least 1"},
		{"negative", -1, "wspulse: sendBufferSize must be at least 1"},
		{"exceeds max", 4097, "wspulse: sendBufferSize exceeds maximum (4096)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if msg, ok := r.(string); !ok || msg != tc.wantMsg {
					t.Fatalf("panic message = %v, want %q", r, tc.wantMsg)
				}
			}()
			client.WithSendBufferSize(tc.n)
		})
	}
}

func TestClient_WithSendBufferSize_ValidValues(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 256, 4096} {
		// Should not panic.
		client.WithSendBufferSize(n)
	}
}

func TestClient_WithMaxMessageSize_InvalidParam_Panics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		n       int64
		wantMsg string
	}{
		{"negative", -1, "wspulse: maxMessageSize must be non-negative"},
		{"exceeds max", 65 << 20, "wspulse: maxMessageSize exceeds maximum (64 MiB)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if msg, ok := r.(string); !ok || msg != tc.wantMsg {
					t.Fatalf("panic message = %v, want %q", r, tc.wantMsg)
				}
			}()
			client.WithMaxMessageSize(tc.n)
		})
	}
}

func TestClient_WithLogger_Nil_Panics(t *testing.T) {
	t.Parallel()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for WithLogger(nil)")
		}
		if msg, ok := r.(string); !ok || msg != "wspulse: logger must not be nil" {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()
	client.WithLogger(nil)
}

func TestClient_WithAutoReconnect_InvalidParams_Panics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		maxRetries int
		base, max  time.Duration
		wantMsg    string
	}{
		{"negative maxRetries", -1, 1 * time.Second, 30 * time.Second, "wspulse: autoReconnect.maxRetries must be non-negative"},
		{"baseDelay zero", 3, 0, 30 * time.Second, "wspulse: autoReconnect.baseDelay must be positive"},
		{"baseDelay negative", 3, -1 * time.Second, 30 * time.Second, "wspulse: autoReconnect.baseDelay must be positive"},
		{"baseDelay exceeds max", 3, 2 * time.Minute, 3 * time.Minute, "wspulse: autoReconnect.baseDelay exceeds maximum (1m)"},
		{"maxDelay < baseDelay", 3, 5 * time.Second, 1 * time.Second, "wspulse: autoReconnect.maxDelay must be >= autoReconnect.baseDelay"},
		{"maxDelay exceeds max", 3, 1 * time.Second, 6 * time.Minute, "wspulse: autoReconnect.maxDelay exceeds maximum (5m)"},
		{"maxRetries exceeds max", 33, 1 * time.Second, 30 * time.Second, "wspulse: autoReconnect.maxRetries exceeds maximum (32)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected panic")
				}
				if msg, ok := r.(string); !ok || msg != tc.wantMsg {
					t.Fatalf("panic message = %v, want %q", r, tc.wantMsg)
				}
			}()
			_ = client.WithAutoReconnect(tc.maxRetries, tc.base, tc.max)
		})
	}
}

func TestClient_WithAutoReconnect_ValidParams_NoPanic(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		maxRetries int
		base, max  time.Duration
	}{
		{"zero retries (unlimited)", 0, 500 * time.Millisecond, 1 * time.Minute},
		{"max boundary values", 32, 1 * time.Minute, 5 * time.Minute},
		{"typical values", 10, 1 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := client.WithAutoReconnect(tc.maxRetries, tc.base, tc.max)
			if opt == nil {
				t.Fatal("expected non-nil option")
			}
		})
	}
}

func TestClient_CallbackOptions_Valid_NoPanic(t *testing.T) {
	t.Parallel()
	// Verify that callback-related option builders accept valid callbacks
	// without panicking and always return non-nil ClientOption values.
	opts := []client.ClientOption{
		client.WithOnMessage(func(f wspulse.Frame) {}),
		client.WithOnDisconnect(func(err error) {}),
		client.WithOnTransportDrop(func(err error) {}),
		client.WithOnTransportRestore(func() {}),
	}
	for _, opt := range opts {
		if opt == nil {
			t.Error("option function returned nil")
		}
	}
}

func TestClient_WithOnMessage_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	// Nil callbacks should not panic at option construction time.
	opt := client.WithOnMessage(nil)
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestClient_WithOnTransportRestore_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnTransportRestore(nil)
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestClient_WithOnDisconnect_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnDisconnect(nil)
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestClient_WithOnTransportDrop_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnTransportDrop(nil)
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestClient_WithDialHeaders_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithDialHeaders(nil)
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestClient_WithMaxMessageSize_BoundaryValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		n    int64
	}{
		{"zero (disabled)", 0},
		{"minimum valid (1)", 1},
		{"1 MiB", 1 << 20},
		{"maximum valid (64 MiB)", 64 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := client.WithMaxMessageSize(tc.n)
			if opt == nil {
				t.Error("expected non-nil option")
			}
		})
	}
}

func TestClient_WithHeartbeat_ValidParams_NoPanic(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name               string
		ping, pong, writeW time.Duration
	}{
		{"typical", 20 * time.Second, 60 * time.Second, 10 * time.Second},
		{"minimum gap", 1 * time.Millisecond, 2 * time.Millisecond, 1 * time.Millisecond},
		{"max boundary", 1 * time.Minute, 2 * time.Minute, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := client.WithHeartbeat(tc.ping, tc.pong, tc.writeW)
			if opt == nil {
				t.Error("expected non-nil option")
			}
		})
	}
}
