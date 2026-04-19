package client_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		{"https with fragment", "https://host/ws#section", "wss://host/ws#section"},
		{"HTTP uppercase", "HTTP://host/ws", "ws://host/ws"},
		{"HTTPS uppercase", "HTTPS://host/ws", "wss://host/ws"},
		{"Http mixed case", "Http://host/ws", "ws://host/ws"},
		{"unsupported scheme passthrough", "ftp://host/ws", "ftp://host/ws"},
		{"missing scheme passthrough", "host/ws", "host/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := client.NormalizeScheme(tc.input)
			assert.Equal(t, tc.want, got, "NormalizeScheme(%q)", tc.input)
		})
	}
}

func TestDial_ReturnsErrorOnBadURL(t *testing.T) {
	t.Parallel()
	_, err := client.Dial("ws://localhost:0/no-such-server")
	assert.Error(t, err, "expected error dialing unreachable server")
}

func TestDial_ErrorFormat(t *testing.T) {
	t.Parallel()
	_, err := client.Dial("ws://localhost:0/no-such-server")
	require.Error(t, err, "expected error dialing unreachable server")
	const wantPrefix = "wspulse: dial: "
	assert.True(t, strings.HasPrefix(err.Error(), wantPrefix),
		"error format: want prefix %q, got %q", wantPrefix, err.Error())
	assert.NotNil(t, errors.Unwrap(err), "Dial error must wrap the underlying dial error")
}

func TestClient_WithCodec_Nil_Panics(t *testing.T) {
	t.Parallel()
	require.PanicsWithValue(t, "wspulse: codec must not be nil", func() {
		_ = client.WithCodec(nil)
	})
}

func TestClient_WithWriteTimeout_InvalidParams_Panics(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		d       time.Duration
		wantMsg string
	}{
		{"zero", 0, "wspulse: writeTimeout must be positive"},
		{"negative", -1 * time.Second, "wspulse: writeTimeout must be positive"},
		{"exceeds max", 31 * time.Second, "wspulse: writeTimeout exceeds maximum (30s)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.PanicsWithValue(t, tc.wantMsg, func() {
				_ = client.WithWriteTimeout(tc.d)
			})
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
			require.PanicsWithValue(t, tc.wantMsg, func() {
				client.WithSendBufferSize(tc.n)
			})
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
			require.PanicsWithValue(t, tc.wantMsg, func() {
				client.WithMaxMessageSize(tc.n)
			})
		})
	}
}

func TestClient_WithLogger_Nil_Panics(t *testing.T) {
	t.Parallel()
	require.PanicsWithValue(t, "wspulse: logger must not be nil", func() {
		client.WithLogger(nil)
	})
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
			require.PanicsWithValue(t, tc.wantMsg, func() {
				_ = client.WithAutoReconnect(tc.maxRetries, tc.base, tc.max)
			})
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
			require.NotNil(t, opt)
		})
	}
}

func TestClient_CallbackOptions_Valid_NoPanic(t *testing.T) {
	t.Parallel()
	// Verify that callback-related option builders accept valid callbacks
	// without panicking and always return non-nil ClientOption values.
	opts := []client.ClientOption{
		client.WithOnMessage(func(f wspulse.Message) {}),
		client.WithOnDisconnect(func(err error) {}),
		client.WithOnTransportDrop(func(err error) {}),
		client.WithOnTransportRestore(func() {}),
	}
	for _, opt := range opts {
		assert.NotNil(t, opt, "option function returned nil")
	}
}

func TestClient_WithOnMessage_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	// Nil callbacks should not panic at option construction time.
	opt := client.WithOnMessage(nil)
	assert.NotNil(t, opt)
}

func TestClient_WithOnTransportRestore_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnTransportRestore(nil)
	assert.NotNil(t, opt)
}

func TestClient_WithOnDisconnect_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnDisconnect(nil)
	assert.NotNil(t, opt)
}

func TestClient_WithOnTransportDrop_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithOnTransportDrop(nil)
	assert.NotNil(t, opt)
}

func TestClient_WithDialHeaders_Nil_NoPanic(t *testing.T) {
	t.Parallel()
	opt := client.WithDialHeaders(nil)
	assert.NotNil(t, opt)
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
			assert.NotNil(t, opt)
		})
	}
}

func TestClient_WithWriteTimeout_ValidParams_NoPanic(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		d    time.Duration
	}{
		{"minimum", 1 * time.Millisecond},
		{"typical", 10 * time.Second},
		{"maximum", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := client.WithWriteTimeout(tc.d)
			assert.NotNil(t, opt)
		})
	}
}
