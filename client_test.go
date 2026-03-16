package client_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/wspulse/client-go"
)

func TestDial_ReturnsErrorOnBadURL(t *testing.T) {
	_, err := client.Dial("ws://localhost:0/no-such-server")
	if err == nil {
		t.Error("expected error dialing unreachable server, got nil")
	}
}

func TestDial_ErrorFormat(t *testing.T) {
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
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil codec, got none")
		}
	}()
	_ = client.WithCodec(nil)
}

func TestClient_WithHeartbeat_InvalidParams_Panics(t *testing.T) {
	cases := []struct {
		name               string
		ping, pong, writeW time.Duration
	}{
		{"ping == pong", 10 * time.Second, 10 * time.Second, 5 * time.Second},
		{"ping > pong", 30 * time.Second, 10 * time.Second, 5 * time.Second},
		{"ping zero", 0, 10 * time.Second, 5 * time.Second},
		{"writeWait zero", 5 * time.Second, 10 * time.Second, 0},
		{"ping exceeds max", 2 * time.Minute, 3 * time.Minute, 5 * time.Second},
		{"pong exceeds max", 1 * time.Second, 3 * time.Minute, 5 * time.Second},
		{"writeWait exceeds max", 1 * time.Second, 5 * time.Second, 31 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			_ = client.WithHeartbeat(tc.ping, tc.pong, tc.writeW)
		})
	}
}

func TestClient_WithMaxMessageSize_InvalidParam_Panics(t *testing.T) {
	cases := []struct {
		name string
		n    int64
	}{
		{"zero", 0},
		{"negative", -1},
		{"exceeds max", 65 << 20},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			client.WithMaxMessageSize(tc.n)
		})
	}
}

func TestClient_WithLogger_Nil_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithLogger(nil)")
		}
	}()
	client.WithLogger(nil)
}

func TestClient_WithAutoReconnect_InvalidParams_Panics(t *testing.T) {
	cases := []struct {
		name       string
		maxRetries int
		base, max  time.Duration
	}{
		{"baseDelay zero", 3, 0, 30 * time.Second},
		{"baseDelay negative", 3, -1 * time.Second, 30 * time.Second},
		{"baseDelay exceeds max", 3, 2 * time.Minute, 3 * time.Minute},
		{"maxDelay < baseDelay", 3, 5 * time.Second, 1 * time.Second},
		{"maxDelay exceeds max", 3, 1 * time.Second, 6 * time.Minute},
		{"maxRetries exceeds max", 33, 1 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			_ = client.WithAutoReconnect(tc.maxRetries, tc.base, tc.max)
		})
	}
}

func TestClient_WithAutoReconnect_ValidParams_NoPanic(t *testing.T) {
	cases := []struct {
		name       string
		maxRetries int
		base, max  time.Duration
	}{
		{"unlimited retries", -1, 1 * time.Second, 30 * time.Second},
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

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
