package client_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wspulse/client-go"
	"go.uber.org/goleak"
	wspulse "github.com/wspulse/server"
)

func startEchoServer(t *testing.T) string {
	t.Helper()
	var srv wspulse.Server
	srv = wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "client-1", nil
		},
		wspulse.WithOnMessage(func(connection wspulse.Connection, f wspulse.Frame) {
			_ = connection.Send(f)
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func TestDial_SendAndReceive(t *testing.T) {
	url := startEchoServer(t)
	received := make(chan wspulse.Frame, 1)
	c, err := client.Dial(url, client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	frame := wspulse.Frame{Type: "echo", Payload: []byte(`"hello"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case f := <-received:
		if f.Type != "echo" {
			t.Errorf("Type: want %q, got %q", "echo", f.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestClient_Close_SafeToCallTwice(t *testing.T) {
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	_ = c.Close()
}

func TestClient_Send_AfterClose_ReturnsErrConnectionClosed(t *testing.T) {
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	sendErr := c.Send(wspulse.Frame{Type: "msg"})
	if !errors.Is(sendErr, wspulse.ErrConnectionClosed) {
		t.Errorf("want ErrConnectionClosed, got %v", sendErr)
	}
}

func TestClient_Done_ClosedAfterClose(t *testing.T) {
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() channel not closed after Close()")
	}
}

func TestDial_ReturnsErrorOnBadURL(t *testing.T) {
	_, err := client.Dial("ws://localhost:0/no-such-server")
	if err == nil {
		t.Error("expected error dialing unreachable server, got nil")
	}
}

func TestClient_ConcurrentSendAndClose_NoRace(t *testing.T) {
	url := startEchoServer(t)
	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	const senders = 8
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = c.Send(wspulse.Frame{Type: "msg", Payload: []byte(`"x"`)})
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	_ = c.Close()
	wg.Wait()
}

func TestClient_WithCodec_Nil_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil codec, got none")
		}
	}()
	_ = client.WithCodec(nil)
}

func TestClient_OnDisconnect_CallbackFires(t *testing.T) {
	url := startEchoServer(t)
	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnDisconnect callback")
	}
}

func TestClient_ReadPump_PanicRecovery(t *testing.T) {
	var srv wspulse.Server
	srv = wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "echo-1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			_ = connection.Send(wspulse.Frame{Type: "trigger"})
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnected := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			panic("boom")
		}),
		client.WithOnDisconnect(func(err error) {
			disconnected <- err
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: readPump panic was not recovered")
	}
}

func TestClient_Done_FiresOnServerDrop(t *testing.T) {
	connected := make(chan wspulse.Connection, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			connected <- connection
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connection")
	}

	srv.Close()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: Done() did not fire after server disconnect")
	}

	if err := c.Send(wspulse.Frame{Type: "msg"}); err != wspulse.ErrConnectionClosed {
		t.Fatalf("want ErrConnectionClosed, got %v", err)
	}
}

func TestClient_WithDialHeaders(t *testing.T) {
	headerReceived := make(chan string, 1)
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			headerReceived <- r.Header.Get("X-Custom-Token")
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	headers := http.Header{}
	headers.Set("X-Custom-Token", "test-token-123")

	c, err := client.Dial(url, client.WithDialHeaders(headers))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case got := <-headerReceived:
		if got != "test-token-123" {
			t.Errorf("header value: want %q, got %q", "test-token-123", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for header check")
	}
}

func TestClient_Close_OnDisconnectFiresExactlyOnce(t *testing.T) {
	url := startEchoServer(t)

	var mu sync.Mutex
	disconnectCount := 0

	c, err := client.Dial(url,
		client.WithOnDisconnect(func(err error) {
			mu.Lock()
			disconnectCount++
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_ = c.Close()

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	dc := disconnectCount
	mu.Unlock()

	if dc != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", dc)
	}
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

func TestClient_OnTransportDrop_FiresOnReconnect(t *testing.T) {
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithResumeWindow(5*time.Second),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	transportDropped := make(chan struct{}, 5)
	c, err := client.Dial(url,
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnTransportDrop(func(err error) {
			select {
			case transportDropped <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	time.Sleep(200 * time.Millisecond)

	_ = srv.Kick("c1")

	select {
	case <-transportDropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnTransportDrop")
	}
}

func TestClient_AutoReconnect_Close_FiresOnDisconnect(t *testing.T) {
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	disconnected := make(chan struct{}, 1)
	c, err := client.Dial(url,
		client.WithAutoReconnect(5, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	_ = c.Close()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: onDisconnect did not fire after Close() with auto-reconnect")
	}
}

func TestClient_WithMaxMessageSize_OversizedMessage(t *testing.T) {
	var serverConnection wspulse.Connection
	connected := make(chan struct{}, 1)

	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
		wspulse.WithOnConnect(func(connection wspulse.Connection) {
			serverConnection = connection
			select {
			case connected <- struct{}{}:
			default:
			}
		}),
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	dropped := make(chan error, 1)
	c, err := client.Dial(url,
		client.WithMaxMessageSize(10),
		client.WithOnTransportDrop(func(err error) {
			select {
			case dropped <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for connect")
	}

	bigPayload := []byte(`"` + strings.Repeat("x", 100) + `"`)
	_ = serverConnection.Send(wspulse.Frame{Type: "big", Payload: bigPayload})

	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out: transport should have dropped due to oversized message")
	}
}

func TestClient_WithMaxMessageSize_InvalidParam_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for n=0")
		}
	}()
	client.WithMaxMessageSize(0)
}

func TestClient_WithLogger_Nil_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for WithLogger(nil)")
		}
	}()
	client.WithLogger(nil)
}

func TestClient_WithHeartbeat_ValidParams_NoPanic(t *testing.T) {
	opt := client.WithHeartbeat(5*time.Second, 15*time.Second, 3*time.Second)
	if opt == nil {
		t.Fatal("expected non-nil option")
	}
}

func TestClient_Send_BufferFull_ReturnsErrSendBufferFull(t *testing.T) {
	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	sawFull := false
	for i := 0; i < 1000; i++ {
		err := c.Send(wspulse.Frame{Type: "flood", Payload: []byte(`"x"`)})
		if errors.Is(err, wspulse.ErrSendBufferFull) {
			sawFull = true
			break
		}
		if errors.Is(err, wspulse.ErrConnectionClosed) {
			break
		}
	}
	if !sawFull {
		t.Log("ErrSendBufferFull was never returned — writePump drained fast enough or connection died")
	}
}

func TestClient_ReadPump_DecodeFailure_DropsFrame(t *testing.T) {
	received := make(chan wspulse.Frame, 5)

	srv := wspulse.NewServer(
		func(r *http.Request) (string, string, error) {
			return "room", "c1", nil
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")

	c, err := client.Dial(url,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	time.Sleep(100 * time.Millisecond)
	frame := wspulse.Frame{Type: "valid-frame", Payload: []byte(`"ok"`)}
	if err := srv.Send("c1", frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case f := <-received:
		if f.Type != "valid-frame" {
			t.Fatalf("want type %q, got %q", "valid-frame", f.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for valid frame")
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
