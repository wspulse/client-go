package client_test

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestClose_WaitsForDisconnectCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

func TestClose_WaitsForTransportDropCallback(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithOnTransportDrop(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onTransportDrop callback finished — orphaned callback goroutine")
	}
}

func TestClose_WaitsForDisconnectCallback_AutoReconnect(t *testing.T) {
	t.Parallel()
	var callbackDone atomic.Bool
	c, _, _ := dialWithMock(t,
		client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		client.WithOnDisconnect(func(err error) {
			time.Sleep(200 * time.Millisecond)
			callbackDone.Store(true)
		}),
	)
	_ = c.Close()
	if !callbackDone.Load() {
		t.Fatal("Close() returned before onDisconnect callback finished — orphaned callback goroutine")
	}
}

func TestClose_WaitsForGoroutines(t *testing.T) {
	t.Parallel()
	const count = 20
	type entry struct {
		c  client.Client
		mt *mockTransport
	}
	entries := make([]entry, count)
	for i := range entries {
		mt := newMockTransport()
		fc := newFakeClock()
		md := newMockDialer(mockDialResult{transport: mt})
		c, err := client.Dial("ws://mock",
			client.WithDialer(md),
			client.WithClock(fc),
		)
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for i, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-e.c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
	}
}

func TestClose_WaitsForGoroutines_AutoReconnect(t *testing.T) {
	t.Parallel()
	const count = 20
	type entry struct {
		c  client.Client
		mt *mockTransport
	}
	entries := make([]entry, count)
	for i := range entries {
		mt := newMockTransport()
		fc := newFakeClock()
		md := newMockDialer(mockDialResult{transport: mt})
		c, err := client.Dial("ws://mock",
			client.WithDialer(md),
			client.WithClock(fc),
			client.WithAutoReconnect(3, 100*time.Millisecond, 500*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("Dial #%d failed: %v", i, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		entries[i] = entry{c: c, mt: mt}
	}

	for i, e := range entries {
		closeDone := make(chan struct{})
		go func() {
			_ = e.c.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Close() did not return within timeout", i)
		}
		select {
		case <-e.c.Done():
		case <-time.After(time.Second):
			t.Fatalf("Client #%d: Done() not closed after Close()", i)
		}
	}
}

func TestDone_FiresOnServerDrop(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 1)
	c, mt, _ := dialWithMock(t,
		client.WithOnMessage(func(f wspulse.Frame) {
			received <- f
		}),
	)
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop.
	echoDone := make(chan struct{})
	go echoLoop(mt, echoDone)

	// Confirm the connection is established by round-tripping a frame.
	if err := c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	// Stop echo and simulate server drop.
	close(echoDone)
	mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})

	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out: Done() did not fire after server disconnect")
	}

	if err := c.Send(wspulse.Frame{Event: "msg"}); !errors.Is(err, wspulse.ErrConnectionClosed) {
		t.Fatalf("want ErrConnectionClosed, got %v", err)
	}
}

func TestConcurrentSendAndClose_NoRace(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)

	const senders = 8
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = c.Send(wspulse.Frame{Event: "msg", Payload: []byte(`"x"`)})
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	_ = c.Close()
	wg.Wait()
}

func TestConcurrentCloseAndTransportDrop_OnDisconnectExactlyOnce(t *testing.T) {
	t.Parallel()
	mt := newMockTransport()
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	var disconnectCount atomic.Int32
	disconnectDone := make(chan struct{}, 1)
	received := make(chan struct{}, 1)

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithOnDisconnect(func(err error) {
			disconnectCount.Add(1)
			select {
			case disconnectDone <- struct{}{}:
			default:
			}
		}),
		client.WithOnMessage(func(f wspulse.Frame) {
			select {
			case received <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	// Confirm the connection is established by round-tripping a frame.
	if err := c.Send(wspulse.Frame{Event: "ping", Payload: []byte(`"1"`)}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	// Simultaneously close the client and drop the transport.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Close()
	}()
	go func() {
		defer wg.Done()
		mt.InjectError(&net.OpError{Op: "read", Err: errors.New("connection reset")})
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client and transport close to complete")
	}

	// Wait for onDisconnect to fire.
	select {
	case <-disconnectDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onDisconnect")
	}

	// Give a short settling time.
	time.Sleep(50 * time.Millisecond)

	if count := disconnectCount.Load(); count != 1 {
		t.Errorf("onDisconnect fired %d times, want exactly 1", count)
	}
}
