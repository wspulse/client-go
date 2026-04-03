package client_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	wspulse "github.com/wspulse/core"

	"github.com/wspulse/client-go"
)

func TestSendAndReceive(t *testing.T) {
	t.Parallel()
	received := make(chan wspulse.Frame, 1)
	c, mt, _ := dialWithMock(t, client.WithOnMessage(func(f wspulse.Frame) {
		received <- f
	}))
	defer func() { _ = c.Close() }()

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	frame := wspulse.Frame{Event: "echo", Payload: []byte(`"hello"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case f := <-received:
		if f.Event != "echo" {
			t.Errorf("Event: want %q, got %q", "echo", f.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestClose_SafeToCallTwice(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	_ = c.Close()
}

func TestSend_AfterClose_ReturnsErrConnectionClosed(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	sendErr := c.Send(wspulse.Frame{Event: "msg"})
	if !errors.Is(sendErr, wspulse.ErrConnectionClosed) {
		t.Errorf("want ErrConnectionClosed, got %v", sendErr)
	}
}

func TestDone_ClosedAfterClose(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() channel not closed after Close()")
	}
}

func TestSend_WritesCorrectData(t *testing.T) {
	t.Parallel()
	c, mt, _ := dialWithMock(t)
	t.Cleanup(func() { _ = c.Close() })

	frame := wspulse.Frame{Event: "test", Payload: []byte(`"data"`)}
	if err := c.Send(frame); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	w, ok := mt.WaitWrite(time.Second)
	if !ok {
		t.Fatal("timed out waiting for write")
	}
	if w.messageType != 1 { // TextMessage (JSONCodec)
		t.Errorf("messageType: want 1, got %d", w.messageType)
	}

	// Decode the written data and verify.
	var wireFrame struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(w.data, &wireFrame); err != nil {
		t.Fatalf("unmarshal written data: %v", err)
	}
	if wireFrame.Event != "test" {
		t.Errorf("event: want %q, got %q", "test", wireFrame.Event)
	}
}

func TestNormalCloseFrame(t *testing.T) {
	t.Parallel()
	// When the client calls Close(), writePump should send a WebSocket close
	// frame (messageType 8) before exiting.
	c, mt, _ := dialWithMock(t)

	_ = c.Close()

	// Close() blocks until writePump exits — drain immediately.
	writes := mt.DrainWrites()

	foundClose := false
	for _, w := range writes {
		if w.messageType == 8 { // CloseMessage
			foundClose = true
			break
		}
	}
	if !foundClose {
		t.Error("Close() did not produce a WebSocket close frame (messageType=8)")
	}
}
