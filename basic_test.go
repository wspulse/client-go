package client_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
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
	require.NoError(t, c.Send(frame), "Send failed")

	f := requireReceive(t, received)
	assert.Equal(t, "echo", f.Event)
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
	assert.ErrorIs(t, sendErr, wspulse.ErrConnectionClosed)
}

func TestDone_ClosedAfterClose(t *testing.T) {
	t.Parallel()
	c, _, _ := dialWithMock(t)
	_ = c.Close()
	requireDone(t, c)
}

func TestSend_WritesCorrectData(t *testing.T) {
	t.Parallel()
	c, mt, _ := dialWithMock(t)
	t.Cleanup(func() { _ = c.Close() })

	frame := wspulse.Frame{Event: "test", Payload: []byte(`"data"`)}
	require.NoError(t, c.Send(frame), "Send failed")

	w := requireReceive(t, mt.writeCh)
	assert.Equal(t, 1, w.messageType, "messageType") // TextMessage (JSONCodec)

	// Decode the written data and verify.
	var wireFrame struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(w.data, &wireFrame), "unmarshal written data")
	assert.Equal(t, "test", wireFrame.Event)
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
	assert.True(t, foundClose, "Close() did not produce a WebSocket close frame (messageType=8)")
}
