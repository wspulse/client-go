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

func TestClose_DiscardsBufferedFrames(t *testing.T) {
	t.Parallel()
	// Contract: close() discards unsent buffered frames. After Close(),
	// writePump must write at most 1 data frame (the one in-flight when
	// c.done fires) before stopping.
	const bufSize = 8
	mt := newMockTransport()
	fc := newFakeClock()
	mt.writeEntered = make(chan struct{}, 16)
	unblock := mt.BlockWrites()

	md := newMockDialer(mockDialResult{transport: mt})
	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
		client.WithSendBufferSize(bufSize),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		unblock()
		_ = c.Close()
	})

	// Send one frame — writePump picks it from c.send and blocks in WriteMessage.
	require.NoError(t, c.Send(wspulse.Frame{Event: "first"}))
	<-mt.writeEntered // writePump is now blocked in WriteMessage

	// Fill the remaining buffer — these frames sit in c.send.
	for i := 0; i < bufSize-1; i++ {
		require.NoError(t, c.Send(wspulse.Frame{Event: "buffered"}))
	}

	// Close in a goroutine — c.done closes immediately, then waits for goroutines.
	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()
	<-c.Done() // c.done is now closed

	// Unblock the in-flight write — writePump completes it, loops back,
	// hits the c.done priority check, sends CloseMessage, and exits.
	unblock()
	require.NoError(t, <-closeDone)

	// Count data frames written to the transport.
	writes := mt.DrainWrites()
	dataFrames := 0
	for _, w := range writes {
		if w.messageType == 1 { // TextMessage
			dataFrames++
		}
	}
	// Priority check guarantees at most 1 data frame (the in-flight one).
	// The remaining 7 frames in c.send are discarded.
	require.LessOrEqual(t, dataFrames, 1,
		"expected at most 1 data frame (in-flight), got %d", dataFrames)
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
