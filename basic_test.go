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
	received := make(chan wspulse.Message, 1)
	c, mt, _ := dialWithMock(t, client.WithOnMessage(func(f wspulse.Message) {
		received <- f
	}))
	defer func() { _ = c.Close() }()

	// Start echo loop.
	echoDone := make(chan struct{})
	defer close(echoDone)
	go echoLoop(mt, echoDone)

	msg := wspulse.Message{Event: "echo", Payload: []byte(`"hello"`)}
	require.NoError(t, c.Send(msg), "Send failed")

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
	sendErr := c.Send(wspulse.Message{Event: "msg"})
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

	msg := wspulse.Message{Event: "test", Payload: []byte(`"data"`)}
	require.NoError(t, c.Send(msg), "Send failed")

	w := requireReceive(t, mt.writeCh)
	assert.Equal(t, wspulse.TextMessage, w.messageType, "messageType")

	// Decode the written data and verify.
	var wireMsg struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(w.data, &wireMsg), "unmarshal written data")
	assert.Equal(t, "test", wireMsg.Event)
}

func TestClose_DiscardsBufferedMessages(t *testing.T) {
	t.Parallel()
	// Contract: close() discards unsent buffered messages. After Close(),
	// writePump must write at most 1 data message (the one in-flight when
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

	// Send one message — writePump picks it from c.send and blocks in Write.
	require.NoError(t, c.Send(wspulse.Message{Event: "first"}))
	<-mt.writeEntered // writePump is now blocked in Write

	// Fill the remaining buffer — these messages sit in c.send.
	for i := 0; i < bufSize-1; i++ {
		require.NoError(t, c.Send(wspulse.Message{Event: "buffered"}))
	}

	// Close in a goroutine — c.done closes immediately, then waits for goroutines.
	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()
	<-c.Done() // c.done is now closed

	// Unblock the in-flight write — writePump completes it, loops back,
	// hits the c.done priority check, and exits.
	unblock()
	require.NoError(t, <-closeDone)

	// Count data messages written to the transport.
	writes := mt.DrainWrites()
	dataMessages := 0
	for _, w := range writes {
		if w.messageType == wspulse.TextMessage {
			dataMessages++
		}
	}
	// Priority check guarantees at most 1 data message (the in-flight one).
	// The remaining 7 messages in c.send are discarded.
	require.LessOrEqual(t, dataMessages, 1,
		"expected at most 1 data message (in-flight), got %d", dataMessages)
}

func TestNormalCloseFrame(t *testing.T) {
	t.Parallel()
	// When the client calls Close(), writePump should send a WebSocket close frame
	// with StatusNormalClosure before exiting.
	mt := newMockTransport()
	mt.closeCalled = make(chan closeCall, 1)
	fc := newFakeClock()
	md := newMockDialer(mockDialResult{transport: mt})

	c, err := client.Dial("ws://mock",
		client.WithDialer(md),
		client.WithClock(fc),
	)
	require.NoError(t, err)

	_ = c.Close()

	// Close() blocks until writePump exits — check immediately.
	cc := requireReceive(t, mt.closeCalled)
	assert.Equal(t, wspulse.StatusNormalClosure, cc.code,
		"Close() did not produce a close frame with StatusNormalClosure")
}
