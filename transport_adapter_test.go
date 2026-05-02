package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wspulse "github.com/wspulse/core"
)

// TestCoderTransport_Read_WrapsCloseFrameAsServerClosedError verifies that
// coderTransport.Read wraps any WebSocket close frame into *ServerClosedError
// carrying the exact code and reason sent by the server. Callers extract
// them via errors.As without importing coder/websocket.
func TestCoderTransport_Read_WrapsCloseFrameAsServerClosedError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       websocket.StatusCode
		reason     string
		wantStatus wspulse.StatusCode
	}{
		{"normal closure (1000)", websocket.StatusNormalClosure, "bye", wspulse.StatusNormalClosure},
		{"policy violation (1008)", websocket.StatusPolicyViolation, "nope", wspulse.StatusPolicyViolation},
		{"going away (1001)", websocket.StatusGoingAway, "server shutting down", wspulse.StatusGoingAway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Errorf("websocket.Accept: %v", err)
					return
				}
				_ = conn.Close(tc.code, tc.reason)
			}))
			t.Cleanup(srv.Close)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.Cleanup(cancel)

			url := "ws" + strings.TrimPrefix(srv.URL, "http")
			conn, _, err := websocket.Dial(ctx, url, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.CloseNow() })

			transport := &coderTransport{conn: conn}
			_, _, readErr := transport.Read(ctx)

			require.Error(t, readErr)
			var sce *ServerClosedError
			require.ErrorAs(t, readErr, &sce,
				"want *ServerClosedError for close code %d, got: %v", tc.code, readErr)
			assert.Equal(t, tc.wantStatus, sce.Code, "close code mismatch")
			assert.Equal(t, tc.reason, sce.Reason, "close reason mismatch")
		})
	}
}
