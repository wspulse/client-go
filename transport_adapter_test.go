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
)

// TestCoderTransport_Read_WrapsCloseFrameAsErrServerClosed verifies that
// coderTransport.Read wraps any WebSocket close frame into ErrServerClosed.
// This ensures callers do not need to import coder/websocket to classify the
// error — errors.Is(err, ErrServerClosed) is sufficient.
func TestCoderTransport_Read_WrapsCloseFrameAsErrServerClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		code websocket.StatusCode
	}{
		{"normal closure (1000)", websocket.StatusNormalClosure},
		{"policy violation (1008)", websocket.StatusPolicyViolation},
		{"going away (1001)", websocket.StatusGoingAway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				require.NoError(t, err)
				_ = conn.Close(tc.code, "test close")
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
			assert.ErrorIs(t, readErr, ErrServerClosed,
				"want ErrServerClosed for close code %d, got: %v", tc.code, readErr)
		})
	}
}
