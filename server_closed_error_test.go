package client_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wspulse/client-go"
	wspulse "github.com/wspulse/core"
)

func TestServerClosedError_Error(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  *client.ServerClosedError
		want string
	}{
		{
			name: "with reason",
			err:  &client.ServerClosedError{Code: wspulse.StatusGoingAway, Reason: "server shutting down"},
			want: `wspulse: server closed connection: code=1001, reason="server shutting down"`,
		},
		{
			name: "empty reason",
			err:  &client.ServerClosedError{Code: wspulse.StatusNormalClosure, Reason: ""},
			want: "wspulse: server closed connection: code=1000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.err.Error())
		})
	}
}

func TestServerClosedError_Is_MatchesAnyServerClosedError(t *testing.T) {
	t.Parallel()
	// Is() enables errors.Is(err, &ServerClosedError{}) as a type-check
	// shortcut — matches any *ServerClosedError regardless of code or reason.
	err := &client.ServerClosedError{Code: wspulse.StatusPolicyViolation, Reason: "kicked"}

	assert.True(t, errors.Is(err, &client.ServerClosedError{}),
		"errors.Is should match any *ServerClosedError")
	assert.False(t, errors.Is(err, errors.New("other")),
		"errors.Is should not match an unrelated error")
}
