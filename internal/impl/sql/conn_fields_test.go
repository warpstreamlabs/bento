package sql

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeSQLStateError struct {
	state string
	msg   string
}

func (e *fakeSQLStateError) Error() string    { return e.msg }
func (e *fakeSQLStateError) SQLState() string { return e.state }

func TestIsAuthError(t *testing.T) {
	tests := []struct {
		name   string
		driver string
		err    error
		want   bool
	}{
		{
			name:   "nil error",
			driver: "postgres",
			err:    nil,
			want:   false,
		},
		{
			name:   "postgres PAM auth failure",
			driver: "postgres",
			err:    &fakeSQLStateError{state: "28P01", msg: `pq: PAM authentication failed for user "streaminguser"`},
			want:   true,
		},
		{
			name:   "postgres invalid password",
			driver: "postgres",
			err:    &fakeSQLStateError{state: "28P01", msg: "pq: password authentication failed"},
			want:   true,
		},
		{
			name:   "postgres invalid authorization specification",
			driver: "postgres",
			err:    &fakeSQLStateError{state: "28000", msg: "pq: invalid authorization specification"},
			want:   true,
		},
		{
			name:   "postgres wrapped auth error",
			driver: "postgres",
			err:    fmt.Errorf("exec failed: %w", &fakeSQLStateError{state: "28P01", msg: "auth failed"}),
			want:   true,
		},
		{
			name:   "postgres non-auth sql error",
			driver: "postgres",
			err:    &fakeSQLStateError{state: "42P01", msg: "relation does not exist"},
			want:   false,
		},
		{
			name:   "postgres error without SQLState interface",
			driver: "postgres",
			err:    errors.New("connection refused"),
			want:   false,
		},
		{
			name:   "mysql access denied",
			driver: "mysql",
			err:    errors.New("Error 1045 (28000): Access denied for user 'root'@'localhost'"),
			want:   true,
		},
		{
			name:   "mysql non-auth error",
			driver: "mysql",
			err:    errors.New("Error 1146 (42S02): Table 'test.foo' doesn't exist"),
			want:   false,
		},
		{
			name:   "unsupported driver returns false",
			driver: "sqlite",
			err:    errors.New("authentication failed"),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isAuthError(tt.driver, tt.err))
		})
	}
}
