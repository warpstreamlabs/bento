package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type errorDriver struct {
	execErr error
}

func (d *errorDriver) Open(_ string) (driver.Conn, error) {
	return &errorConn{execErr: d.execErr}, nil
}

type errorConn struct {
	execErr error
}

func (c *errorConn) Prepare(_ string) (driver.Stmt, error) {
	return &errorStmt{execErr: c.execErr}, nil
}
func (c *errorConn) Close() error              { return nil }
func (c *errorConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not supported") }

type errorStmt struct {
	execErr error
}

func (s *errorStmt) Close() error                                 { return nil }
func (s *errorStmt) NumInput() int                                { return -1 }
func (s *errorStmt) Exec(_ []driver.Value) (driver.Result, error) { return nil, s.execErr }
func (s *errorStmt) Query(_ []driver.Value) (driver.Rows, error)  { return nil, s.execErr }

var registerErrorDriverOnce sync.Once

func newOutputWithDriver(t *testing.T, driverName string, execErr error) *sqlRawOutput {
	t.Helper()
	db, err := sql.Open(driverName, "test")
	require.NoError(t, err)
	return &sqlRawOutput{
		driver:       driverName,
		dsn:          "test",
		db:           db,
		queryStatic:  "INSERT INTO foo VALUES (1)",
		connSettings: &connSettings{},
		logger:       service.MockResources().Logger(),
		shutSig:      shutdown.NewSignaller(),
	}
}

func TestSQLRawOutputReconnectsOnPAMAuthError(t *testing.T) {
	driverName := "test_pam_err"
	registerErrorDriverOnce.Do(func() {
		sql.Register(driverName, &errorDriver{
			execErr: fmt.Errorf(`pq: PAM authentication failed for user "streaminguser"`),
		})
	})

	output := newOutputWithDriver(t, driverName, nil)

	err := output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})

	assert.Equal(t, service.ErrNotConnected, err, "PAM auth error should trigger reconnect")

	output.dbMut.RLock()
	dbIsNil := output.db == nil
	output.dbMut.RUnlock()
	assert.True(t, dbIsNil, "db must be nil after auth error so Connect() regenerates IAM token")
}
