package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"

	"github.com/warpstreamlabs/bento/public/service"
)

type clickhouseSettingsContextKey struct{}

var capturingInsertDriverID atomic.Uint64

type capturingInsertDriver struct {
	state *capturingInsertState
}

func (d *capturingInsertDriver) Open(string) (driver.Conn, error) {
	return &capturingInsertConn{state: d.state}, nil
}

type capturingInsertState struct {
	mut         sync.Mutex
	settings    []clickhouse.Settings
	rowAppends  int
	commits     int
	commitError error
}

type capturingInsertConn struct {
	state *capturingInsertState
}

func (c *capturingInsertConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *capturingInsertConn) PrepareContext(ctx context.Context, _ string) (driver.Stmt, error) {
	settings, _ := ctx.Value(clickhouseSettingsContextKey{}).(clickhouse.Settings)
	settingsCopy := make(clickhouse.Settings, len(settings))
	maps.Copy(settingsCopy, settings)
	c.state.mut.Lock()
	c.state.settings = append(c.state.settings, settingsCopy)
	c.state.mut.Unlock()
	return &capturingInsertStmt{state: c.state}, nil
}

func (c *capturingInsertConn) Close() error { return nil }
func (c *capturingInsertConn) Begin() (driver.Tx, error) {
	return &capturingInsertTx{state: c.state}, nil
}

type capturingInsertTx struct {
	state *capturingInsertState
}

func (t *capturingInsertTx) Commit() error {
	t.state.mut.Lock()
	defer t.state.mut.Unlock()
	t.state.commits++
	err := t.state.commitError
	t.state.commitError = nil
	return err
}

func (t *capturingInsertTx) Rollback() error { return nil }

type capturingInsertStmt struct {
	state *capturingInsertState
}

func (s *capturingInsertStmt) Close() error  { return nil }
func (s *capturingInsertStmt) NumInput() int { return -1 }
func (s *capturingInsertStmt) Exec([]driver.Value) (driver.Result, error) {
	s.state.mut.Lock()
	s.state.rowAppends++
	s.state.mut.Unlock()
	return driver.RowsAffected(1), nil
}
func (s *capturingInsertStmt) Query([]driver.Value) (driver.Rows, error) {
	return nil, io.EOF
}

func TestSQLInsertOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`

	spec := sqlInsertOutputConfig()
	env := service.NewEnvironment()

	insertConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	insertOutput, err := newSQLInsertOutputFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, insertOutput.Close(context.Background()))
}

func TestSQLInsertOutputConnectNoGoroutineLeak(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
table: foo
columns: [ id ]
args_mapping: 'root = [ this.id ]'
`
	parsed, err := sqlInsertOutputConfig().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLInsertOutputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	assertNoConnectGoroutineLeak(t, o, func() {
		o.dbMut.Lock()
		if o.db != nil {
			_ = o.db.Close()
			o.db = nil
		}
		o.dbMut.Unlock()
	})
}

func TestSQLInsertOutputClickhouseSettingsBatchAndRetrySemantics(t *testing.T) {
	conf := `
driver: clickhouse
dsn: clickhouse://localhost:9000/default
table: events
columns: [ id ]
args_mapping: 'root = [ this.id ]'
clickhouse_settings:
  insert_deduplication_token: '${! uuid_v4() }'
  deduplicate_blocks_in_dependent_materialized_views: '1'
`
	parsed, err := sqlInsertOutputConfig().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)

	o, err := newSQLInsertOutputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)
	o.applyClickhouseSettings = func(ctx context.Context, settings clickhouse.Settings) context.Context {
		return context.WithValue(ctx, clickhouseSettingsContextKey{}, settings)
	}

	state := &capturingInsertState{commitError: errors.New("ambiguous commit failure")}
	driverName := fmt.Sprintf("bento_capturing_clickhouse_insert_%d", capturingInsertDriverID.Add(1))
	sql.Register(driverName, &capturingInsertDriver{state: state})
	db, err := sql.Open(driverName, "")
	require.NoError(t, err)
	o.db = db
	t.Cleanup(func() { require.NoError(t, o.Close(context.Background())) })

	batchOne := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"one"}`)),
		service.NewMessage([]byte(`{"id":"two"}`)),
	}
	batchOneCtx, err := o.prepareBatchContext(context.Background(), batchOne)
	require.NoError(t, err)

	// A failed native batch and its retry both prepare with the one resolved
	// settings context, while every record is appended to each native batch.
	require.EqualError(t, o.writeBatch(batchOneCtx, batchOne), "ambiguous commit failure")
	require.NoError(t, o.writeBatch(batchOneCtx, batchOne))

	batchTwo := service.MessageBatch{service.NewMessage([]byte(`{"id":"three"}`))}
	batchTwoCtx, err := o.prepareBatchContext(context.Background(), batchTwo)
	require.NoError(t, err)
	require.NoError(t, o.writeBatch(batchTwoCtx, batchTwo))

	state.mut.Lock()
	defer state.mut.Unlock()
	require.Len(t, state.settings, 3)
	tokenOne := state.settings[0]["insert_deduplication_token"]
	require.NotEmpty(t, tokenOne)
	require.Equal(t, tokenOne, state.settings[1]["insert_deduplication_token"])
	require.NotEqual(t, tokenOne, state.settings[2]["insert_deduplication_token"])
	require.Equal(t, "1", state.settings[0]["deduplicate_blocks_in_dependent_materialized_views"])
	require.Equal(t, 5, state.rowAppends)
	require.Equal(t, 3, state.commits)
}

func TestSQLInsertOutputRejectsClickhouseSettingsForOtherDrivers(t *testing.T) {
	conf := `
driver: sqlite
dsn: ":memory:"
table: events
columns: [ id ]
args_mapping: 'root = [ this.id ]'
clickhouse_settings:
  insert_deduplication_token: fixed
`
	parsed, err := sqlInsertOutputConfig().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)
	_, err = newSQLInsertOutputFromConfig(parsed, service.MockResources())
	require.EqualError(t, err, "clickhouse_settings can only be used with the clickhouse driver")
}
