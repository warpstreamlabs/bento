//go:build x_bento_extra

package sql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	isql "github.com/warpstreamlabs/bento/internal/impl/sql"
	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/sql"
)

func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func createSchema(t *testing.T, path, query string) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	require.NoError(t, err)

	_, err = db.Exec(query)
	require.NoError(t, err)

	require.NoError(t, db.Close())
}

func newOutput(t *testing.T, yaml string) service.BatchOutput {
	t.Helper()

	env := service.NewEnvironment()

	config, err := isql.DuckDBAppendOutputConfig().ParseYAML(yaml, env)
	require.NoError(t, err)

	out, err := isql.NewDuckDBAppendOutputFromConfig(config, service.MockResources())
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = out.Close(context.Background())
	})

	return out
}

func batch(msgs ...string) service.MessageBatch {
	batch := make(service.MessageBatch, len(msgs))
	for i, m := range msgs {
		batch[i] = service.NewMessage([]byte(m))
	}
	return batch
}

const vaultDepositsSchema = `
	CREATE TABLE vault_deposits (
		deposit_id   VARCHAR   PRIMARY KEY,
		duck         VARCHAR   NOT NULL,
		item         VARCHAR   NOT NULL,
		quantity     BIGINT    NOT NULL,
		deposited_at TIMESTAMP NOT NULL
	)
`

func TestDuckDBAppend_EmptyClose(t *testing.T) {
	out := newOutput(t, `
dsn: /tmp/nonexistent.duckdb
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: 'root = [this.deposit_id, this.duck, this.item, this.quantity, this.deposited_at]'
`)
	require.NoError(t, out.Close(context.Background()))
}

func TestDuckDBAppend_BasicWrite(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "basic.duckdb")

	createSchema(t, dbFile, vaultDepositsSchema)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-001","duck":"Scrooge McDuck","item":"Gold Coin","quantity":9999,"deposited_at":"2024-01-01T00:00:00Z"}`,
		`{"deposit_id":"dep-002","duck":"Donald Duck","item":"Magic Dime","quantity":1,"deposited_at":"2024-01-02T12:00:00Z"}`,
		`{"deposit_id":"dep-003","duck":"Daffy Duck","item":"Silver Talisman","quantity":42,"deposited_at":"2024-01-03T08:30:00Z"}`,
	)))
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	rows, err := db.Query(`SELECT deposit_id, duck, item, quantity FROM vault_deposits ORDER BY deposit_id`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		depositID string
		duck      string
		item      string
		quantity  int64
	}

	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.depositID, &r.duck, &r.item, &r.quantity))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, got, 3)

	assert.Equal(t, row{"dep-001", "Scrooge McDuck", "Gold Coin", 9999}, got[0])
	assert.Equal(t, row{"dep-002", "Donald Duck", "Magic Dime", 1}, got[1])
	assert.Equal(t, row{"dep-003", "Daffy Duck", "Silver Talisman", 42}, got[2])
}

func TestDuckDBAppend_InitStatement(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "init_stmt.duckdb")

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
init_statement: |
  CREATE TABLE IF NOT EXISTS vault_deposits (
    deposit_id   VARCHAR   PRIMARY KEY,
    duck         VARCHAR   NOT NULL,
    item         VARCHAR   NOT NULL,
    quantity     BIGINT    NOT NULL,
    deposited_at TIMESTAMP NOT NULL
  )
`, dbFile))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-001","duck":"Howard the Duck","item":"Bottle of Gin","quantity":7,"deposited_at":"2024-06-01T00:00:00Z"}`,
	)))
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var depositID, duck string
	require.NoError(t, db.QueryRow(`SELECT deposit_id, duck FROM vault_deposits`).Scan(&depositID, &duck))
	assert.Equal(t, "dep-001", depositID)
	assert.Equal(t, "Howard the Duck", duck)
}

func TestDuckDBAppend_MultipleBatches(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "multi.duckdb")

	createSchema(t, dbFile, vaultDepositsSchema)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))

	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-001","duck":"Scrooge McDuck","item":"Gold Coin","quantity":100,"deposited_at":"2024-01-01T00:00:00Z"}`,
		`{"deposit_id":"dep-002","duck":"Scrooge McDuck","item":"Gold Coin","quantity":200,"deposited_at":"2024-01-02T00:00:00Z"}`,
	)))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-003","duck":"Donald Duck","item":"Magic Dime","quantity":1,"deposited_at":"2024-01-03T00:00:00Z"}`,
	)))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-004","duck":"Daffy Duck","item":"Silver Talisman","quantity":50,"deposited_at":"2024-01-04T00:00:00Z"}`,
		`{"deposit_id":"dep-005","duck":"Psyduck","item":"Can of Corn","quantity":3,"deposited_at":"2024-01-05T00:00:00Z"}`,
	)))

	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM vault_deposits`).Scan(&count))
	assert.Equal(t, 5, count)
}

func TestDuckDBAppend_LargeBatch(t *testing.T) {
	const rowCount = 10_000
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "large.duckdb")

	createSchema(t, dbFile, vaultDepositsSchema)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))

	msgs := make([]string, rowCount)
	for i := range rowCount {
		msgs[i] = fmt.Sprintf(
			`{"deposit_id":"dep-%06d","duck":"Scrooge McDuck","item":"Gold Coin","quantity":%d,"deposited_at":"2024-01-01T00:00:00Z"}`,
			i, i+1,
		)
	}
	require.NoError(t, out.WriteBatch(ctx, batch(msgs...)))
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM vault_deposits`).Scan(&count))
	assert.Equal(t, rowCount, count)
}

func TestDuckDBAppend_CloseFlushesRows(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "flush.duckdb")

	createSchema(t, dbFile, vaultDepositsSchema)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-001","duck":"Scrooge McDuck","item":"Gold Coin","quantity":1,"deposited_at":"2024-01-01T00:00:00Z"}`,
		`{"deposit_id":"dep-002","duck":"Donald Duck","item":"Magic Dime","quantity":2,"deposited_at":"2024-01-02T00:00:00Z"}`,
		`{"deposit_id":"dep-003","duck":"Daffy Duck","item":"Silver Talisman","quantity":3,"deposited_at":"2024-01-03T00:00:00Z"}`,
	)))

	// Close() should flush without explicit call
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM vault_deposits`).Scan(&count))
	assert.Equal(t, 3, count)
}

func TestDuckDBAppend_SchemaField(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "schema.duckdb")

	createSchema(t, dbFile, `
		CREATE SCHEMA money_bin;
		CREATE TABLE money_bin.vault_deposits (
			deposit_id   VARCHAR   PRIMARY KEY,
			duck         VARCHAR   NOT NULL,
			item         VARCHAR   NOT NULL,
			quantity     BIGINT    NOT NULL,
			deposited_at TIMESTAMP NOT NULL
		);
	`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
schema: money_bin
table: vault_deposits
columns: [deposit_id, duck, item, quantity, deposited_at]
args_mapping: |
  root = [
    this.deposit_id,
    this.duck,
    this.item,
    this.quantity,
    this.deposited_at,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(
		`{"deposit_id":"dep-001","duck":"Scrooge McDuck","item":"Ancient Artifact","quantity":1,"deposited_at":"2024-01-01T00:00:00Z"}`,
	)))
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var depositID, duck string
	require.NoError(t, db.QueryRow(`SELECT deposit_id, duck FROM money_bin.vault_deposits`).Scan(&depositID, &duck))
	assert.Equal(t, "dep-001", depositID)
	assert.Equal(t, "Scrooge McDuck", duck)
}

func TestDuckDBAppend_TypeMapping(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "types.duckdb")

	createSchema(t, dbFile, `
		CREATE TABLE type_test (
			col_tinyint   TINYINT,
			col_smallint  SMALLINT,
			col_int       INTEGER,
			col_bigint    BIGINT,
			col_double    DOUBLE,
			col_varchar   VARCHAR,
			col_bool      BOOLEAN,
			col_ts        TIMESTAMP,
			col_nullable  VARCHAR
		)
	`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: type_test
columns: [col_tinyint, col_smallint, col_int, col_bigint, col_double, col_varchar, col_bool, col_ts, col_nullable]
args_mapping: |
  root = [
    this.col_tinyint,
    this.col_smallint,
    this.col_int,
    this.col_bigint,
    this.col_double,
    this.col_varchar,
    this.col_bool,
    this.col_ts,
    this.col_nullable,
  ]
`, dbFile))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(`{
		"col_tinyint":  127,
		"col_smallint": 32767,
		"col_int":      2147483647,
		"col_bigint":   9876543210,
		"col_double":   3.141592653589793,
		"col_varchar":  "Scrooge McDuck",
		"col_bool":     true,
		"col_ts":       "1947-12-24T00:00:00Z",
		"col_nullable": null
	}`)))
	require.NoError(t, out.Close(ctx))

	db := openDB(t, dbFile)

	var (
		colTinyint  int8
		colSmallint int16
		colInt      int32
		colBigint   int64
		colDouble   float64
		colVarchar  string
		colBool     bool
		colTs       time.Time
		colNullable sql.NullString
	)
	err := db.QueryRow(`
		SELECT col_tinyint, col_smallint, col_int, col_bigint, col_double,
		       col_varchar, col_bool, col_ts, col_nullable
		FROM type_test
	`).Scan(&colTinyint, &colSmallint, &colInt, &colBigint, &colDouble,
		&colVarchar, &colBool, &colTs, &colNullable)
	require.NoError(t, err)

	assert.Equal(t, int8(127), colTinyint)
	assert.Equal(t, int16(32767), colSmallint)
	assert.Equal(t, int32(2147483647), colInt)
	assert.Equal(t, int64(9876543210), colBigint)
	assert.InDelta(t, 3.141592653589793, colDouble, 1e-10)
	assert.Equal(t, "Scrooge McDuck", colVarchar)
	assert.True(t, colBool)
	assert.Equal(t, 1947, colTs.UTC().Year())
	assert.Equal(t, time.December, colTs.UTC().Month())
	assert.Equal(t, 24, colTs.UTC().Day())
	assert.False(t, colNullable.Valid, "null JSON value must produce SQL NULL")
}

func TestDuckDBAppend_ArgsNotArray(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "bad_mapping.duckdb")

	createSchema(t, dbFile, `CREATE TABLE vault_deposits (deposit_id VARCHAR PRIMARY KEY)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id]
args_mapping: 'root = this'
`, dbFile))

	require.NoError(t, out.Connect(ctx))

	err := out.WriteBatch(ctx, batch(`{"deposit_id":"dep-001"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "array")

	var batchErr *service.BatchError
	require.True(t, errors.As(err, &batchErr), "expected BatchError")
	assert.Contains(t, batchErr.Error(), "non-array")
}

func TestDuckDBAppend_ArgCountMismatch(t *testing.T) {
	ctx := context.Background()
	dbFile := filepath.Join(t.TempDir(), "mismatch.duckdb")

	createSchema(t, dbFile, `
		CREATE TABLE vault_deposits (
			deposit_id VARCHAR PRIMARY KEY,
			duck       VARCHAR NOT NULL
		)
	`)

	// args_mapping returns 1 value but columns declares 2.
	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id, duck]
args_mapping: 'root = [this.deposit_id]'
`, dbFile))

	require.NoError(t, out.Connect(ctx))

	err := out.WriteBatch(ctx, batch(`{"deposit_id":"dep-001","duck":"Scrooge McDuck"}`))
	require.Error(t, err)

	var batchErr *service.BatchError
	require.True(t, errors.As(err, &batchErr), "expected BatchError")
	assert.Contains(t, batchErr.Error(), "values for")
}

func TestDuckDBAppend_ConnectNotConnected(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "not_connected.duckdb")

	createSchema(t, dbFile, `CREATE TABLE vault_deposits (deposit_id VARCHAR PRIMARY KEY)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: vault_deposits
columns: [deposit_id]
args_mapping: 'root = [this.deposit_id]'
`, dbFile))

	err := out.WriteBatch(context.Background(), batch(`{"deposit_id":"dep-001"}`))
	assert.ErrorIs(t, err, service.ErrNotConnected)
}
