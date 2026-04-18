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
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func createSchema(t *testing.T, path, schema string) {
	t.Helper()

	db, err := sql.Open("duckdb", path)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(schema)
	require.NoError(t, err)
}

func newOutput(t *testing.T, cfg string) service.BatchOutput {
	t.Helper()

	env := service.NewEnvironment()
	config, err := isql.DuckDBAppendOutputConfig().ParseYAML(cfg, env)
	require.NoError(t, err)

	out, err := isql.NewDuckDBAppendOutputFromConfig(config, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close(context.Background()) })

	return out
}

func msg(s string) service.MessageBatch {
	return service.MessageBatch{service.NewMessage([]byte(s))}
}

func msgs(ss ...string) service.MessageBatch {
	b := make(service.MessageBatch, len(ss))
	for i, s := range ss {
		b[i] = service.NewMessage([]byte(s))
	}

	return b
}

func TestDuckDBAppend_NoConnect(t *testing.T) {
	out := newOutput(t, `
dsn: ":memory:"
table: t
columns: [id]
args_mapping: root = [this.id]
`)

	err := out.WriteBatch(context.Background(), msg(`{"id": 1}`))
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestDuckDBAppend_Write(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, name VARCHAR)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, name]
args_mapping: root = [this.id, this.name]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msgs(
		`{"id": 1, "name": "alice"}`,
		`{"id": 2, "name": "bob"}`,
	)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	rows, err := conn.Query(`SELECT id, name FROM t ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	type Row struct {
		ID   int
		Name string
	}

	var results []Row
	for rows.Next() {
		var r Row
		require.NoError(t, rows.Scan(&r.ID, &r.Name))
		results = append(results, r)
	}

	require.Len(t, results, 2)
	assert.Equal(t, Row{1, "alice"}, results[0])
	assert.Equal(t, Row{2, "bob"}, results[1])
}

func TestDuckDBAppend_Schema(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `
		CREATE SCHEMA s;
		CREATE TABLE s.t (id INTEGER);
	`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
schema: s
table: t
columns: [id]
args_mapping: root = [this.id]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 42}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var id int
	require.NoError(t, conn.QueryRow(`SELECT id FROM s.t`).Scan(&id))
	assert.Equal(t, 42, id)
}

func TestDuckDBAppend_InitStatement(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, val]
args_mapping: root = [this.id, this.val]
init_statement: CREATE TABLE t (id INTEGER, val BIGINT)
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1, "val": 999}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var id int
	var val int64
	require.NoError(t, conn.QueryRow(`SELECT id, val FROM t`).Scan(&id, &val))
	assert.Equal(t, 1, id)
	assert.Equal(t, int64(999), val)
}

func TestDuckDBAppend_SmallBatch(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id]
args_mapping: root = [this.id]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1}`)))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 2}`)))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 3}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var count int
	require.NoError(t, conn.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	assert.Equal(t, 3, count)
}

func TestDuckDBAppend_LargeBatch(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id]
args_mapping: root = [this.id]
`, db))

	require.NoError(t, out.Connect(ctx))

	const n = 10_000
	ss := make([]string, n)
	for i := range n {
		ss[i] = fmt.Sprintf(`{"id": %d}`, i)
	}
	require.NoError(t, out.WriteBatch(ctx, msgs(ss...)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var count int
	require.NoError(t, conn.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	assert.Equal(t, n, count)
}

func TestDuckDBAppend_TypeCoercion(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `
		CREATE TABLE t (
			i8 TINYINT,
			i16 SMALLINT,
			i32 INTEGER,
			i64 BIGINT,
			f DOUBLE,
			s VARCHAR,
			b BOOLEAN,
			ts TIMESTAMP,
			n VARCHAR
		)
	`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [i8, i16, i32, i64, f, s, b, ts, n]
args_mapping: root = [this.i8, this.i16, this.i32, this.i64, this.f, this.s, this.b, this.ts, this.n]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{
		"i8": 127,
		"i16": 32767,
		"i32": 2147483647,
		"i64": 9876543210,
		"f": 3.14159,
		"s": "text",
		"b": true,
		"ts": "1947-12-24T00:00:00Z",
		"n": null
	}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var f float64
	var s string
	var b bool
	var ts time.Time
	var n sql.NullString

	require.NoError(t, conn.QueryRow(`SELECT i8, i16, i32, i64, f, s, b, ts, n FROM t`).
		Scan(&i8, &i16, &i32, &i64, &f, &s, &b, &ts, &n))

	assert.Equal(t, int8(127), i8)
	assert.Equal(t, int16(32767), i16)
	assert.Equal(t, int32(2147483647), i32)
	assert.Equal(t, int64(9876543210), i64)
	assert.InDelta(t, 3.14159, f, 1e-5)
	assert.Equal(t, "text", s)
	assert.True(t, b)
	assert.Equal(t, 1947, ts.UTC().Year())
	assert.False(t, n.Valid)
}

func TestDuckDBAppend_ListType(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, items BIGINT[])`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, items]
args_mapping: root = [this.id, this.items]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1, "items": [10, 20, 30]}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var id int32
	var items []any
	require.NoError(t, conn.QueryRow(`SELECT id, items FROM t`).Scan(&id, &items))
	require.Len(t, items, 3)
	assert.Equal(t, int32(1), id)
	assert.Equal(t, int64(10), items[0])
	assert.Equal(t, int64(20), items[1])
	assert.Equal(t, int64(30), items[2])
}

func TestDuckDBAppend_EmptyList(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, items BIGINT[])`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, items]
args_mapping: root = [this.id, this.items]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1, "items": []}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var items []any
	require.NoError(t, conn.QueryRow(`SELECT items FROM t`).Scan(&items))
	assert.Len(t, items, 0)
}

func TestDuckDBAppend_Struct(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, data STRUCT(num BIGINT, str VARCHAR))`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, data]
args_mapping: root = [this.id, this.data]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1, "data": {"num": 100, "str": "hello"}}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var id int32
	var num int64
	var str string
	require.NoError(t, conn.QueryRow(`SELECT id, data.num, data.str FROM t`).Scan(&id, &num, &str))
	assert.Equal(t, int32(1), id)
	assert.Equal(t, int64(100), num)
	assert.Equal(t, "hello", str)
}

func TestDuckDBAppend_StructWithNull(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, data STRUCT(num BIGINT, str VARCHAR))`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, data]
args_mapping: root = [this.id, this.data]
`, db))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, msg(`{"id": 1, "data": {"num": null, "str": "text"}}`)))
	require.NoError(t, out.Close(ctx))

	conn := openDB(t, db)

	var id int32
	var num sql.NullInt64
	var str string
	require.NoError(t, conn.QueryRow(`SELECT id, data.num, data.str FROM t`).Scan(&id, &num, &str))
	assert.Equal(t, int32(1), id)
	assert.False(t, num.Valid)
	assert.Equal(t, "text", str)
}

func TestDuckDBAppend_ErrorMappingNotArray(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id]
args_mapping: root = this
`, db))

	require.NoError(t, out.Connect(ctx))
	err := out.WriteBatch(ctx, msg(`{"id": 1}`))
	require.Error(t, err)

	var batchErr *service.BatchError
	require.True(t, errors.As(err, &batchErr))
	assert.Contains(t, err.Error(), "non-array")
}

func TestDuckDBAppend_ErrorArgCountMismatch(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "test.duckdb")

	createSchema(t, db, `CREATE TABLE t (id INTEGER, name VARCHAR)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id, name]
args_mapping: root = [this.id]
`, db))

	require.NoError(t, out.Connect(ctx))
	err := out.WriteBatch(ctx, msg(`{"id": 1, "name": "alice"}`))
	require.Error(t, err)

	var batchErr *service.BatchError
	require.True(t, errors.As(err, &batchErr))
	assert.Contains(t, err.Error(), "values for")
}
