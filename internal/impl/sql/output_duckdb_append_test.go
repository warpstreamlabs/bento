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

func batch(msgs ...string) service.MessageBatch {
	b := make(service.MessageBatch, len(msgs))
	for i, s := range msgs {
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

	err := out.WriteBatch(context.Background(), batch(`{"id": 1}`))
	assert.ErrorIs(t, err, service.ErrNotConnected)
}

func TestDuckDBAppend_Write(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		config     string
		messages   []string
		verifyFunc func(*testing.T, *sql.DB)
	}{
		{
			name:   "basic write two rows",
			schema: `CREATE TABLE t (id INTEGER, name VARCHAR)`,
			config: `
dsn: %s
table: t
columns: [id, name]
args_mapping: root = [this.id, this.name]
`,
			messages: []string{`{"id": 1, "name": "alice"}`, `{"id": 2, "name": "bob"}`},
			verifyFunc: func(t *testing.T, db *sql.DB) {
				rows, err := db.Query(`SELECT id, name FROM t ORDER BY id`)
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
			},
		},
		{
			name: "schema qualified table",
			schema: `
CREATE SCHEMA s;
CREATE TABLE s.t (id INTEGER);
`,
			config: `
dsn: %s
schema: s
table: t
columns: [id]
args_mapping: root = [this.id]
`,
			messages: []string{`{"id": 1}`},
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var id int
				require.NoError(t, db.QueryRow(`SELECT id FROM s.t`).Scan(&id))
				assert.Equal(t, 1, id)
			},
		},
		{
			name: "init statement creates table",
			config: `
dsn: %s
table: t
columns: [id, val]
args_mapping: root = [this.id, this.val]
init_statement: CREATE TABLE t (id INTEGER, val BIGINT)
`,
			messages: []string{`{"id": 1, "val": 999}`},
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var id int
				var val int64
				require.NoError(t, db.QueryRow(`SELECT id, val FROM t`).Scan(&id, &val))
				assert.Equal(t, 1, id)
				assert.Equal(t, int64(999), val)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "test.duckdb")

			if tc.schema != "" {
				createSchema(t, dbPath, tc.schema)
			}

			out := newOutput(t, fmt.Sprintf(tc.config, dbPath))
			require.NoError(t, out.Connect(ctx))
			require.NoError(t, out.WriteBatch(ctx, batch(tc.messages...)))
			require.NoError(t, out.Close(ctx))

			tc.verifyFunc(t, openDB(t, dbPath))
		})
	}
}

func TestDuckDBAppend_SmallBatch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.duckdb")
	createSchema(t, dbPath, `CREATE TABLE t (id INTEGER)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id]
args_mapping: root = [this.id]
`, dbPath))

	require.NoError(t, out.Connect(ctx))
	require.NoError(t, out.WriteBatch(ctx, batch(`{"id": 1}`)))
	require.NoError(t, out.WriteBatch(ctx, batch(`{"id": 2}`)))
	require.NoError(t, out.WriteBatch(ctx, batch(`{"id": 3}`)))
	require.NoError(t, out.Close(ctx))

	var count int
	require.NoError(t, openDB(t, dbPath).QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	assert.Equal(t, 3, count)
}

func TestDuckDBAppend_LargeBatch(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.duckdb")
	createSchema(t, dbPath, `CREATE TABLE t (id INTEGER)`)

	out := newOutput(t, fmt.Sprintf(`
dsn: %s
table: t
columns: [id]
args_mapping: root = [this.id]
`, dbPath))

	require.NoError(t, out.Connect(ctx))

	const n = 10_000
	ss := make([]string, n)
	for i := range n {
		ss[i] = fmt.Sprintf(`{"id": %d}`, i)
	}
	require.NoError(t, out.WriteBatch(ctx, batch(ss...)))
	require.NoError(t, out.Close(ctx))

	var count int
	require.NoError(t, openDB(t, dbPath).QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count))
	assert.Equal(t, n, count)
}

func TestDuckDBAppend_Types(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		config     string
		message    string
		verifyFunc func(*testing.T, *sql.DB)
	}{
		{
			name: "type coercion",
			schema: `
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
`,
			config: `
dsn: %s
table: t
columns: [i8, i16, i32, i64, f, s, b, ts, n]
args_mapping: root = [this.i8, this.i16, this.i32, this.i64, this.f, this.s, this.b, this.ts, this.n]
`,
			message: `{
	"i8": 127,
	"i16": 32767,
	"i32": 2147483647,
	"i64": 9876543210,
	"f": 3.14159,
	"s": "text",
	"b": true,
	"ts": "1947-12-24T00:00:00Z",
	"n": null
}`,
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var i8 int8
				var i16 int16
				var i32 int32
				var i64 int64
				var f float64
				var s string
				var b bool
				var ts time.Time
				var n sql.NullString

				require.NoError(t, db.QueryRow(`SELECT i8, i16, i32, i64, f, s, b, ts, n FROM t`).
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
			},
		},
		{
			name:   "list type",
			schema: `CREATE TABLE t (id INTEGER, items BIGINT[])`,
			config: `
dsn: %s
table: t
columns: [id, items]
args_mapping: root = [this.id, this.items]
`,
			message: `{"id": 1, "items": [10, 20, 30]}`,
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var items []any
				require.NoError(t, db.QueryRow(`SELECT items FROM t`).Scan(&items))
				require.Len(t, items, 3)
				assert.Equal(t, int64(10), items[0])
				assert.Equal(t, int64(20), items[1])
				assert.Equal(t, int64(30), items[2])
			},
		},
		{
			name:   "empty list",
			schema: `CREATE TABLE t (id INTEGER, items BIGINT[])`,
			config: `
dsn: %s
table: t
columns: [id, items]
args_mapping: root = [this.id, this.items]
`,
			message: `{"id": 1, "items": []}`,
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var items []any
				require.NoError(t, db.QueryRow(`SELECT items FROM t`).Scan(&items))
				assert.Len(t, items, 0)
			},
		},
		{
			name:   "struct type",
			schema: `CREATE TABLE t (id INTEGER, data STRUCT(num BIGINT, str VARCHAR))`,
			config: `
dsn: %s
table: t
columns: [id, data]
args_mapping: root = [this.id, this.data]
`,
			message: `{"id": 1, "data": {"num": 100, "str": "hello"}}`,
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var num int64
				var str string
				require.NoError(t, db.QueryRow(`SELECT data.num, data.str FROM t`).Scan(&num, &str))
				assert.Equal(t, int64(100), num)
				assert.Equal(t, "hello", str)
			},
		},
		{
			name:   "struct with null field",
			schema: `CREATE TABLE t (id INTEGER, data STRUCT(num BIGINT, str VARCHAR))`,
			config: `
dsn: %s
table: t
columns: [id, data]
args_mapping: root = [this.id, this.data]
`,
			message: `{"id": 1, "data": {"num": null, "str": "text"}}`,
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var num sql.NullInt64
				var str string
				require.NoError(t, db.QueryRow(`SELECT data.num, data.str FROM t`).Scan(&num, &str))
				assert.False(t, num.Valid)
				assert.Equal(t, "text", str)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "test.duckdb")

			createSchema(t, dbPath, tc.schema)

			out := newOutput(t, fmt.Sprintf(tc.config, dbPath))
			require.NoError(t, out.Connect(ctx))
			require.NoError(t, out.WriteBatch(ctx, batch(tc.message)))
			require.NoError(t, out.Close(ctx))

			tc.verifyFunc(t, openDB(t, dbPath))
		})
	}
}

func TestDuckDBAppend_Errors(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		config      string
		message     string
		expectedErr string
	}{
		{
			name:   "mapping not array",
			schema: `CREATE TABLE t (id INTEGER)`,
			config: `
dsn: %s
table: t
columns: [id]
args_mapping: root = this
`,
			message:     `{"id": 1}`,
			expectedErr: "non-array",
		},
		{
			name:   "arg count mismatch",
			schema: `CREATE TABLE t (id INTEGER, name VARCHAR)`,
			config: `
dsn: %s
table: t
columns: [id, name]
args_mapping: root = [this.id]
`,
			message:     `{"id": 1}`,
			expectedErr: "values for", // x "values for" y columns
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "test.duckdb")

			createSchema(t, dbPath, tc.schema)

			out := newOutput(t, fmt.Sprintf(tc.config, dbPath))
			require.NoError(t, out.Connect(ctx))

			err := out.WriteBatch(ctx, batch(tc.message))
			require.Error(t, err)

			var batchErr *service.BatchError
			require.True(t, errors.As(err, &batchErr))
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}
