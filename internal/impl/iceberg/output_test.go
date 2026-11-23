package iceberg

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/public/service"
)

// memCatalog is a test catalog: table entries in memory, metadata and data
// files on the local filesystem. Commits validate requirements like a real
// catalog, so concurrent writers conflict and retry.
type memCatalog struct {
	mu         sync.Mutex
	namespaces map[string]bool
	tables     map[string]*memTable
	// failCommit, if set, fails every commit with this error.
	failCommit error
}

type memTable struct {
	meta    table.Metadata
	loc     string
	version int
}

var _ table.CatalogIO = (*memCatalog)(nil)

func newMemCatalog() *memCatalog {
	return &memCatalog{namespaces: map[string]bool{}, tables: map[string]*memTable{}}
}

func memKey(ident table.Identifier) string { return strings.Join(ident, ".") }

func localFS(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil }

func (c *memCatalog) CheckNamespaceExists(_ context.Context, ns table.Identifier) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.namespaces[memKey(ns)], nil
}

func (c *memCatalog) CreateNamespace(_ context.Context, ns table.Identifier, _ iceberg.Properties) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.namespaces[memKey(ns)] {
		return catalog.ErrNamespaceAlreadyExists
	}
	c.namespaces[memKey(ns)] = true
	return nil
}

func (c *memCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tables[memKey(ident)]
	if !ok {
		return nil, catalog.ErrNoSuchTable
	}
	return table.New(ident, t.meta, t.loc, localFS, c), nil
}

// writeMetadata writes meta to loc, as a real catalog does on every commit.
func writeMetadata(meta table.Metadata, loc string) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return icebergio.LocalFS{}.WriteFile(loc, b)
}

func (c *memCatalog) RegisterTable(ctx context.Context, ident table.Identifier, loc string) (*table.Table, error) {
	tbl, err := table.NewFromLocation(ctx, ident, loc, localFS, c)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.namespaces[ident[0]] {
		return nil, catalog.ErrNoSuchNamespace
	}
	if _, ok := c.tables[memKey(ident)]; ok {
		return nil, catalog.ErrTableAlreadyExists
	}
	// Continue version numbering after the registered file's.
	version := 1
	for range tbl.Metadata().PreviousFiles() {
		version++
	}
	c.tables[memKey(ident)] = &memTable{meta: tbl.Metadata(), loc: loc, version: version}
	return tbl, nil
}

// forget drops the catalog entries (table and namespace) but keeps every
// file, like a Glue database deleted by mistake.
func (c *memCatalog) forget(ident table.Identifier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tables, memKey(ident))
	delete(c.namespaces, ident[0])
}

func (c *memCatalog) CreateTable(_ context.Context, ident table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	var cfg catalog.CreateTableCfg
	for _, o := range opts {
		o(&cfg)
	}
	meta, err := table.NewMetadata(schema, cfg.PartitionSpec, cfg.SortOrder, cfg.Location, cfg.Properties)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.namespaces[ident[0]] {
		return nil, catalog.ErrNoSuchNamespace
	}
	if _, ok := c.tables[memKey(ident)]; ok {
		return nil, catalog.ErrTableAlreadyExists
	}
	t := &memTable{meta: meta, loc: cfg.Location + "/metadata/00000-create.metadata.json"}
	if err := writeMetadata(meta, t.loc); err != nil {
		return nil, err
	}
	c.tables[memKey(ident)] = t
	return table.New(ident, meta, t.loc, localFS, c), nil
}

func (c *memCatalog) CommitTable(_ context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failCommit != nil {
		return nil, "", c.failCommit
	}
	t, ok := c.tables[memKey(ident)]
	if !ok {
		return nil, "", catalog.ErrNoSuchTable
	}
	for _, r := range reqs {
		if err := r.Validate(t.meta); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	updated, err := table.UpdateTableMetadata(t.meta, updates, t.loc)
	if err != nil {
		return nil, "", err
	}
	t.version++
	loc := fmt.Sprintf("%s/metadata/%05d-commit.metadata.json", updated.Location(), t.version)
	if err := writeMetadata(updated, loc); err != nil {
		return nil, "", err
	}
	t.meta, t.loc = updated, loc
	return updated, t.loc, nil
}

// testTableYAML creates a table with a column of every supported type.
const testTableYAML = `
partition_by:
  - day(created_at)
schema:
  - { name: id, type: string }
  - { name: created_at, type: timestamptz }
  - { name: count, type: int }
  - { name: total, type: long }
  - { name: score, type: double }
  - { name: ratio, type: float }
  - { name: active, type: boolean }
  - { name: born, type: date }
  - { name: local_time, type: timestamp }
  - { name: payload, type: string }
`

func testRow(id string) map[string]any {
	return map[string]any{
		"id":         id,
		"created_at": "2026-11-20T14:03:00.123456Z",
		"count":      json.Number("3"),
		"total":      int64(1) << 40,
		"score":      1.5,
		"ratio":      0.25,
		"active":     true,
		"born":       "1990-05-17",
		"local_time": "2026-11-20T09:30:00",
		"payload":    map[string]any{"k": []any{"a", "b"}},
	}
}

func testMessage(id string) *service.Message {
	msg := service.NewMessage(nil)
	msg.SetStructured(testRow(id))
	return msg
}

// newTestOutput returns a connected output on cat. tableYAML holds the fields
// used to create the table, if any.
func newTestOutput(t *testing.T, cat *memCatalog, warehouse, tableYAML string) *icebergOutput {
	t.Helper()
	out := newUnconnectedTestOutput(t, cat, warehouse, tableYAML)
	require.NoError(t, out.Connect(context.Background()))
	return out
}

func newUnconnectedTestOutput(t *testing.T, cat *memCatalog, warehouse, tableYAML string) *icebergOutput {
	t.Helper()
	conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
catalog:
  type: glue
warehouse: %s
namespace: acme
table: events
%s`, warehouse, tableYAML), nil)
	require.NoError(t, err)
	out, err := newOutputFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	out.newCatalog = func(context.Context) (icebergConn, error) {
		return icebergConn{catalog: cat}, nil
	}
	return out
}

func testWarehouse(t *testing.T) string {
	return "file://" + filepath.ToSlash(t.TempDir())
}

func tableDir(warehouse string) string {
	return filepath.Join(strings.TrimPrefix(warehouse, "file://"), "acme", "events")
}

func TestSchemaFromConfig(t *testing.T) {
	schema, err := schemaFromConfig([]columnConfig{{"id", "string"}, {"n", "long"}, {"at", "timestamptz"}})
	require.NoError(t, err)
	fields := schema.Fields()
	require.Len(t, fields, 3)
	for i, f := range fields {
		assert.Equal(t, i+1, f.ID, "field IDs follow config order")
		assert.False(t, f.Required)
	}
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[1].Type)
	assert.Equal(t, iceberg.PrimitiveTypes.TimestampTz, fields[2].Type)

	for name, cols := range map[string][]columnConfig{
		"empty":     nil,
		"no name":   {{"", "string"}},
		"duplicate": {{"a", "string"}, {"a", "int"}},
		"bad type":  {{"a", "varchar"}},
	} {
		_, err := schemaFromConfig(cols)
		assert.Error(t, err, name)
	}
}

func TestPartitionSpecFromConfig(t *testing.T) {
	schema, err := schemaFromConfig([]columnConfig{{"id", "string"}, {"country", "string"}, {"at", "timestamptz"}, {"n", "int"}})
	require.NoError(t, err)

	spec, err := partitionSpecFromConfig([]string{"day(at)", "country", "bucket(16, id)", " truncate( 4 ,id ) ", "HOUR(at)"}, schema)
	require.NoError(t, err)
	var got []string
	for i := range spec.NumFields() {
		f := spec.Field(i)
		got = append(got, fmt.Sprintf("%s=%s(%d)", f.Name, f.Transform, f.SourceIDs[0]))
		assert.Equal(t, firstPartitionFieldID+i, f.FieldID)
	}
	assert.Equal(t, []string{"at_day=day(3)", "country=identity(2)", "id_bucket=bucket[16](1)", "id_trunc=truncate[4](1)", "at_hour=hour(3)"}, got)

	unpartitioned, err := partitionSpecFromConfig(nil, schema)
	require.NoError(t, err)
	assert.Equal(t, 0, unpartitioned.NumFields())

	for _, bad := range []string{"day(missing)", "week(at)", "bucket(id)", "day(4, at)", "day(id)", "day(at", "a b", ""} {
		_, err := partitionSpecFromConfig([]string{bad}, schema)
		assert.Error(t, err, "%q", bad)
	}
}

func TestConvertValue(t *testing.T) {
	ts := time.Date(2026, 11, 20, 14, 3, 0, 123000000, time.UTC)
	plusOne := time.FixedZone("x", 3600)
	types := iceberg.PrimitiveTypes
	tests := []struct {
		typ     iceberg.Type
		in      any
		want    any
		wantErr bool
	}{
		{types.TimestampTz, ts, ts, false},
		{types.TimestampTz, ts.In(plusOne), ts, false},
		{types.TimestampTz, "2026-11-20T14:03:00.123Z", ts, false},
		{types.TimestampTz, "2026-11-20T15:03:00.123+01:00", ts, false},
		{types.TimestampTz, "yesterday", nil, true},
		{types.TimestampTz, 12, nil, true},
		// A timestamp without zone keeps the wall-clock reading.
		{types.Timestamp, "2026-11-20T14:03:00.123", ts, false},
		{types.Timestamp, "2026-11-20 14:03:00.123", ts, false},
		{types.Timestamp, "2026-11-20T14:03:00.123+01:00", ts, false},
		{types.Timestamp, time.Date(2026, 11, 20, 14, 3, 0, 123000000, plusOne), ts, false},
		{types.Date, "2026-11-20", arrow.Date32FromTime(ts), false},
		{types.Date, ts, arrow.Date32FromTime(ts), false},
		{types.Date, "20/11/2026", nil, true},
		{types.Int32, 42, int32(42), false},
		{types.Int32, float64(42), int32(42), false},
		{types.Int32, json.Number("42"), int32(42), false},
		{types.Int32, "42", int32(42), false},
		{types.Int32, 4.5, nil, true},
		{types.Int32, int64(1) << 40, nil, true},
		{types.Int32, true, nil, true},
		{types.Int64, int64(1) << 40, int64(1) << 40, false},
		{types.Int64, json.Number("1099511627776"), int64(1) << 40, false},
		{types.Int64, "x", nil, true},
		{types.Float64, 1.5, 1.5, false},
		{types.Float64, 2, float64(2), false},
		{types.Float64, "x", nil, true},
		{types.Float32, 0.25, float32(0.25), false},
		{types.Bool, true, true, false},
		{types.Bool, "true", true, false},
		{types.Bool, "maybe", nil, true},
		{types.Bool, 1, nil, true},
		{types.String, "a", "a", false},
		{types.String, 7, "7", false},
		{types.String, true, "true", false},
		// Objects and arrays are stored as JSON.
		{types.String, map[string]any{"k": []any{1}}, `{"k":[1]}`, false},
		{types.String, []any{"a", 1}, `["a",1]`, false},
		{types.String, `{"already":"json"}`, `{"already":"json"}`, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%v", tt.typ, tt.in), func(t *testing.T) {
			got, err := convertValue(tt.typ, tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEncoder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "n", Type: iceberg.PrimitiveTypes.Int32},
	)

	t.Run("a bad row is rejected whole", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer mem.AssertSize(t, 0)
		enc, err := newRowEncoder(mem, schema, true)
		require.NoError(t, err)
		defer enc.Release()

		require.NoError(t, enc.Append(map[string]any{"id": "a", "n": 1}))
		// The second field fails, so nothing of this row may be appended.
		require.Error(t, enc.Append(map[string]any{"id": "b", "n": "not a number"}))
		require.Error(t, enc.Append(map[string]any{"n": 2}), "required column without a value")
		require.NoError(t, enc.Append(map[string]any{"id": "c", "n": nil}))

		rec := enc.NewRecordBatch()
		defer rec.Release()
		require.EqualValues(t, 2, rec.NumRows())
		ids := rec.Column(0).(*array.String)
		assert.Equal(t, "a", ids.Value(0))
		assert.Equal(t, "c", ids.Value(1))
		assert.True(t, rec.Column(1).IsNull(1))
	})

	t.Run("unknown fields", func(t *testing.T) {
		row := map[string]any{"id": "a", "extra": 1}

		reject, err := newRowEncoder(memory.DefaultAllocator, schema, true)
		require.NoError(t, err)
		defer reject.Release()
		assert.ErrorIs(t, reject.Append(row), errUnknownField)

		ignore, err := newRowEncoder(memory.DefaultAllocator, schema, false)
		require.NoError(t, err)
		defer ignore.Release()
		assert.NoError(t, ignore.Append(row))
	})

	t.Run("unsupported column type", func(t *testing.T) {
		nested := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "tags", Type: &iceberg.ListType{ElementID: 2, Element: iceberg.PrimitiveTypes.String}})
		_, err := newRowEncoder(memory.DefaultAllocator, nested, true)
		assert.ErrorContains(t, err, `column "tags"`)
	})
}

func TestTableLocation(t *testing.T) {
	loc, err := tableLocation(catalogGlue, "s3://lake", "acme", "events")
	require.NoError(t, err)
	assert.Equal(t, "s3://lake/acme/events", loc)

	loc, err = tableLocation(catalogGlue, "s3://lake/a/b/", "acme", "events")
	require.NoError(t, err)
	assert.Equal(t, "s3://lake/a/b/acme/events", loc)

	// A REST catalog may choose the location itself.
	loc, err = tableLocation(catalogRest, "", "acme", "events")
	require.NoError(t, err)
	assert.Empty(t, loc)

	for _, bad := range [][4]string{
		{catalogGlue, "", "acme", "events"},
		{catalogGlue, "lake", "acme", "events"},
		{catalogGlue, "s3:///nobucket", "acme", "events"},
		{catalogGlue, "s3://lake", "", "events"},
		{catalogGlue, "s3://lake", "acme", "ev/ents"},
		{catalogGlue, "s3://lake", "ac.me", "events"},
	} {
		_, err := tableLocation(bad[0], bad[1], bad[2], bad[3])
		assert.Error(t, err, "%v", bad)
	}
}

func TestConfigErrors(t *testing.T) {
	for name, tt := range map[string]struct{ yaml, want string }{
		"rest without uri": {`
catalog:
  type: rest
namespace: acme
table: events
`, "catalog.rest.uri"},
		"partition_by without schema": {`
warehouse: s3://lake
namespace: acme
table: events
partition_by: [ day(created_at) ]
`, "partition_by"},
		"partition_by unknown column": {`
warehouse: s3://lake
namespace: acme
table: events
partition_by: [ day(created_at) ]
schema:
  - { name: id, type: string }
`, `no column "created_at"`},
		"duplicate column": {`
warehouse: s3://lake
namespace: acme
table: events
schema:
  - { name: id, type: string }
  - { name: id, type: long }
`, "defined twice"},
	} {
		t.Run(name, func(t *testing.T) {
			conf, err := outputSpec().ParseYAML(tt.yaml, nil)
			require.NoError(t, err)
			_, err = newOutputFromConfig(conf, service.MockResources())
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestOutputCreatesNamespaceAndTableOnce(t *testing.T) {
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	a := newTestOutput(t, cat, warehouse, testTableYAML)
	b := newTestOutput(t, cat, warehouse, testTableYAML)

	assert.Equal(t, a.currentTable().Metadata().TableUUID(), b.currentTable().Metadata().TableUUID())
	assert.Equal(t, warehouse+"/acme/events", a.currentTable().Location())

	meta := a.currentTable().Metadata()
	assert.Equal(t, 2, meta.Version())
	assert.Equal(t, "zstd", meta.Properties()[table.ParquetCompressionKey])
	fields := meta.CurrentSchema().Fields()
	require.Len(t, fields, 10)
	assert.Equal(t, "created_at", fields[1].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.TimestampTz, fields[1].Type)
	spec := meta.PartitionSpec()
	require.Equal(t, 1, spec.NumFields())
	field := spec.Field(0)
	assert.Equal(t, "created_at_day", field.Name)
	assert.Equal(t, "day", field.Transform.String())
}

func TestOutputTableProperties(t *testing.T) {
	out := newTestOutput(t, newMemCatalog(), testWarehouse(t), testTableYAML+`
table_properties:
  write.metadata.metrics.default: counts
  write.parquet.compression-codec: snappy
`)
	props := out.currentTable().Metadata().Properties()
	assert.Equal(t, "counts", props["write.metadata.metrics.default"])
	assert.Equal(t, "snappy", props[table.ParquetCompressionKey], "config overrides the defaults")
	assert.Equal(t, "10", props[table.CommitNumRetriesKey])
}

func TestOutputNeedsSchemaToCreate(t *testing.T) {
	cat := newMemCatalog()
	out := newUnconnectedTestOutput(t, cat, testWarehouse(t), "")
	err := out.Connect(context.Background())
	require.ErrorContains(t, err, "no schema is configured")
	assert.Empty(t, cat.namespaces, "nothing may be created")
}

// TestOutputUsesExistingTable: the table is the source of truth, so an output
// with no schema, or a different one, writes with the table's columns.
func TestOutputUsesExistingTable(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	newTestOutput(t, cat, warehouse, testTableYAML)

	noSchema := newTestOutput(t, cat, warehouse, "")
	require.NoError(t, noSchema.WriteBatch(ctx, service.MessageBatch{testMessage("a")}))

	other := newTestOutput(t, cat, warehouse, `
schema:
  - { name: something_else, type: long }
`)
	require.NoError(t, other.WriteBatch(ctx, service.MessageBatch{testMessage("b")}))

	rows := readTableRows(t, other)
	require.Len(t, rows, 2)
	assert.Len(t, rows[0], 10)
}

// TestOutputRoundTrip: a row read back from the table equals the message that
// went in, for every supported type.
func TestOutputRoundTrip(t *testing.T) {
	ctx := context.Background()
	warehouse := testWarehouse(t)
	out := newTestOutput(t, newMemCatalog(), warehouse, testTableYAML)

	sparse := service.NewMessage([]byte(`{"id":"sparse","created_at":"2026-11-20T23:59:59Z"}`))
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("full"), sparse}))

	rows := readTableRows(t, out)
	require.Len(t, rows, 2)
	byID := map[string]map[string]any{}
	for _, r := range rows {
		byID[r["id"].(string)] = r
	}

	assert.Equal(t, map[string]any{
		"id":         "full",
		"created_at": time.Date(2026, 11, 20, 14, 3, 0, 123456000, time.UTC),
		"count":      int32(3),
		"total":      int64(1) << 40,
		"score":      1.5,
		"ratio":      float32(0.25),
		"active":     true,
		"born":       time.Date(1990, 5, 17, 0, 0, 0, 0, time.UTC),
		"local_time": time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC),
		"payload":    `{"k":["a","b"]}`,
	}, byID["full"])

	// Fields the message doesn't have are null.
	for name, v := range byID["sparse"] {
		if name != "id" && name != "created_at" {
			assert.Nil(t, v, name)
		}
	}

	// Data files land in the day partition of created_at.
	entries, err := os.ReadDir(filepath.Join(tableDir(warehouse), "data", "created_at_day=2026-11-20"))
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	assert.True(t, strings.HasSuffix(entries[0].Name(), ".parquet"))
}

func TestOutputRejectsOnlyBadMessages(t *testing.T) {
	ctx := context.Background()
	out := newTestOutput(t, newMemCatalog(), testWarehouse(t), testTableYAML)

	notObject := service.NewMessage([]byte(`[1,2,3]`))
	notStructured := service.NewMessage([]byte(`plain text`))
	badType := service.NewMessage(nil)
	badType.SetStructured(map[string]any{"id": "x", "count": "not a number"})

	batch := service.MessageBatch{testMessage("good"), notObject, badType, notStructured}
	idx := batch.Index()
	err := out.WriteBatch(ctx, batch)
	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 3, batchErr.IndexedErrors())
	failed := map[int]bool{}
	batchErr.WalkMessagesIndexedBy(idx, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed[i] = true
		}
		return true
	})
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true}, failed)

	// The good row was still committed.
	assert.Len(t, readTableRows(t, out), 1)
}

func TestOutputUnknownFields(t *testing.T) {
	ctx := context.Background()
	extra := func() *service.Message {
		row := testRow("extra")
		row["not_a_column"] = 1
		msg := service.NewMessage(nil)
		msg.SetStructured(row)
		return msg
	}

	t.Run("reject by default", func(t *testing.T) {
		out := newTestOutput(t, newMemCatalog(), testWarehouse(t), testTableYAML)
		err := out.WriteBatch(ctx, service.MessageBatch{extra()})
		var batchErr *service.BatchError
		require.ErrorAs(t, err, &batchErr)
		assert.Empty(t, readTableRows(t, out))
	})

	t.Run("ignore", func(t *testing.T) {
		out := newTestOutput(t, newMemCatalog(), testWarehouse(t), testTableYAML+"unknown_fields: ignore\n")
		require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{extra()}))
		rows := readTableRows(t, out)
		require.Len(t, rows, 1)
		assert.NotContains(t, rows[0], "not_a_column")
	})
}

// TestOutputPicksUpNewColumn: a column added to the table by someone else
// (ALTER TABLE in a query engine) is used without restarting the output.
func TestOutputPicksUpNewColumn(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	out := newTestOutput(t, cat, testWarehouse(t), testTableYAML)
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("before")}))

	tbl, err := cat.LoadTable(ctx, out.ident)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"plan"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit())
	_, err = tx.Commit(ctx)
	require.NoError(t, err)

	row := testRow("after")
	row["plan"] = "pro"
	msg := service.NewMessage(nil)
	msg.SetStructured(row)
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{msg}))

	plans := map[string]any{}
	for _, r := range readTableRows(t, out) {
		plans[r["id"].(string)] = r["plan"]
	}
	assert.Equal(t, map[string]any{"before": nil, "after": "pro"}, plans)
}

func TestOutputConcurrentBatches(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	// Two outputs on the same table behave like two replicas.
	a := newTestOutput(t, cat, warehouse, testTableYAML)
	b := newTestOutput(t, cat, warehouse, testTableYAML)

	const batches = 6
	var wg sync.WaitGroup
	for i := range batches {
		out := a
		if i%2 == 1 {
			out = b
		}
		wg.Go(func() {
			assert.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage(fmt.Sprint(i))}))
		})
	}
	wg.Wait()

	assert.Len(t, readTableRows(t, a), batches, "every concurrently committed batch must be in the table")
}

func TestOutputTransientErrorFailsWholeBatch(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	out := newTestOutput(t, cat, testWarehouse(t), testTableYAML)

	cat.failCommit = errors.New("catalog is down")
	err := out.WriteBatch(ctx, service.MessageBatch{testMessage("a")})
	require.Error(t, err)
	var batchErr *service.BatchError
	assert.NotErrorAs(t, err, &batchErr, "a transient failure must nack the whole batch, not route rows to fallback")

	cat.failCommit = nil
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("a")}))
	assert.Len(t, readTableRows(t, out), 1)
}

func readCommitPointer(t *testing.T, warehouse string, seq int64) commitPointer {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(tableDir(warehouse), "_bento", "commits", fmt.Sprintf("v%012d.json", seq)))
	require.NoError(t, err)
	var p commitPointer
	require.NoError(t, json.Unmarshal(b, &p))
	return p
}

func TestOutputWritesCommitPointers(t *testing.T) {
	ctx := context.Background()
	warehouse := testWarehouse(t)
	out := newTestOutput(t, newMemCatalog(), warehouse, testTableYAML)

	// Connect backs up the newly created, empty table.
	p := readCommitPointer(t, warehouse, 0)
	assert.Nil(t, p.SnapshotID)
	assert.Equal(t, out.currentTable().MetadataLocation(), p.MetadataLocation)

	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("a")}))
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("b")}))

	tbl := out.currentTable()
	p = readCommitPointer(t, warehouse, 2)
	assert.Equal(t, tbl.MetadataLocation(), p.MetadataLocation)
	assert.Equal(t, tbl.Metadata().TableUUID().String(), p.TableUUID)
	require.NotNil(t, p.SnapshotID)
	assert.Equal(t, tbl.Metadata().CurrentSnapshot().SnapshotID, *p.SnapshotID)
	assert.EqualValues(t, 2, p.SequenceNumber)

	seq, ok := parseCommitPointerName("v000000000042.json")
	assert.True(t, ok)
	assert.EqualValues(t, 42, seq)
	for _, bad := range []string{"v42.txt", "x42.json", "v-1.json", "vabc.json"} {
		_, ok := parseCommitPointerName(bad)
		assert.False(t, ok, bad)
	}
}

// TestOutputReRegistersLostTable: if the catalog entry disappears, the output
// registers the existing table again instead of creating an empty one over it.
func TestOutputReRegistersLostTable(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	out := newTestOutput(t, cat, warehouse, testTableYAML)
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("a")}))
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("b")}))
	uuid := out.currentTable().Metadata().TableUUID()

	// Leave an uncommitted metadata file with a higher number behind, as a
	// commit that lost a conflict does: it must not be picked.
	stray := filepath.Join(tableDir(warehouse), "metadata", "99999-stray.metadata.json")
	require.NoError(t, os.WriteFile(stray, []byte(`{"not":"current"}`), 0o644))

	cat.forget(out.ident)

	// No schema needed: the table is recovered, not created.
	again := newTestOutput(t, cat, warehouse, "")
	assert.Equal(t, uuid, again.currentTable().Metadata().TableUUID())
	assert.Len(t, readTableRows(t, again), 2)

	require.NoError(t, again.WriteBatch(ctx, service.MessageBatch{testMessage("c")}))
	assert.Len(t, readTableRows(t, again), 3)
}

// TestOutputRefusesUnknownCurrentVersion: committed data but no pointer means
// the current version can't be told; the output must not create a new table.
func TestOutputRefusesUnknownCurrentVersion(t *testing.T) {
	ctx := context.Background()
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	out := newTestOutput(t, cat, warehouse, testTableYAML)
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{testMessage("a")}))

	cat.forget(out.ident)
	require.NoError(t, os.RemoveAll(filepath.Join(tableDir(warehouse), "_bento")))

	again := newUnconnectedTestOutput(t, cat, warehouse, testTableYAML)
	err := again.Connect(ctx)
	require.ErrorIs(t, err, errTableNotRecoverable)
	_, err = cat.LoadTable(ctx, out.ident)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable, "nothing may be created over the existing data")
}

// TestOutputCreatesOverEmptyLeftovers: metadata files of a table that never
// had a commit (say, a create that failed after writing its metadata) hold no
// data, so a new table may be created.
func TestOutputCreatesOverEmptyLeftovers(t *testing.T) {
	cat := newMemCatalog()
	warehouse := testWarehouse(t)
	out := newTestOutput(t, cat, warehouse, testTableYAML)

	cat.forget(out.ident)
	require.NoError(t, os.RemoveAll(filepath.Join(tableDir(warehouse), "_bento")))

	again := newTestOutput(t, cat, warehouse, testTableYAML)
	assert.Empty(t, readTableRows(t, again))
}

// testPageViewEnrichedJSON is a page_view event exactly as the opensnowcat
// processor emits it with `output_format: enriched_json`.
//
//go:embed testdata/page_view_enriched.json
var testPageViewEnrichedJSON []byte

// TestOpenSnowcatExample runs the OpenSnowcat example config against a real
// enriched_json event: every field of the event must have a column, and every
// value must read back unchanged.
func TestOpenSnowcatExample(t *testing.T) {
	ctx := context.Background()
	opensnowcatExample, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "examples", "opensnowcat_iceberg.yaml"))
	require.NoError(t, err)

	var example struct {
		Output struct {
			Fallback []map[string]map[string]any `yaml:"fallback"`
		} `yaml:"output"`
	}
	require.NoError(t, yaml.Unmarshal(opensnowcatExample, &example))
	conf := example.Output.Fallback[0]["iceberg"]
	require.NotNil(t, conf)
	for _, key := range []string{"catalog", "warehouse", "namespace", "table", "region"} {
		delete(conf, key)
	}
	tableYAML, err := yaml.Marshal(conf)
	require.NoError(t, err)

	warehouse := testWarehouse(t)
	out := newTestOutput(t, newMemCatalog(), warehouse, string(tableYAML))
	meta := out.currentTable().Metadata()
	assert.Len(t, meta.CurrentSchema().Fields(), 131)
	assert.Equal(t, "counts", meta.Properties()["write.metadata.metrics.default"])
	spec := meta.PartitionSpec()
	require.Equal(t, 1, spec.NumFields())
	field := spec.Field(0)
	assert.Equal(t, "collector_tstamp_day", field.Name)

	msg := service.NewMessage(testPageViewEnrichedJSON)
	v, err := msg.AsStructured()
	require.NoError(t, err)
	event := v.(map[string]any)

	// unknown_fields is reject, so this fails if any event field has no column.
	require.NoError(t, out.WriteBatch(ctx, service.MessageBatch{msg}))

	rows := readTableRows(t, out)
	require.Len(t, rows, 1)
	row := rows[0]
	for _, col := range meta.CurrentSchema().Fields() {
		raw, ok := event[col.Name]
		if !ok || raw == nil {
			assert.Nil(t, row[col.Name], "column %s should be null", col.Name)
			continue
		}
		want, err := convertValue(col.Type, raw)
		require.NoError(t, err, col.Name)
		if ts, ok := want.(time.Time); ok {
			// Iceberg timestamps are microseconds.
			want = ts.Truncate(time.Microsecond)
		}
		assert.Equal(t, want, row[col.Name], "column %s", col.Name)
	}

	// Timestamps are UTC instants.
	ct, ok := meta.CurrentSchema().FindFieldByName("collector_tstamp")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.TimestampTz, ct.Type)

	// The context columns hold exactly the enriched_json objects.
	var contexts map[string]any
	require.NoError(t, json.Unmarshal([]byte(row["contexts"].(string)), &contexts))
	assert.Contains(t, contexts, "com_snowplowanalytics_snowplow_web_page")
}

// readTableRows loads the current table and returns its rows as maps.
func readTableRows(t *testing.T, out *icebergOutput) []map[string]any {
	t.Helper()
	ctx := out.withAWS(context.Background())
	tbl, err := out.currentCatalog().LoadTable(ctx, out.ident)
	require.NoError(t, err)
	arrTbl, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer arrTbl.Release()

	var rows []map[string]any
	tr := array.NewTableReader(arrTbl, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			row := map[string]any{}
			for c, field := range rec.Schema().Fields() {
				row[field.Name] = arrowValue(rec.Column(c), r)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func arrowValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Int32:
		return c.Value(i)
	case *array.Int64:
		return c.Value(i)
	case *array.Float32:
		return c.Value(i)
	case *array.Float64:
		return c.Value(i)
	case *array.Boolean:
		return c.Value(i)
	case *array.Date32:
		return c.Value(i).ToTime().UTC()
	case *array.Timestamp:
		unit := c.DataType().(*arrow.TimestampType).Unit
		return c.Value(i).ToTime(unit).UTC()
	}
	return fmt.Sprintf("unsupported %T", col)
}
