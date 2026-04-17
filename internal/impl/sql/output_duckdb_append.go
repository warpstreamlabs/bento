//go:build x_bento_extra

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func duckdbAppendOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Inserts rows into a DuckDB database using the Appender API.").
		Description(`Writes rows directly into DuckDB's columnar storage via the Appender API, bypassing SQL parsing entirely. Faster than ` + "`sql_insert`" + ` for bulk ingestion.

The target table must exist before the component connects. Use ` + "`init_statement`" + ` to create it if needed.

Type coercions applied to ` + "`args_mapping`" + ` output before passing to the Appender:
- JSON integers (` + "`json.Number`" + `) → ` + "`int64`" + `
- JSON decimals (` + "`json.Number`" + `) → ` + "`float64`" + `
- RFC3339 strings → ` + "`time.Time`" + ` (for TIMESTAMP columns)
- Nested types (maps, slices) are recursively coerced for STRUCT and LIST columns
- All other types passed through unchanged.

Rows are flushed to disk on every WriteBatch call. When the component shuts down, Close() also flushes any remaining buffered rows.`).
		Field(service.NewStringField("dsn").
			Description("Path to the DuckDB database file. Use `:memory:` for an ephemeral in-process database.").
			Example("/data/bento.duckdb").
			Example(":memory:")).
		Field(service.NewStringField("table").
			Description("Target table name. The table must exist when `Connect()` is called; use `init_statement` to create it.")).
		Field(service.NewStringField("schema").
			Description("DuckDB schema containing the table. Empty string uses the default schema.").
			Default("")).
		Field(service.NewStringListField("columns").
			Description("Ordered list of column names. Must match the order of values produced by `args_mapping`.").
			Example([]string{"duck", "coins", "deposited_at"})).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that must evaluate to an array of values, one per column in the same order as `columns`.").
			Example("root = [ this.duck, this.coins, this.deposited_at ]"))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Example("Table Insert via Appender",
			`
Insert rows into a database by bulk-loading them directly into DuckDB's columnar storage via the Appender (no SQL parsing overhead).`,
			`
# BENTO LINT DISABLE
output:
  duckdb_append:
    dsn: /data/vault.duckdb
    table: vault_deposits
    columns: [ deposit_id, duck, coins, denomination, deposited_at ]
    args_mapping: |
      root = [
        this.deposit_id,
        this.duck,
        this.coins,
        this.denomination,
        this.deposited_at,
      ]
    init_statement: |
      CREATE TABLE IF NOT EXISTS vault_deposits (
        deposit_id   VARCHAR PRIMARY KEY,
        duck         VARCHAR NOT NULL,
        coins        BIGINT  NOT NULL,
        denomination VARCHAR NOT NULL,
        deposited_at TIMESTAMP NOT NULL
      )
    batching:
      count: 10000
      period: 5s
`,
		)
	return spec
}

func init() {
	err := service.RegisterBatchOutput(
		"duckdb_append", duckdbAppendOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			out service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}

			maxInFlight = 1 // DuckDB Appender uses single writer

			out, err = newDuckDBAppendOutputFromConfig(conf, mgr)
			return
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type duckdbAppendOutput struct {
	mu       sync.RWMutex
	db       *sql.DB
	sqlConn  *sql.Conn
	appender *duckdb.Appender

	dsn     string
	table   string
	schema  string
	columns []string

	argsMapping  *bloblang.Executor
	connSettings *connSettings

	shutSig *shutdown.Signaller
	logger  *service.Logger
}

func DuckDBAppendOutputConfig() *service.ConfigSpec {
	return duckdbAppendOutputConfig()
}

func NewDuckDBAppendOutputFromConfig(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (service.BatchOutput, error) {
	return newDuckDBAppendOutputFromConfig(conf, mgr)
}

func newDuckDBAppendOutputFromConfig(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (*duckdbAppendOutput, error) {
	d := &duckdbAppendOutput{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if d.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}

	if d.table, err = conf.FieldString("table"); err != nil {
		return nil, err
	}

	if d.schema, err = conf.FieldString("schema"); err != nil {
		return nil, err
	}

	if d.columns, err = conf.FieldStringList("columns"); err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if d.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	if d.connSettings, err = connSettingsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *duckdbAppendOutput) startShutdownListener() {
	go func() {
		<-d.shutSig.HardStopChan()

		d.mu.Lock()
		defer d.mu.Unlock()

		if d.appender != nil {
			d.logger.Debugf("Closing DuckDB appender for table %q (schema %q), flushing pending rows", d.table, d.schema)
			if err := d.appender.Close(); err != nil {
				d.logger.Errorf("Closing DuckDB appender: %v", err)
			}
			d.appender = nil
		}

		if d.sqlConn != nil {
			if err := d.sqlConn.Close(); err != nil {
				d.logger.Errorf("Closing DuckDB database connection: %v", err)
			}
			d.sqlConn = nil
		}

		if d.db != nil {
			if err := d.db.Close(); err != nil {
				d.logger.Errorf("Closing DuckDB connection pool: %v", err)
			}
			d.db = nil
		}

		d.shutSig.TriggerHasStopped()
	}()
}

func (d *duckdbAppendOutput) Connect(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.appender != nil {
		return nil
	}

	var err error
	if d.db, err = sqlOpenWithReworks(ctx, d.logger, "duckdb", d.dsn, d.connSettings); err != nil {
		return fmt.Errorf("opening duckdb: %w", err)
	}

	// Runs init_statement to create the the table first
	// otherwise retrieval of DuckDB appender will error
	d.connSettings.apply(ctx, d.db, d.logger)

	d.sqlConn, err = d.db.Conn(ctx)
	if err != nil {
		_ = d.db.Close()
		return fmt.Errorf("acquiring dedicated connection: %w", err)
	}

	if err := d.sqlConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("expected *duckdb.Conn, got %T", driverConn)
		}

		var err error
		// Schema and table must exist before retrieving appender
		d.appender, err = duckdb.NewAppenderFromConn(conn, d.schema, d.table)
		return err
	}); err != nil {
		_ = d.sqlConn.Close()
		_ = d.db.Close()
		return fmt.Errorf("creating appender for table %q: %w", d.table, err)
	}

	d.startShutdownListener()
	return nil
}

func (d *duckdbAppendOutput) Close(ctx context.Context) error {
	d.shutSig.TriggerHardStop()

	d.mu.RLock()
	isNil := d.appender == nil
	d.mu.RUnlock()
	if isNil {
		return nil
	}

	select {
	case <-d.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (d *duckdbAppendOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	d.mu.RLock()
	if d.appender == nil {
		d.mu.RUnlock()
		return service.ErrNotConnected
	}
	appender := d.appender
	d.mu.RUnlock()

	var executor *service.MessageBatchBloblangExecutor
	if d.argsMapping != nil {
		executor = batch.BloblangExecutor(d.argsMapping)
	}

	var batchErr *service.BatchError
	for i := range batch {
		err := d.appendRow(executor, i, len(d.columns))
		if err != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, err)
			}
			batchErr.Failed(i, err)
		}
	}

	if batchErr != nil {
		return batchErr
	}

	// Flushing after every batch to access the appended rows immediately
	// Produces partially-filled row groups, which may be less optimal for larger queries
	if err := appender.Flush(); err != nil {
		return err
	}

	return nil
}

func (d *duckdbAppendOutput) appendRow(executor *service.MessageBatchBloblangExecutor, i int, colCount int) error {
	var args []any

	if executor != nil {
		resMsg, err := executor.Query(i)
		if err != nil {
			return fmt.Errorf("executing args_mapping: %w", err)
		}

		iargs, err := resMsg.AsStructured()
		if err != nil {
			return fmt.Errorf("reading args_mapping result: %w", err)
		}

		var ok bool
		if args, ok = iargs.([]any); !ok {
			return fmt.Errorf("non-array result: %T", iargs)
		}
		if len(args) != colCount {
			return fmt.Errorf("%d values for %d columns", len(args), colCount)
		}
	}

	driverArgs := make([]driver.Value, len(args))
	for j, v := range args {
		driverArgs[j] = d.coerceValue(v)
	}

	if err := d.appender.AppendRow(driverArgs...); err != nil {
		return fmt.Errorf("appending row: %w", err)
	}

	return nil
}

func (d *duckdbAppendOutput) coerceValue(v any) driver.Value {
	switch n := v.(type) {
	case json.Number:
		if i64, err := n.Int64(); err == nil {
			return i64
		} else if f64, err := n.Float64(); err == nil {
			return f64
		} else {
			return n.String()
		}

	case string:
		if t, err := time.Parse(time.RFC3339, n); err == nil {
			return t
		}
		return v

	case map[string]any:
		coercedMap := make(map[string]any, len(n))
		for k, nv := range n {
			coercedMap[k] = d.coerceValue(nv)
		}
		return coercedMap

	case []any:
		coercedList := make([]any, len(n))
		for i, nv := range n {
			coercedList[i] = d.coerceValue(nv)
		}
		return coercedList

	default:
		return v
	}
}
