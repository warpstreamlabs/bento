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
- All other types passed through unchanged.`).
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
	mu       sync.Mutex
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

	go func() {
		<-d.shutSig.HardStopChan()

		d.mu.Lock()
		defer d.mu.Unlock()

		if d.appender != nil {
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

	return nil
}

func (d *duckdbAppendOutput) Close(ctx context.Context) error {
	d.shutSig.TriggerHardStop()

	d.mu.Lock()
	isNil := d.appender == nil
	d.mu.Unlock()
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
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.appender == nil {
		return service.ErrNotConnected
	}

	var executor *service.MessageBatchBloblangExecutor
	if d.argsMapping != nil {
		executor = batch.BloblangExecutor(d.argsMapping)
	}

	for i := range batch {
		var args []any

		if executor != nil {
			resMsg, err := executor.Query(i)
			if err != nil {
				return fmt.Errorf("executing args_mapping for message %d: %w", i, err)
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				return fmt.Errorf("reading args_mapping result for message %d: %w", i, err)
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				return fmt.Errorf("mapping returned non-array result: %T", iargs)
			}
			if len(args) != len(d.columns) {
				return fmt.Errorf("mapping returned %d values for %d columns", len(args), len(d.columns))
			}
		}

		driverArgs := make([]driver.Value, len(args))
		for j, v := range args {
			switch n := v.(type) {
			case json.Number:
				if i64, err := n.Int64(); err == nil {
					driverArgs[j] = i64
				} else if f64, err := n.Float64(); err == nil {
					driverArgs[j] = f64
				} else {
					driverArgs[j] = n.String()
				}
			case string:
				if t, err := time.Parse(time.RFC3339, n); err == nil {
					driverArgs[j] = t
				} else {
					driverArgs[j] = v
				}
			default:
				driverArgs[j] = v
			}
		}

		if err := d.appender.AppendRow(driverArgs...); err != nil {
			return fmt.Errorf("appending row %d: %w", i, err)
		}
	}

	// Flushing after every batch to access the appended rows immediately
	// Produces partially-filled row groups, which may be less optimal for larger queries
	if err := d.appender.Flush(); err != nil {
		return err
	}

	return nil
}
