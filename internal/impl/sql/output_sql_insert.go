package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Masterminds/squirrel"

	"github.com/Jeffail/shutdown"

	"github.com/aws/aws-sdk-go-v2/aws"

	bento_aws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func sqlInsertOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Inserts a row into an SQL database for each message.").
		Description(``).
		Field(driverField).
		Field(dsnField).
		Field(service.NewStringField("table").
			Description("The table to insert to.").
			Example("foo")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to insert.").
			Example([]string{"foo", "bar", "baz"})).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of columns specified.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ metadata("user.id").string() ]`)).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the insert query (before INSERT).").
			Optional().
			Advanced()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the insert query.").
			Optional().
			Advanced().
			Example("ON CONFLICT (name) DO NOTHING")).
		Field(service.NewInterpolatedStringMapField("clickhouse_settings").
			Description("A map of ClickHouse query settings to apply to each insert batch. Values support interpolation and are evaluated once against the first message in each dispatched output batch. The resolved settings are reused unchanged if Bento reconnects and retries that output transaction. An upstream nack and reprocessing creates a new output transaction and reevaluates the settings.").
			Default(map[string]any{}).
			Advanced().
			Version("1.20.0").
			Example(map[string]any{
				"insert_deduplication_token":                         `${! uuid_v4() }`,
				"deduplicate_blocks_in_dependent_materialized_views": "1",
			})).
		Field(service.NewIntField("max_in_flight").
						Description("The maximum number of inserts to run in parallel.").
						Default(64)).
		LintRule(SQLConnLintRule) // TODO: Move AWS related fields to an 'aws' object field in Bento v2

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Example("Table Insert (MySQL)",
			`
Here we insert rows into a database by populating the columns id, name and topic with values extracted from messages and metadata:`,
			`
output:
  sql_insert:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    table: footable
    columns: [ id, name, topic ]
    args_mapping: |
      root = [
        this.user.id,
        this.user.name,
        metadata("kafka_topic"),
      ]
`,
		).
		Example("Table Insert (DuckDB)",
			"Write events to a local DuckDB file, creating the table on first run via `init_statement`.",
			`
# BENTO LINT DISABLE
output:
  sql_insert:
    driver: duckdb
    dsn: /tmp/duckburg.duckdb
    table: vault_deposits
    columns: [id, duck, gold_coins]
    args_mapping: "root = [this.id, this.duck, this.gold_coins]"
    init_statement: |
      CREATE TABLE IF NOT EXISTS vault_deposits (
        id         INTEGER PRIMARY KEY,
        duck       VARCHAR,
        gold_coins BIGINT
      )
`,
		).
		Example("ClickHouse Batch Deduplication",
			"Assign a unique deduplication token to each dispatched Bento output batch. All rows in the batch and any connection retry of that output transaction use the same token.",
			`
output:
  sql_insert:
    driver: clickhouse
    dsn: clickhouse://default:@localhost:9000/default
    table: events
    columns: [event_id, payload]
    args_mapping: 'root = [this.event_id, this]'
    clickhouse_settings:
      insert_deduplication_token: '${! uuid_v4() }'
      deduplicate_blocks_in_dependent_materialized_views: '1'
    batching:
      count: 1000
      period: 1s
`,
		)
	return spec
}

func init() {
	err := service.RegisterBatchOutput(
		"sql_insert", sqlInsertOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			var sqlOut *sqlInsertOutput
			if sqlOut, err = newSQLInsertOutputFromConfig(conf, mgr); err == nil {
				if len(sqlOut.clickhouseSettings) > 0 {
					out = &sqlInsertOutputWithBatchContext{sqlInsertOutput: sqlOut}
				} else {
					out = sqlOut
				}
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlInsertOutput struct {
	driver  string
	dsn     string
	db      *sql.DB
	builder squirrel.InsertBuilder
	dbMut   sync.RWMutex

	useTxStmt   bool
	argsMapping *bloblang.Executor

	clickhouseSettings      map[string]*service.InterpolatedString
	applyClickhouseSettings func(context.Context, clickhouse.Settings) context.Context

	connSettings *connSettings
	awsConf      aws.Config

	logger  *service.Logger
	shutSig *shutdown.Signaller
}

type sqlInsertOutputWithBatchContext struct {
	*sqlInsertOutput
}

func (s *sqlInsertOutputWithBatchContext) PrepareBatchContext(ctx context.Context, batch service.MessageBatch) (context.Context, error) {
	return s.prepareBatchContext(ctx, batch)
}

func newSQLInsertOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlInsertOutput, error) {
	s := &sqlInsertOutput{
		logger:  mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
		applyClickhouseSettings: func(ctx context.Context, settings clickhouse.Settings) context.Context {
			return clickhouse.Context(ctx, clickhouse.WithSettings(settings))
		},
	}

	var err error

	if s.driver, err = conf.FieldString("driver"); err != nil {
		return nil, err
	}
	if _, in := map[string]struct{}{
		"clickhouse": {},
		"oracle":     {},
	}[s.driver]; in {
		s.useTxStmt = true
	}

	if s.dsn, err = conf.FieldString("dsn"); err != nil {
		return nil, err
	}

	if s.clickhouseSettings, err = conf.FieldInterpolatedStringMap("clickhouse_settings"); err != nil {
		return nil, err
	}
	if s.driver != "clickhouse" && len(s.clickhouseSettings) > 0 {
		return nil, errors.New("clickhouse_settings can only be used with the clickhouse driver")
	}

	tableStr, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}

	columns, err := conf.FieldStringList("columns")
	if err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if s.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	s.builder = squirrel.Insert(tableStr).Columns(columns...)
	switch s.driver {
	case "postgres", "clickhouse":
		s.builder = s.builder.PlaceholderFormat(squirrel.Dollar)
	case "oracle", "gocosmos":
		s.builder = s.builder.PlaceholderFormat(squirrel.Colon)
	}

	if s.useTxStmt {
		values := make([]any, 0, len(columns))
		for _, c := range columns {
			values = append(values, c)
		}
		s.builder = s.builder.Values(values...)
	}

	if conf.Contains("prefix") {
		prefixStr, err := conf.FieldString("prefix")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Prefix(prefixStr)
	}

	if conf.Contains("suffix") {
		suffixStr, err := conf.FieldString("suffix")
		if err != nil {
			return nil, err
		}
		s.builder = s.builder.Suffix(suffixStr)
	}

	if s.connSettings, err = connSettingsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	awsEnabled, err := IsAWSEnabled(conf)
	if err != nil {
		return nil, err
	}
	if awsEnabled {
		s.awsConf, err = bento_aws.GetSession(context.Background(), conf.Namespace("aws"))
		if err != nil {
			return nil, err
		}
	}

	go func() {
		<-s.shutSig.HardStopChan()

		s.dbMut.Lock()
		if s.db != nil {
			_ = s.db.Close()
			s.db = nil
		}
		s.dbMut.Unlock()

		s.shutSig.TriggerHasStopped()
	}()

	return s, nil
}

func (s *sqlInsertOutput) Connect(ctx context.Context) error {
	s.dbMut.Lock()
	defer s.dbMut.Unlock()

	if s.db != nil {
		return nil
	}

	var err error
	if s.db, err = sqlOpenWithReworks(ctx, s.logger, s.driver, s.dsn, s.connSettings); err != nil {
		return err
	}

	s.connSettings.apply(ctx, s.db, s.logger)
	return nil
}

func (s *sqlInsertOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	err := s.writeBatch(ctx, batch)
	if err == nil {
		return nil
	}

	if errors.Is(err, driver.ErrBadConn) || isAuthError(s.driver, err) {
		s.dbMut.Lock()
		if s.db != nil {
			_ = s.db.Close()
			s.db = nil
		}
		s.dbMut.Unlock()
		return service.ErrNotConnected
	}

	return err
}

func (s *sqlInsertOutput) writeBatch(ctx context.Context, batch service.MessageBatch) error {
	s.dbMut.RLock()
	defer s.dbMut.RUnlock()

	if s.db == nil {
		return service.ErrNotConnected
	}

	insertBuilder := s.builder

	var tx *sql.Tx
	var stmt *sql.Stmt
	var executor *service.MessageBatchBloblangExecutor

	if s.argsMapping != nil {
		executor = batch.BloblangExecutor(s.argsMapping)
	}

	if s.useTxStmt {
		var err error
		if tx, err = s.db.Begin(); err != nil {
			return err
		}
		sqlStr, _, err := insertBuilder.ToSql()
		if err != nil {
			return err
		}
		if len(s.clickhouseSettings) > 0 {
			stmt, err = tx.PrepareContext(ctx, sqlStr)
		} else {
			stmt, err = tx.Prepare(sqlStr)
		}
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for i := range batch {
		var args []any
		if s.argsMapping != nil {
			resMsg, err := executor.Query(i)
			if err != nil {
				return err
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				return err
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				return fmt.Errorf("mapping returned non-array result: %T", iargs)
			}
		}

		if tx == nil {
			insertBuilder = insertBuilder.Values(args...)
		} else if _, err := stmt.Exec(args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	var err error
	if tx == nil {
		_, err = insertBuilder.RunWith(s.db).ExecContext(ctx)
	} else {
		err = tx.Commit()
	}
	return err
}

func (s *sqlInsertOutput) prepareBatchContext(ctx context.Context, batch service.MessageBatch) (context.Context, error) {
	if len(s.clickhouseSettings) == 0 {
		return ctx, nil
	}
	if len(batch) == 0 {
		return nil, errors.New("cannot resolve clickhouse_settings for an empty batch")
	}

	settings := make(clickhouse.Settings, len(s.clickhouseSettings))
	for key, expr := range s.clickhouseSettings {
		value, err := batch.TryInterpolatedString(0, expr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve ClickHouse setting %q: %w", key, err)
		}
		settings[key] = value
	}

	return s.applyClickhouseSettings(ctx, settings), nil
}

func (s *sqlInsertOutput) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	s.dbMut.RLock()
	isNil := s.db == nil
	s.dbMut.RUnlock()
	if isNil {
		return nil
	}
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
