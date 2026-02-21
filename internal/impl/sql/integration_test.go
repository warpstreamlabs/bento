//nolint:staticcheck // Ignore SA1019
package sql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	isql "github.com/warpstreamlabs/bento/internal/impl/sql"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	_ "github.com/warpstreamlabs/bento/public/components/sql"
)

type testFn func(t *testing.T, driver, dsn, table string)

func testProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		colList := `[ "foo", "bar", "baz" ]`
		if driver == "oracle" {
			colList = `[ "\"foo\"", "\"bar\"", "\"baz\"" ]`
		}
		whereClause := `'foo = ?'`
		if driver == "oracle" {
			whereClause = `'"foo" = ?'`
		}
		t.Run(name, func(t *testing.T) {
			insertConf := fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: %s
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table, colList)

			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
table: %s
columns: [ "*" ]
where: %s
args_mapping: 'root = [ this.id ]'
`, driver, dsn, table, whereClause)

			env := service.NewEnvironment()

			insertConfig, err := isql.InsertProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc, driver)
		})
	}
}

func testRawProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			colListStr := `"foo", "bar", "baz"`
			if driver == "postgres" || driver == "clickhouse" {
				valuesStr = `($1, $2, $3)`
			} else if driver == "oracle" {
				valuesStr = `(:1, :2, :3)`
			}

			if driver == "spanner" {
				colListStr = `foo, bar, baz`
			}

			insertConf := fmt.Sprintf(`
driver: %s
dsn: %s
query: insert into %s ( `+colListStr+` ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
exec_only: true
`, driver, dsn, table)

			placeholderStr := "?"
			if driver == "postgres" || driver == "clickhouse" {
				placeholderStr = "$1"
			} else if driver == "oracle" {
				placeholderStr = ":1"
			}

			selectColStr := `"foo"`
			if driver == "spanner" {
				selectColStr = `foo`
			}
			queryConf := fmt.Sprintf(`
driver: %s
dsn: %s
query: select `+colListStr+` from %s where `+selectColStr+` = `+placeholderStr+`
args_mapping: 'root = [ this.id ]'
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.RawProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.RawProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLRawProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLRawProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc, driver)
		})
	}
}

func testRawDeprecatedProcessors(name string, fn func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string)) testFn {
	return func(t *testing.T, driver, dsn, table string) {
		t.Run(name, func(t *testing.T) {
			valuesStr := `(?, ?, ?)`
			colListStr := `"foo", "bar", "baz"`

			if driver == "postgres" || driver == "clickhouse" {
				valuesStr = `($1, $2, $3)`
			} else if driver == "oracle" {
				valuesStr = `(:1, :2, :3)`
			}

			if driver == "spanner" {
				colListStr = `foo, bar, baz`
			}

			insertConf := fmt.Sprintf(`
driver: %s
data_source_name: %s
query: insert into %s ( `+colListStr+` ) values `+valuesStr+`
args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`, driver, dsn, table)

			placeholderStr := "?"
			if driver == "postgres" || driver == "clickhouse" {
				placeholderStr = "$1"
			} else if driver == "oracle" {
				placeholderStr = ":1"
			}

			selectColStr := `"foo"`
			if driver == "spanner" {
				selectColStr = `foo`
			}

			queryConf := fmt.Sprintf(`
driver: %s
data_source_name: %s
query: select `+colListStr+` from %s where `+selectColStr+` = `+placeholderStr+`
args_mapping: 'root = [ this.id ]'
result_codec: json_array
`, driver, dsn, table)

			env := service.NewEnvironment()

			insertConfig, err := isql.DeprecatedProcessorConfig().ParseYAML(insertConf, env)
			require.NoError(t, err)

			selectConfig, err := isql.DeprecatedProcessorConfig().ParseYAML(queryConf, env)
			require.NoError(t, err)

			insertProc, err := isql.NewSQLDeprecatedProcessorFromConfig(insertConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { insertProc.Close(context.Background()) })

			selectProc, err := isql.NewSQLDeprecatedProcessorFromConfig(selectConfig, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { selectProc.Close(context.Background()) })

			fn(t, insertProc, selectProc, driver)
		})
	}
}

var testBatchProcessorBasic = testProcessors("basic", func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		// TODO - Make a breaking change to the SQL components when using driver
		// oracle such that it will marshall int types to a json number instead of string
		var exp string
		exp = fmt.Sprintf(`[{"bar":%d,"baz":"and this","foo":"doc-%d"}]`, i, i)
		if driver == "oracle" {
			exp = fmt.Sprintf(`[{"bar":"%d","baz":"and this","foo":"doc-%d"}]`, i, i)
		}

		actBytes, err := v.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(actBytes))
	}
})

var testBatchProcessorParallel = testProcessors("parallel", func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string) {
	nParallel, nLoops := 10, 50

	startChan := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < nParallel; i++ {
		var insertBatch service.MessageBatch
		for j := 0; j < nLoops; j++ {
			index := i*nLoops + j
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, index, index))))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startChan
			for _, msg := range insertBatch {
				_, err := insertProc.ProcessBatch(context.Background(), service.MessageBatch{msg})
				require.NoError(t, err)
			}
		}()
	}

	close(startChan)
	wg.Wait()

	startChan = make(chan struct{})
	wg = sync.WaitGroup{}
	for i := 0; i < nParallel; i++ {
		var queryBatch service.MessageBatch

		for j := 0; j < nLoops; j++ {
			index := i*nLoops + j
			queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, index))))
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startChan
			for _, msg := range queryBatch {
				resBatches, err := selectProc.ProcessBatch(context.Background(), service.MessageBatch{msg})
				require.NoError(t, err)
				require.Len(t, resBatches, 1)
				require.Len(t, resBatches[0], 1)
				require.NoError(t, resBatches[0][0].GetError())
			}
		}()
	}

	close(startChan)
	wg.Wait()
})

var testRawProcessorsBasic = testRawProcessors("raw", func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		// TODO - Make a breaking change to the SQL components when using driver
		// oracle such that it will marshall int types to a json number instead of string
		var exp string
		exp = fmt.Sprintf(`[{"bar":%d,"baz":"and this","foo":"doc-%d"}]`, i, i)
		if driver == "oracle" {
			exp = fmt.Sprintf(`[{"bar":"%d","baz":"and this","foo":"doc-%d"}]`, i, i)
		}
		actBytes, err := v.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(actBytes))
	}
})

var testDeprecatedProcessorsBasic = testRawDeprecatedProcessors("deprecated", func(t *testing.T, insertProc, selectProc service.BatchProcessor, driver string) {
	var insertBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
  "foo": "doc-%d",
  "bar": %d,
  "baz": "and this"
}`, i, i))))
	}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	var queryBatch service.MessageBatch
	for i := 0; i < 10; i++ {
		queryBatch = append(queryBatch, service.NewMessage([]byte(fmt.Sprintf(`{"id":"doc-%d"}`, i))))
	}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(queryBatch))
	for i, v := range resBatches[0] {
		require.NoError(t, v.GetError())

		// TODO - Make a breaking change to the SQL components when using driver
		// oracle such that it will marshall int types to a json number instead of string
		var exp string
		exp = fmt.Sprintf(`[{"bar":%d,"baz":"and this","foo":"doc-%d"}]`, i, i)
		if driver == "oracle" {
			exp = fmt.Sprintf(`[{"bar":"%d","baz":"and this","foo":"doc-%d"}]`, i, i)
		}
		actBytes, err := v.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(actBytes))
	}
})

func testBatchInputOutputBatch(t *testing.T, driver, dsn, table string) {
	colList := `[ "foo", "bar", "baz" ]`
	orderBy := `"bar"`

	if driver == "oracle" {
		colList = `[ "\"foo\"", "\"bar\"", "\"baz\"" ]`
	} else if driver == "spanner" {
		colList = `[ foo, bar, baz ]`
		orderBy = `bar`
	}

	t.Run("batch_input_output", func(t *testing.T) {
		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
			"$columnlist", colList,
			"$orderby", orderBy,
		)

		outputConf := confReplacer.Replace(`
sql_insert:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: $columnlist
  args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`)

		inputConf := confReplacer.Replace(`
sql_select:
  driver: $driver
  dsn: $dsn
  table: $table
  columns: [ "*" ]
  suffix: ' ORDER BY $orderby ASC'
processors:
  # For some reason MySQL driver doesn't resolve to integer by default.
  - bloblang: |
      root = this
      root.bar = this.bar.number()
`)

		streamInBuilder := service.NewStreamBuilder()
		require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

		inFn, err := streamInBuilder.AddBatchProducerFunc()
		require.NoError(t, err)

		streamIn, err := streamInBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamIn.Run(context.Background()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i))))
		}
		require.NoError(t, inFn(context.Background(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(context.Background()))

		assert.Equal(t, []string{
			"{\"bar\":0,\"baz\":\"and this\",\"foo\":\"doc-0\"}",
			"{\"bar\":1,\"baz\":\"and this\",\"foo\":\"doc-1\"}",
			"{\"bar\":2,\"baz\":\"and this\",\"foo\":\"doc-2\"}",
			"{\"bar\":3,\"baz\":\"and this\",\"foo\":\"doc-3\"}",
			"{\"bar\":4,\"baz\":\"and this\",\"foo\":\"doc-4\"}",
			"{\"bar\":5,\"baz\":\"and this\",\"foo\":\"doc-5\"}",
			"{\"bar\":6,\"baz\":\"and this\",\"foo\":\"doc-6\"}",
			"{\"bar\":7,\"baz\":\"and this\",\"foo\":\"doc-7\"}",
			"{\"bar\":8,\"baz\":\"and this\",\"foo\":\"doc-8\"}",
			"{\"bar\":9,\"baz\":\"and this\",\"foo\":\"doc-9\"}",
		}, outBatches)
	})
}

func testBatchInputOutputRaw(t *testing.T, driver, dsn, table string) {
	t.Run("raw_input_output", func(t *testing.T) {
		confReplacer := strings.NewReplacer(
			"$driver", driver,
			"$dsn", dsn,
			"$table", table,
		)

		valuesStr := `(?, ?, ?)`
		colsStr := `"foo", "bar", "baz"`
		orderByStr := `"bar"`
		if driver == "postgres" || driver == "clickhouse" {
			valuesStr = `($1, $2, $3)`
		} else if driver == "oracle" {
			valuesStr = `(:1, :2, :3)`
		}

		if driver == "spanner" {
			colsStr = `foo, bar, baz`
			orderByStr = `bar`
		}

		outputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  query: insert into $table (` + colsStr + `) values ` + valuesStr + `
  args_mapping: 'root = [ this.foo, this.bar.floor(), this.baz ]'
`)

		inputConf := confReplacer.Replace(`
sql_raw:
  driver: $driver
  dsn: $dsn
  query: 'select * from $table ORDER BY ` + orderByStr + ` ASC'
processors:
  # For some reason MySQL driver doesn't resolve to integer by default.
  - bloblang: |
      root = this
      root.bar = this.bar.number()
`)
		streamInBuilder := service.NewStreamBuilder()
		require.NoError(t, streamInBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamInBuilder.AddOutputYAML(outputConf))

		inFn, err := streamInBuilder.AddBatchProducerFunc()
		require.NoError(t, err)

		streamIn, err := streamInBuilder.Build()
		require.NoError(t, err)

		go func() {
			assert.NoError(t, streamIn.Run(context.Background()))
		}()

		streamOutBuilder := service.NewStreamBuilder()
		require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		require.NoError(t, streamOutBuilder.AddInputYAML(inputConf))

		var outBatches []string
		require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
			msgBytes, err := mb[0].AsBytes()
			require.NoError(t, err)
			outBatches = append(outBatches, string(msgBytes))
			return nil
		}))

		streamOut, err := streamOutBuilder.Build()
		require.NoError(t, err)

		var insertBatch service.MessageBatch
		for i := 0; i < 10; i++ {
			insertBatch = append(insertBatch, service.NewMessage([]byte(fmt.Sprintf(`{
	"foo": "doc-%d",
	"bar": %d,
	"baz": "and this"
}`, i, i))))
		}
		require.NoError(t, inFn(context.Background(), insertBatch))
		require.NoError(t, streamIn.StopWithin(15*time.Second))

		require.NoError(t, streamOut.Run(context.Background()))

		assert.Equal(t, []string{
			"{\"bar\":0,\"baz\":\"and this\",\"foo\":\"doc-0\"}",
			"{\"bar\":1,\"baz\":\"and this\",\"foo\":\"doc-1\"}",
			"{\"bar\":2,\"baz\":\"and this\",\"foo\":\"doc-2\"}",
			"{\"bar\":3,\"baz\":\"and this\",\"foo\":\"doc-3\"}",
			"{\"bar\":4,\"baz\":\"and this\",\"foo\":\"doc-4\"}",
			"{\"bar\":5,\"baz\":\"and this\",\"foo\":\"doc-5\"}",
			"{\"bar\":6,\"baz\":\"and this\",\"foo\":\"doc-6\"}",
			"{\"bar\":7,\"baz\":\"and this\",\"foo\":\"doc-7\"}",
			"{\"bar\":8,\"baz\":\"and this\",\"foo\":\"doc-8\"}",
			"{\"bar\":9,\"baz\":\"and this\",\"foo\":\"doc-9\"}",
		}, outBatches)
	})
}

func testSuite(t *testing.T, driver, dsn string, createTableFn func(string) (string, error)) {

	for _, fn := range []testFn{
		testBatchProcessorBasic,
		testBatchProcessorParallel,
		testBatchInputOutputBatch,
		testBatchInputOutputRaw,
		testRawProcessorsBasic,
		testDeprecatedProcessorsBasic,
	} {
		tableName, err := gonanoid.Generate("abcdefghijklmnopqrstuvwxyz", 40)
		require.NoError(t, err)

		tableName, err = createTableFn(tableName)
		require.NoError(t, err)

		fn(t, driver, dsn, tableName)
	}
}

func TestIntegrationClickhouse(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "clickhouse/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" String,
  "bar" Int64,
  "baz" String
		) engine=Memory;`, name))
		return name, err
	}

	dsn := fmt.Sprintf("clickhouse://localhost:%s/", resource.GetPort("9000/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("clickhouse", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "clickhouse", dsn, createTable)
}

func TestIntegrationOldClickhouse(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "clickhouse/clickhouse-server",
		ExposedPorts: []string{"9000/tcp"},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" String,
  "bar" Int64,
  "baz" String
		) engine=Memory;`, name))
		return name, err
	}

	dsn := fmt.Sprintf("tcp://localhost:%s/", resource.GetPort("9000/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("clickhouse", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "clickhouse", dsn, createTable)
}

func TestIntegrationPostgres(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "postgres",
		ExposedPorts: []string{"5432/tcp"},
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "postgres", dsn, createTable)
}

func TestIntegrationSpanner(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.BuildAndRunWithBuildOptions(&dockertest.BuildOptions{
		ContextDir: "./resources",
		Dockerfile: "Dockerfile_spanner",
	}, &dockertest.RunOptions{
		Name:         "spannertest",
		ExposedPorts: []string{"9010/tcp", "9020/tcp"},
	})
	if err != nil {
		t.Logf("Could not start resource: %s", err)
	}
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("localhost:%s", resource.GetPort("9010/tcp"))

	// This needs to be set so that the Spanner connector will pick up that we are using the emulator.
	t.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)

	project := "test-project"
	instance := "test-instance"
	database := "test-database"

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}

	})

	createTable := func(name string) (string, error) {
		var db *sql.DB
		db, err = sql.Open("spanner", dsn)
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  foo string(50) not null,
  bar int64 not null,
  baz string(50) not null,
		) primary key (foo)`, name))
		return name, err
	}

	require.NoError(t, pool.Retry(func() error {
		var db *sql.DB
		db, err = sql.Open("spanner", dsn)
		if err != nil {
			t.Logf(`open error: %s`, err)
			return err
		}
		if err = db.Ping(); err != nil {
			t.Logf(`ping error: %s`, err)
			db.Close()
			return err
		}
		if _, err := createTable("footable"); err != nil {
			t.Logf(`create table error: %s`, err)
			return err
		}
		return nil
	}))

	testSuite(t, "spanner", dsn, createTable)
}

func TestIntegrationMySQL(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mysql",
		ExposedPorts: []string{"3306/tcp"},
		Cmd: []string{
			"--sql_mode=ANSI_QUOTES",
		},
		Env: []string{
			"MYSQL_USER=testuser",
			"MYSQL_PASSWORD=testpass",
			"MYSQL_DATABASE=testdb",
			"MYSQL_RANDOM_ROOT_PASSWORD=yes",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("testuser:testpass@tcp(localhost:%s)/testdb", resource.GetPort("3306/tcp"))
	require.NoError(t, pool.Retry(func() error {
		if db, err = sql.Open("mysql", dsn); err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "mysql", dsn, createTable)
}

func TestIntegrationMSSQL(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	testPassword := "ins4n3lyStrongP4ssword"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mcr.microsoft.com/mssql/server",
		ExposedPorts: []string{"1433/tcp"},
		Env: []string{
			"ACCEPT_EULA=Y",
			"SA_PASSWORD=" + testPassword,
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("sqlserver://sa:"+testPassword+"@localhost:%s?database=master", resource.GetPort("1433/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("mssql", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "mssql", dsn, createTable)
}

func TestIntegrationSQLite(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	var db *sql.DB
	var err error
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := "file::memory:?cache=shared"

	require.NoError(t, func() error {
		db, err = sql.Open("sqlite", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}())

	testSuite(t, "sqlite", dsn, createTable)
}

func TestIntegrationOracle(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "gvenzl/oracle-free",
		Tag:          "slim-faststart",
		ExposedPorts: []string{"1521/tcp"},
		Env: []string{
			"ORACLE_PASSWORD=testpass",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table %s (
  "foo" varchar(50) not null,
  "bar" integer not null,
  "baz" varchar(50) not null,
  primary key ("foo")
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("oracle://system:testpass@localhost:%s/FREEPDB1", resource.GetPort("1521/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("oracle", dsn)
		if err != nil {
			return err
		}

		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}

		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "oracle", dsn, createTable)
}

func TestIntegrationTrino(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	testPassword := ""
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "trinodb/trino",
		ExposedPorts: []string{"8080/tcp"},
		Env: []string{
			"PASSWORD=" + testPassword,
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		name = "memory.default." + name
		_, err := db.Exec(fmt.Sprintf(`
create table %s (
  "foo" varchar,
  "bar" integer,
  "baz" varchar
)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("http://trinouser:"+testPassword+"@localhost:%s", resource.GetPort("8080/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("trino", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("test"); err != nil {
			return err
		}
		return nil
	}))

	testSuite(t, "trino", dsn, createTable)
}

func TestIntegrationCosmosDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator",
		Tag:        "latest",
		Env: []string{
			// The bigger the value, the longer it takes for the container to start up.
			"AZURE_COSMOS_EMULATOR_PARTITION_COUNT=2",
			"AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false",
		},
		ExposedPorts: []string{"8081/tcp"},
	})
	require.NoError(t, err)

	_ = resource.Expire(900)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createContainer := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create collection %s with pk=/foo`, name))
		return name, err
	}

	dummyDatabase := "PacificOcean"
	dummyContainer := "ChallengerDeep"
	emulatorAccountKey := "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	dsn := fmt.Sprintf(
		"AccountEndpoint=https://localhost:%s;AccountKey=%s;DefaultDb=%s;AutoId=true;InsecureSkipVerify=true",
		resource.GetPort("8081/tcp"), emulatorAccountKey, dummyDatabase,
	)

	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("gocosmos", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := db.Exec(fmt.Sprintf(`create database %s`, dummyDatabase)); err != nil {
			return err
		}
		if _, err := createContainer(dummyContainer); err != nil {
			return err
		}
		return nil
	}))

	// TODO: Enable the full test suite once https://github.com/microsoft/gocosmos/issues/15 is addressed and increase
	// increase `AZURE_COSMOS_EMULATOR_PARTITION_COUNT` so the emulator can create all the required containers. Note
	// that select queries must prefix the column names with the container name (i.e `test.foo`) and, also `select *`
	// will return the autogenerated `id` column, which will break the naive diff when asserting the results.
	// testSuite(t, "gocosmos", dsn, createContainer)

	insertConf := fmt.Sprintf(`
driver: gocosmos
dsn: %s
table: %s
columns:
  - foo
  - bar
  - baz
args_mapping: 'root = [ this.foo, this.bar.uppercase(), this.baz ]'
`, dsn, dummyContainer)

	queryConf := fmt.Sprintf(`
driver: gocosmos
dsn: %s
table: %s
columns:
  - %s.foo
  - %s.bar
  - %s.baz
where: '%s.foo = ?'
args_mapping: 'root = [ this.foo ]'
`, dsn, dummyContainer, dummyContainer, dummyContainer, dummyContainer, dummyContainer)

	env := service.NewEnvironment()

	insertConfig, err := isql.InsertProcessorConfig().ParseYAML(insertConf, env)
	require.NoError(t, err)

	selectConfig, err := isql.SelectProcessorConfig().ParseYAML(queryConf, env)
	require.NoError(t, err)

	insertProc, err := isql.NewSQLInsertProcessorFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { insertProc.Close(context.Background()) })

	selectProc, err := isql.NewSQLSelectProcessorFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { selectProc.Close(context.Background()) })

	insertBatch := service.MessageBatch{service.NewMessage([]byte(`{
  "foo": "blobfish",
  "bar": "are really cool",
  "baz": 41
}`))}

	resBatches, err := insertProc.ProcessBatch(context.Background(), insertBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], len(insertBatch))
	for _, v := range resBatches[0] {
		require.NoError(t, v.GetError())
	}

	queryBatch := service.MessageBatch{service.NewMessage([]byte(`{"foo":"blobfish"}`))}

	resBatches, err = selectProc.ProcessBatch(context.Background(), queryBatch)
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)
	m := resBatches[0][0]
	require.NoError(t, m.GetError())
	actBytes, err := m.AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `[{"foo": "blobfish", "bar": "ARE REALLY COOL", "baz": 41}]`, string(actBytes))
}

func TestIntegrationRedshiftSecret(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	// we need a localstack pro image
	if os.Getenv("LOCALSTACK_AUTH_TOKEN") == "" {
		t.Skip("LOCALSTACK_AUTH_TOKEN is not set")
	}

	tfPath, err := filepath.Abs("./resources/redshiftsecret.tf")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "localstack/localstack-pro",
		Env: []string{
			"LOCALSTACK_AUTH_TOKEN=" + os.Getenv("LOCALSTACK_AUTH_TOKEN"),
			"EXTENSION_AUTO_INSTALL=localstack-extension-terraform-init",
		},
		Mounts: []string{
			tfPath + ":/etc/localstack/init/ready.d/main.tf",
			"/var/run/docker.sock:/var/run/docker.sock",
		},
		ExposedPorts: []string{"4566/tcp", "4510-4559/tcp"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
	})

	_ = resource.Expire(900)

	// grab some ports and wait for the tf to boot up redshift etc.
	localstackPort := resource.GetPort("4566/tcp")
	redshiftPort := resource.GetPort("4510/tcp")

	redshiftEndpointAddress, err := waitForRedshiftAndGetAddress(localstackPort, redshiftPort, "my-redshift-cluster")
	require.NoError(t, err)

	// run a config that will fetch the secrets and create 10 rows in the redshift db
	builder := service.NewStreamBuilder()

	err = builder.SetYAML(fmt.Sprintf(`input:
  generate:
    mapping: |
        root = {"age":random_int(min: 0, max: 100), "name":fake("name")}
    interval: 1ms
    count: 10

output:
  sql_insert:
    driver: postgres 
    dsn: postgresql://username_from_secret:password_from_secret@%v/dev?sslmode=disable
    table: test
    columns: [age, name]
    args_mapping: |
      root = [
        this.age,
        this.name,
      ]
    init_statement: "CREATE TABLE test (name varchar(255), age int);"
    secret_name: "redshift-password"
    region: us-east-1
    endpoint: http://localhost:%v
    credentials:
      id: test
      secret: test`, redshiftEndpointAddress, localstackPort))

	stream, err := builder.Build()
	require.NoError(t, err)

	err = stream.Run(context.Background())
	require.NoError(t, err)

	// check there is 10 rows in the db
	connStr := fmt.Sprintf("postgres://admin:Password123@localhost:%v/dev?sslmode=disable", redshiftPort)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test;").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

func waitForRedshiftAndGetAddress(localstackPort string, redshiftPort string, clusterID string) (redshiftEndpointAddress string, err error) {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%v", localstackPort)}, nil
			},
		),
	}

	client := redshift.NewFromConfig(cfg)
	maxAttempts := 120
	for range maxAttempts {
		time.Sleep(time.Second)
		resp, err := client.DescribeClusters(context.Background(), &redshift.DescribeClustersInput{
			ClusterIdentifier: &clusterID,
		})
		if err != nil {
			continue
		}
		if len(resp.Clusters) == 0 {
			continue
		}
		if *resp.Clusters[0].ClusterStatus == "available" {
			time.Sleep(time.Second * 5) // redshift reports available somewhat prematurely
			for _, cluster := range resp.Clusters {
				if *cluster.ClusterIdentifier == "my-redshift-cluster" {
					redshiftEndpointAddress = fmt.Sprintf("%v:%v", *cluster.Endpoint.Address, redshiftPort)
				}
			}
			return redshiftEndpointAddress, nil
		}
	}
	return "", errors.New("polling localstack for ready Redshift failed")
}

func TestIntegrationRdsIamAuth(t *testing.T) {
	// TODO SKIP TEST - LOCALSTACK ISSUE
	t.Skip("issue with localstack and IAM Auth with RDS")

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	// we need a localstack pro image
	if os.Getenv("LOCALSTACK_AUTH_TOKEN") == "" {
		t.Skip("LOCALSTACK_AUTH_TOKEN is not set")
	}

	tfPath, err := filepath.Abs("./resources/rdsIamAuth.tf")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "localstack/localstack-pro",
		Env: []string{
			"LOCALSTACK_AUTH_TOKEN=" + os.Getenv("LOCALSTACK_AUTH_TOKEN"),
			"EXTENSION_AUTO_INSTALL=localstack-extension-terraform-init",
		},
		Mounts: []string{
			tfPath + ":/etc/localstack/init/ready.d/main.tf",
			"/var/run/docker.sock:/var/run/docker.sock",
		},
		ExposedPorts: []string{"4566/tcp", "4510-4559/tcp"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
	})

	// grab some ports and wait for the tf to boot up rds etc.
	localstackPort := resource.GetPort("4566/tcp")
	rdsPort := resource.GetPort("4510/tcp")

	_ = resource.Expire(900)

	err = waitForRds(localstackPort)
	require.NoError(t, err)

	// create user in DB
	connStr := fmt.Sprintf("postgres://masterusername:password123@localhost.localstack.cloud:%v/testdb?sslmode=disable", rdsPort)

	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE USER iam_user WITH LOGIN;")
	require.NoError(t, err)

	_, err = db.Exec("GRANT rds_iam TO iam_user;")
	require.NoError(t, err)

	// run a config that uses IAM authentication
	builder := service.NewStreamBuilder()

	err = builder.SetYAML(fmt.Sprintf(`input:
  generate:
    mapping: |
        root = {"age":random_int(min: 0, max: 100), "name":fake("name")}
    interval: 1ms
    count: 10
  
output:
  sql_insert:
    driver: postgres 
    dsn: postgresql://iam_user:password@localhost.localstack.cloud:%v/testdb?sslmode=disable
    table: test
    columns: [age, name]
    args_mapping: |
      root = [
        this.age,
        this.name,
      ]
    iam_enabled: true
    init_statement: "CREATE TABLE test (name varchar(255), age int);"`, rdsPort))

	stream, err := builder.Build()
	require.NoError(t, err)

	err = stream.Run(context.Background())
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test;").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

func waitForRds(localstackPort string) (err error) {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%v", localstackPort)}, nil
			},
		),
	}

	client := rds.NewFromConfig(cfg)
	maxAttempts := 120
	for range maxAttempts {
		time.Sleep(time.Second)

		instance := "my-rds-instance"
		resp, err := client.DescribeDBInstances(context.Background(), &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: &instance,
		})

		if err != nil {
			continue
		}
		if len(resp.DBInstances) == 0 {
			continue
		}
		if *resp.DBInstances[0].DBInstanceStatus == "available" {
			time.Sleep(5 * time.Second)
			return nil
		}
	}
	return errors.New("polling localstack for ready RDS failed")
}

// TODO(gregfurman): Generalise this into a testSuite that can be run against all SQL TestIntegration cases.
func TestIntegrationCheckReconnectLogic(t *testing.T) {
	integration.CheckSkip(t)

	freePortInt, err := integration.GetFreePort()
	require.NoError(t, err)
	freePort := strconv.Itoa(freePortInt)

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
				{
					HostIP:   "0.0.0.0",
					HostPort: freePort,
				},
			},
		},
		ExposedPorts: []string{"5432/tcp"},
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		first_name varchar(255),
		last_name varchar(255), 
		age int
		)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("test_data"); err != nil {
			return err
		}
		return nil
	}))

	// start a go routine to create a stream into db above
	sb := service.NewStreamBuilder()

	err = sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = {"first_name":fake("first_name"), "last_name":fake("last_name"), "age":random_int(min:0, max:99)}'
    interval: 1ns

output:
  sql_insert:
    driver: postgres
    dsn: %v
    table: test_data
    columns:
      - first_name
      - last_name
      - age
    args_mapping: |
        root = [
          this.first_name,
          this.last_name,
          this.age
        ]`, dsn))
	require.NoError(t, err)
	stream, err := sb.Build()
	streamCtx := t.Context()

	go func() {
		err := stream.Run(streamCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	}()

	// when we have over 1000 records in the db - stop the container
	var count int
	db, err = sql.Open("postgres", dsn)
	require.Eventually(t, func() bool {
		err := db.QueryRow("select count(*) from test_data;").Scan(&count)
		require.NoError(t, err)
		return count > 1000
	}, time.Minute, time.Second)

	err = pool.Client.StopContainer(resource.Container.ID, 60)
	require.NoError(t, err)

	err = pool.Client.StartContainer(resource.Container.ID, &docker.HostConfig{
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
				{
					HostIP:   "0.0.0.0",
					HostPort: freePort,
				},
			},
		},
	})
	require.NoError(t, err)

	// we should now have reconnected and have more rows in the table
	var countAfterRestart int
	require.Eventually(t, func() bool {
		//nolint:errcheck // We expect errors here whilst reconnecting
		db.QueryRow("select count(*) from test_data;").Scan(&countAfterRestart)
		return countAfterRestart > count+100
	}, time.Minute, time.Second)
}
