package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

type bigQuerySelectProcessorConfig struct {
	project string

	queryParts  *bqQueryParts
	jobLabels   map[string]string
	argsMapping *bloblang.Executor
	unsafeDyn   bool
}

func bigQuerySelectProcessorConfigFromParsed(inConf *service.ParsedConfig) (conf bigQuerySelectProcessorConfig, err error) {
	queryParts := bqQueryParts{}
	conf.queryParts = &queryParts

	if conf.project, err = inConf.FieldString("project"); err != nil {
		return
	}

	conf.unsafeDyn, err = inConf.FieldBool("unsafe_dynamic_query")
	if err != nil {
		return
	}

	if inConf.Contains("args_mapping") {
		if conf.argsMapping, err = inConf.FieldBloblang("args_mapping"); err != nil {
			return
		}
	}

	if conf.jobLabels, err = inConf.FieldStringMap("job_labels"); err != nil {
		return
	}

	if conf.unsafeDyn {
		if queryParts.tableDyn, err = inConf.FieldInterpolatedString("table"); err != nil {
			return
		}
		if inConf.Contains("where") {
			if queryParts.whereDyn, err = inConf.FieldInterpolatedString("where"); err != nil {
				return
			}
		}
		if inConf.Contains("columns_mapping") {
			if conf.queryParts.columnsMapping, err = inConf.FieldBloblang("columns_mapping"); err != nil {
				return
			}
		}
	} else {
		if queryParts.table, err = inConf.FieldString("table"); err != nil {
			return
		}
		if inConf.Contains("where") {
			if queryParts.where, err = inConf.FieldString("where"); err != nil {
				return
			}
		}
	}

	if inConf.Contains("columns") {
		if queryParts.columns, err = inConf.FieldStringList("columns"); err != nil {
			return
		}
	}

	if inConf.Contains("prefix") {
		queryParts.prefix, err = inConf.FieldString("prefix")
		if err != nil {
			return
		}
	}

	if inConf.Contains("suffix") {
		queryParts.suffix, err = inConf.FieldString("suffix")
		if err != nil {
			return
		}
	}

	// check config fields are being used appropriately
	if conf.queryParts.columnsMapping == nil && conf.unsafeDyn {
		err = errors.New("invalid gcp_bigquery_select config: unsafe_dynamic_query set to true but no columns_mapping provided")
		return
	}
	if inConf.Contains("columns_mapping") && !conf.unsafeDyn {
		err = errors.New("invalid gcp_bigquery_select config: unsafe_dynamic_query set to false but columns_mapping provided")
		return
	}
	if conf.queryParts.columnsMapping != nil && len(conf.queryParts.columns) != 0 {
		err = errors.New("invalid gcp_bigquery_select config: cannot set both columns_mapping and columns field")
		return
	}

	return
}

func newBigQuerySelectProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.0.0").
		Stable().
		Categories("Integration").
		Summary("Executes a `SELECT` query against BigQuery and replaces messages with the rows returned.").
		Field(service.NewStringField("project").Description("GCP project where the query job will execute.")).
		Field(service.NewInterpolatedStringField("table").Description("Fully-qualified BigQuery table name to query.").Example("bigquery-public-data.samples.shakespeare")).
		Field(service.NewStringListField("columns").
			Description("A list of columns to query.").
			Optional()).
		Field(service.NewBloblangField("columns_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of column names to query.").
			Optional().
			Version("1.5.0").
			Advanced()).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks (`?`).").
			Example("type = ? and created_at > ?").
			Example("user_id = ?").
			Optional(),
		).
		Field(service.NewStringMapField("job_labels").Description("A list of labels to add to the query job.").Default(map[string]any{})).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example(`root = [ "article", now().ts_format("2006-01-02") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the select query (before SELECT).").
			Optional()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional()).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable [interpolation functions](/docs/configuration/interpolation/#bloblang-queries) in the columns_mapping, table & where fields. When `unsafe_dynamic_query` is set to true, you should provide a bloblang mapping via the `columns_mapping` config field, and not `columns`. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false).
			Version("1.5.0").
			Optional()).
		Example("Word count",
			`
Given a stream of English terms, enrich the messages with the word count from Shakespeare's public works:`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - gcp_bigquery_select:
              project: test-project
              table: bigquery-public-data.samples.shakespeare
              columns:
                - word
                - sum(word_count) as total_count
              where: word = ?
              suffix: |
                GROUP BY word
                ORDER BY total_count DESC
                LIMIT 10
              args_mapping: root = [ this.term ]
        result_map: |
          root.count = this.get("0.total_count")
`,
		).
		Example("Unsafe Dynamic Query",
			`
An example to show the use of the unsafe_dynamic_query field:`,
			`
# {"table": "test.people", "columns": ["name", "age", "city"], "args": ["London", "Paris", "Dublin"]}
pipeline:
  processors:
    - gcp_bigquery_select:
        project: ${GCP_PROJECT}
        table: ${! this.table } # test.people
        columns_mapping: root = this.columns #["name", "age", "city"]
        where:  ${! "city IN ("+"?,".repeat(this.args.length()-1)+"?)" } # city IN (?,?,?)
        args_mapping: root = this.args # ["London", "Paris", "Dublin"]
        unsafe_dynamic_query: true
`,
		)
}

type bigQueryProcessorOptions struct {
	logger *service.Logger

	// Allows passing additional to the underlying BigQuery client.
	// Useful when writing tests.
	clientOptions []option.ClientOption
}

type bigQuerySelectProcessor struct {
	logger   *service.Logger
	config   *bigQuerySelectProcessorConfig
	client   bqClient
	closeCtx context.Context
	closeF   context.CancelFunc
}

func newBigQuerySelectProcessor(inConf *service.ParsedConfig, options *bigQueryProcessorOptions) (*bigQuerySelectProcessor, error) {
	conf, err := bigQuerySelectProcessorConfigFromParsed(inConf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	closeCtx, closeF := context.WithCancel(context.Background())

	wrapped, err := bigquery.NewClient(closeCtx, conf.project, options.clientOptions...)
	if err != nil {
		closeF()
		return nil, fmt.Errorf("failed to create bigquery client: %w", err)
	}

	client := wrapBQClient(wrapped, options.logger)

	return &bigQuerySelectProcessor{
		logger:   options.logger,
		config:   &conf,
		client:   client,
		closeCtx: closeCtx,
		closeF:   closeF,
	}, nil
}

func (proc *bigQuerySelectProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	argsMapping := proc.config.argsMapping
	columnsMapping := proc.config.queryParts.columnsMapping

	outBatch := make(service.MessageBatch, 0, len(batch))

	var argsMappingExecutor *service.MessageBatchBloblangExecutor
	if argsMapping != nil {
		argsMappingExecutor = batch.BloblangExecutor(argsMapping)
	}

	var columnsMappingExecutor *service.MessageBatchBloblangExecutor
	if columnsMapping != nil {
		columnsMappingExecutor = batch.BloblangExecutor(columnsMapping)
	}

	for i, msg := range batch {
		outBatch = append(outBatch, msg)

		if proc.config.unsafeDyn {
			err := proc.resolveUnsafeDynamicFields(
				msg,
				columnsMappingExecutor,
				i,
			)
			if err != nil {
				msg.SetError(err)
				continue
			}
		}

		var args []any
		if argsMapping != nil {
			resMsg, err := argsMappingExecutor.Query(i)
			if err != nil {
				msg.SetError(fmt.Errorf("failed to resolve args mapping: %w", err))
				continue
			}

			iargs, err := resMsg.AsStructured()
			if err != nil {
				msg.SetError(fmt.Errorf("args mapping returned non-structured result: %w", err))
				continue
			}

			var ok bool
			if args, ok = iargs.([]any); !ok {
				msg.SetError(fmt.Errorf("args mapping returned non-array result: %T", iargs))
				continue
			}
		}

		iter, err := proc.client.RunQuery(ctx, &bqQueryBuilderOptions{
			queryParts: proc.config.queryParts,
			jobLabels:  proc.config.jobLabels,
			args:       args,
		})
		if err != nil {
			msg.SetError(err)
			continue
		}

		rows, err := consumeIterator(iter)
		if err != nil {
			msg.SetError(fmt.Errorf("failed to read all rows: %w", err))
			continue
		}

		bs, err := json.Marshal(rows)
		if err != nil {
			msg.SetError(fmt.Errorf("failed to marshal rows to json: %w", err))
			continue
		}

		msg.SetBytes(bs)
	}

	return []service.MessageBatch{outBatch}, nil
}

func (proc *bigQuerySelectProcessor) Close(ctx context.Context) error {
	proc.closeF()
	return nil
}

func consumeIterator(iter bigqueryIterator) ([]map[string]bigquery.Value, error) {
	var rows []map[string]bigquery.Value

	for {
		var row map[string]bigquery.Value
		err := iter.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func init() {
	err := service.RegisterBatchProcessor(
		"gcp_bigquery_select", newBigQuerySelectProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newBigQuerySelectProcessor(conf, &bigQueryProcessorOptions{
				logger: mgr.Logger(),
			})
		})
	if err != nil {
		panic(err)
	}
}

func (proc *bigQuerySelectProcessor) resolveUnsafeDynamicFields(
	msg *service.Message,
	columnsMappingExecutor *service.MessageBatchBloblangExecutor,
	i int,
) (err error) {
	proc.config.queryParts.table, err = proc.config.queryParts.tableDyn.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to resolve table mapping: %w", err)
	}
	proc.config.queryParts.where, err = proc.config.queryParts.whereDyn.TryString(msg)
	if err != nil {
		return fmt.Errorf("failed to resolve where mapping: %w", err)
	}

	resMsg, err := columnsMappingExecutor.Query(i)
	if err != nil {
		return fmt.Errorf("failed to resolve columns mapping: %w", err)
	}

	icols, err := resMsg.AsStructured()
	if err != nil {
		return fmt.Errorf("mapping returned non-structured result: %w", err)
	}
	cols, ok := icols.([]any)
	if !ok {
		return fmt.Errorf("col mapping returned non-array result: %T", icols)
	}

	proc.config.queryParts.columns, err = toStringSlice(cols)
	if err != nil {
		return err
	}

	return nil
}

func toStringSlice(in []any) (out []string, err error) {
	if in == nil {
		return nil, errors.New("column mapping returned nil")
	}
	out = make([]string, len(in))
	for i, v := range in {
		out[i] = fmt.Sprint(v)
	}
	return out, nil
}
