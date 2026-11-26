package iceberg

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud/s3" // registers the s3:// file IO
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"

	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fieldCatalog              = "catalog"
	fieldCatalogType          = "type"
	fieldCatalogGlue          = "glue"
	fieldCatalogGlueCatalogID = "catalog_id"
	fieldCatalogRest          = "rest"
	fieldCatalogRestURI       = "uri"
	fieldCatalogRestWarehouse = "warehouse"
	fieldCatalogRestPrefix    = "prefix"
	fieldCatalogRestCred      = "credential"
	fieldCatalogRestToken     = "token"
	fieldCatalogRestSigV4     = "sigv4_service"
	fieldWarehouse            = "warehouse"
	fieldNamespace            = "namespace"
	fieldTable                = "table"
	fieldSchema               = "schema"
	fieldSchemaName           = "name"
	fieldSchemaType           = "type"
	fieldPartitionBy          = "partition_by"
	fieldTableProperties      = "table_properties"
	fieldUnknownFields        = "unknown_fields"
	fieldForcePathStyleURLs   = "force_path_style_urls"
	fieldBatching             = "batching"

	catalogGlue = "glue"
	catalogRest = "rest"

	unknownFieldsReject = "reject"
	unknownFieldsIgnore = "ignore"

	// Count-based by default: a byte_size policy has to JSON-encode every
	// structured message just to measure it.
	defaultBatchCount  = 100000
	defaultBatchPeriod = "60s"
	maxCommitAttempts  = 5
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("1.24.0").
		Summary("Appends structured messages to an [Apache Iceberg](https://iceberg.apache.org/) table registered in an AWS Glue or Iceberg REST catalog.").
		Description(`
Each message must be a structured object. Its fields are written to the table columns with the same names, as Parquet data files plus Iceberg metadata. The table is append-only: rows are never updated or deleted.

### Schema

The table is the source of truth for the schema:

- If the table exists, its schema is used and the `+"`schema`"+` field is ignored. Columns added to the table later (for example with `+"`ALTER TABLE`"+` from a query engine) are picked up automatically.
- If the table doesn't exist, it is created, along with its namespace, from the `+"`schema`"+`, `+"`partition_by`"+` and `+"`table_properties`"+` fields.

Types are never inferred from the data. Supported column types are `+"`boolean`"+`, `+"`int`"+`, `+"`long`"+`, `+"`float`"+`, `+"`double`"+`, `+"`string`"+`, `+"`date`"+`, `+"`timestamp`"+` and `+"`timestamptz`"+`. Values are converted where that is lossless: numbers and booleans may arrive as strings, and timestamps as RFC 3339 strings. Objects and arrays written to a `+"`string`"+` column are stored as JSON. A message field with no value writes a null.

A message that can't be written (a value of the wrong type, or with `+"`unknown_fields: reject`"+` a field the table has no column for) fails on its own; the rest of the batch is still committed.

### Catalogs

- `+"`glue`"+`: the AWS Glue Data Catalog. The table is created at `+"`{warehouse}/{namespace}/{table}`"+`.
- `+"`rest`"+`: any [Iceberg REST catalog](https://iceberg.apache.org/rest-catalog-spec/), such as Amazon S3 Tables, Apache Polaris, Snowflake Open Catalog or Lakekeeper. If `+"`warehouse`"+` is empty, the catalog chooses the table location.

Data files are written to S3 with the AWS settings of this output.

### Writing to another AWS account

To write a table owned by another AWS account (both the bucket and the Glue catalog), set `+"`credentials.role`"+` (and `+"`credentials.role_external_id`"+`) to a role in that account. The catalog and the data files then both use the assumed role, and `+"`catalog.glue.catalog_id`"+` can stay empty. The role needs:

- Glue: `+"`glue:GetDatabase`"+`, `+"`glue:CreateDatabase`"+`, `+"`glue:GetTable`"+`, `+"`glue:CreateTable`"+` and `+"`glue:UpdateTable`"+` on the catalog, the database and its tables.
- S3: `+"`s3:ListBucket`"+` on the bucket (limited to the table prefix), and `+"`s3:GetObject`"+`, `+"`s3:PutObject`"+` and `+"`s3:DeleteObject`"+` on the table prefix.
- If the account uses AWS Lake Formation, Lake Formation permissions on the database and the data location as well.

### Delivery guarantees

A batch is acknowledged only after its rows are committed to the table, so delivery is at-least-once: a crash between commit and acknowledgement replays the batch and its rows are written again. Messages that can't be written fail individually and can be routed elsewhere with a `+"[`fallback`](/docs/components/outputs/fallback)"+` output.

If the catalog or the bucket can't be reached, the whole batch fails and is retried, so messages wait in the input (for example a Kafka topic or Kinesis stream) until the table is writable again. Make the input's retention longer than the longest outage you want to ride out. To bound the wait instead, wrap this output in a `+"[`retry`](/docs/components/outputs/retry)"+` output with `+"`backoff.max_elapsed_time`"+` and put another output after it in a `+"`fallback`"+`, as in the examples.

Several instances of this output, in one or many Bento processes, can write to the same table. Each batch is committed as its own snapshot and conflicting commits are retried.

### Catalog backup

A catalog entry only points at the table's current metadata file, which is stored with the data. After every commit the output also writes `+"`{table location}/_bento/commits/v{version}.json`"+`, naming that file; the latest 100 are kept. If the table disappears from the catalog while its files remain, the output registers the table again from the latest of these instead of creating a new, empty table over it. If the files show committed data but no such backup exists, the output refuses to connect until the table is registered by hand. The AWS credentials therefore need to list, read and delete objects under the table location, as well as write them.

### Performance

Each batch becomes one Parquet file per partition, so batch size is file size. The defaults batch 100,000 messages or 60 seconds, whichever comes first. Prefer `+"`count`"+` over `+"`byte_size`"+`: measuring the byte size of a structured message means encoding it to JSON first.`).
		Fields(
			service.NewObjectField(fieldCatalog,
				service.NewStringEnumField(fieldCatalogType, catalogGlue, catalogRest).
					Description("The catalog the table is registered in.").
					Default(catalogGlue),
				service.NewObjectField(fieldCatalogGlue,
					service.NewStringField(fieldCatalogGlueCatalogID).
						Description("The ID of the Glue Data Catalog (an AWS account ID). Defaults to the catalog of the account in use.").
						Default(""),
				).Description("Settings for the `glue` catalog."),
				service.NewObjectField(fieldCatalogRest,
					service.NewStringField(fieldCatalogRestURI).
						Description("The base URI of the REST catalog.").
						Default("").
						Example("https://catalog.example.com/iceberg"),
					service.NewStringField(fieldCatalogRestWarehouse).
						Description("The catalog's warehouse identifier, if it needs one.").
						Default(""),
					service.NewStringField(fieldCatalogRestPrefix).
						Description("An optional prefix for REST catalog requests.").
						Default("").
						Advanced(),
					service.NewStringField(fieldCatalogRestCred).
						Description("OAuth2 client credentials, as `client_id:client_secret`.").
						Default("").
						Secret(),
					service.NewStringField(fieldCatalogRestToken).
						Description("A bearer token for the REST catalog.").
						Default("").
						Secret(),
					service.NewStringField(fieldCatalogRestSigV4).
						Description("Signs REST catalog requests with AWS SigV4 for this service, for example `glue` for the Glue Iceberg REST endpoint or `s3tables` for Amazon S3 Tables. Empty disables SigV4.").
						Default("").
						Advanced(),
				).Description("Settings for the `rest` catalog."),
			).Description("The Iceberg catalog."),
			service.NewStringField(fieldWarehouse).
				Description("The base `s3://` location for the table, which is created at `{warehouse}/{namespace}/{table}`. Required for the `glue` catalog; optional for `rest`.").
				Default("").
				Example("s3://my-lake").
				Example("s3://my-lake/events"),
			service.NewStringField(fieldNamespace).
				Description("The namespace (Glue database) of the table.").
				Example("analytics"),
			service.NewStringField(fieldTable).
				Description("The table name."),
			service.NewObjectListField(fieldSchema,
				service.NewStringField(fieldSchemaName).
					Description("The column name, which is also the message field it is filled from."),
				service.NewStringEnumField(fieldSchemaType, columnTypeNames()...).
					Description("The Iceberg type of the column."),
			).
				Description("The columns of the table, used only to create it. Required if the table doesn't exist yet; ignored if it does.").
				Default([]any{}).
				Example([]any{
					map[string]any{"name": "id", "type": "string"},
					map[string]any{"name": "created_at", "type": "timestamptz"},
					map[string]any{"name": "amount", "type": "double"},
				}),
			service.NewStringListField(fieldPartitionBy).
				Description("How to partition the table, used only to create it. Each entry is a column name, or a transform of one: `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)` or `truncate(N, col)`.").
				Default([]any{}).
				Example([]any{"day(created_at)"}).
				Example([]any{"country", "bucket(16, id)"}),
			service.NewStringMapField(fieldTableProperties).
				Description("[Iceberg table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties) to set when creating the table, on top of this output's defaults (format version 2, zstd-compressed Parquet).").
				Default(map[string]any{}).
				Example(map[string]any{"write.metadata.metrics.default": "counts"}).
				Advanced(),
			service.NewStringEnumField(fieldUnknownFields, unknownFieldsReject, unknownFieldsIgnore).
				Description("What to do with a message that has a field the table has no column for: `reject` fails the message, `ignore` writes it without that field.").
				Default(unknownFieldsReject),
			service.NewBoolField(fieldForcePathStyleURLs).
				Description("Forces path style URLs for S3 requests, as required by some S3-compatible stores.").
				Default(false).
				Advanced(),
			service.NewOutputMaxInFlightField().Default(2),
			service.NewBatchPolicyField(fieldBatching).
				Description("Allows you to configure a [batching policy](/docs/configuration/batching). Each batch is written as one Parquet file per partition. If no batching is configured, batches of 100,000 messages or 60 seconds are used."),
		).
		Fields(config.SessionFields()...).
		Example(
			"Glue",
			"Creates the table if needed and appends to it. Messages that don't fit the table go to a bucket instead of being dropped.",
			`
output:
  fallback:
    - iceberg:
        catalog:
          type: glue
        warehouse: s3://my-lake
        namespace: analytics
        table: orders
        region: us-east-1
        partition_by:
          - day(created_at)
        schema:
          - { name: id, type: string }
          - { name: created_at, type: timestamptz }
          - { name: customer_id, type: long }
          - { name: amount, type: double }
          - { name: items, type: string } # an array, stored as JSON
    - aws_s3:
        bucket: my-bad-rows
        path: orders/${! timestamp_unix_nano() }.json
`,
		).
		Example(
			"Glue in another AWS account",
			"Appends to an existing table in a bucket and Glue catalog owned by another AWS account by assuming a role there. If the table can't be written for 30 minutes, messages are kept in a bucket of this account to be replayed later.",
			`
output:
  fallback:
    - retry:
        backoff:
          max_elapsed_time: 30m
        output:
          iceberg:
            catalog:
              type: glue
            warehouse: s3://customer-lake
            namespace: analytics
            table: orders
            region: us-east-1
            credentials:
              role: arn:aws:iam::123456789012:role/iceberg-writer
              role_external_id: ${ROLE_EXTERNAL_ID}
    - aws_s3:
        bucket: my-iceberg-spill
        path: orders/${! timestamp_unix_nano() }-${! uuid_v4() }.json
        region: us-east-1
`,
		).
		Example(
			"REST catalog",
			"Appends to a table in a REST catalog that chooses table locations itself.",
			`
output:
  iceberg:
    catalog:
      type: rest
      rest:
        uri: https://catalog.example.com/iceberg
        warehouse: my_warehouse
        credential: ${CATALOG_CLIENT_ID}:${CATALOG_CLIENT_SECRET}
    namespace: analytics
    table: orders
    region: us-east-1
    schema:
      - { name: id, type: string }
      - { name: created_at, type: timestamptz }
      - { name: amount, type: double }
`,
		).
		Example(
			"OpenSnowcat enriched events",
			"Writes the `enriched_json` events of the [`opensnowcat` processor](/docs/components/processors/opensnowcat) to a table with one column per event field. `contexts`, `derived_contexts` and `unstruct_event` are objects, stored as JSON strings. The schema is shortened here: an event has 131 fields, and the complete config is in [`config/examples/opensnowcat_iceberg.yaml`](https://github.com/warpstreamlabs/bento/blob/main/config/examples/opensnowcat_iceberg.yaml).",
			`
pipeline:
  processors:
    - opensnowcat:
        output_format: enriched_json

output:
  fallback:
    - iceberg:
        catalog:
          type: glue
        warehouse: s3://my-lake
        namespace: analytics
        table: events
        region: us-east-1
        partition_by:
          - day(collector_tstamp)
        schema:
          - { name: app_id, type: string }
          - { name: platform, type: string }
          - { name: collector_tstamp, type: timestamptz }
          - { name: event, type: string }
          - { name: event_id, type: string }
          - { name: user_id, type: string }
          - { name: domain_userid, type: string }
          - { name: page_url, type: string }
          - { name: contexts, type: string }
          - { name: unstruct_event, type: string }
          - { name: derived_contexts, type: string }
          # ...one entry per enriched_json field
    # Events that don't fit the table are kept instead of dropped.
    - aws_s3:
        bucket: my-bad-rows
        path: opensnowcat/${! timestamp_unix_nano() }.json
`,
		)
}

func init() {
	err := service.RegisterBatchOutput("iceberg", outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, policy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if policy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
				return
			}
			if policy.IsNoop() {
				policy.Count = defaultBatchCount
				policy.Period = defaultBatchPeriod
			}
			out, err = newOutputFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

// icebergCatalog is the part of catalog.Catalog the output uses.
type icebergCatalog interface {
	CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error)
	CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error
	LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error)
	CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error)
	RegisterTable(ctx context.Context, identifier table.Identifier, metadataLocation string) (*table.Table, error)
}

// icebergConn is what Connect needs: the catalog, plus the AWS config and
// properties for iceberg-go's file IO.
type icebergConn struct {
	catalog icebergCatalog
	awsConf *aws.Config
	ioProps iceberg.Properties
}

type icebergOutput struct {
	conf     *service.ParsedConfig
	ident    table.Identifier
	location string // empty: the catalog chooses it

	// Used only to create the table. schema is nil if none is configured.
	schema        *iceberg.Schema
	partitionSpec iceberg.PartitionSpec
	tableProps    iceberg.Properties

	rejectUnknown bool

	// newCatalog builds the catalog on Connect. Tests replace it.
	newCatalog func(ctx context.Context) (icebergConn, error)

	log            *service.Logger
	mRows          *service.MetricCounter
	mFiles         *service.MetricCounter
	mRowErrors     *service.MetricCounter
	mCommitRetries *service.MetricCounter
	mCommitLatency *service.MetricTimer
	mPointerErrors *service.MetricCounter

	mu   sync.Mutex
	conn icebergConn
	tbl  *table.Table
}

func newOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*icebergOutput, error) {
	namespace, err := conf.FieldString(fieldNamespace)
	if err != nil {
		return nil, err
	}
	tableName, err := conf.FieldString(fieldTable)
	if err != nil {
		return nil, err
	}
	warehouse, err := conf.FieldString(fieldWarehouse)
	if err != nil {
		return nil, err
	}
	catType, err := conf.FieldString(fieldCatalog, fieldCatalogType)
	if err != nil {
		return nil, err
	}
	if catType == catalogRest {
		if uri, _ := conf.FieldString(fieldCatalog, fieldCatalogRest, fieldCatalogRestURI); uri == "" {
			return nil, errors.New("catalog.rest.uri is required with the rest catalog")
		}
	}
	location, err := tableLocation(catType, warehouse, namespace, tableName)
	if err != nil {
		return nil, err
	}
	unknown, err := conf.FieldString(fieldUnknownFields)
	if err != nil {
		return nil, err
	}

	m := mgr.Metrics()
	o := &icebergOutput{
		conf:           conf,
		ident:          table.Identifier{namespace, tableName},
		location:       location,
		rejectUnknown:  unknown == unknownFieldsReject,
		log:            mgr.Logger(),
		mRows:          m.NewCounter("iceberg_rows_written"),
		mFiles:         m.NewCounter("iceberg_files_written"),
		mRowErrors:     m.NewCounter("iceberg_row_errors"),
		mCommitRetries: m.NewCounter("iceberg_commit_retries"),
		mCommitLatency: m.NewTimer("iceberg_commit_latency_ns"),
		mPointerErrors: m.NewCounter("iceberg_commit_pointer_errors"),
	}
	if err := o.parseTableConfig(conf); err != nil {
		return nil, err
	}
	o.newCatalog = o.catalogFromConfig
	return o, nil
}

// parseTableConfig reads the fields used to create the table.
func (o *icebergOutput) parseTableConfig(conf *service.ParsedConfig) error {
	colConfs, err := conf.FieldObjectList(fieldSchema)
	if err != nil {
		return err
	}
	partitionBy, err := conf.FieldStringList(fieldPartitionBy)
	if err != nil {
		return err
	}
	props, err := conf.FieldStringMap(fieldTableProperties)
	if err != nil {
		return err
	}
	o.tableProps = defaultTableProperties()
	maps.Copy(o.tableProps, props)

	if len(colConfs) == 0 {
		if len(partitionBy) > 0 {
			return errors.New("partition_by is only used to create the table and needs a schema")
		}
		return nil
	}
	cols := make([]columnConfig, 0, len(colConfs))
	for _, c := range colConfs {
		var col columnConfig
		if col.name, err = c.FieldString(fieldSchemaName); err != nil {
			return err
		}
		if col.typ, err = c.FieldString(fieldSchemaType); err != nil {
			return err
		}
		cols = append(cols, col)
	}
	if o.schema, err = schemaFromConfig(cols); err != nil {
		return err
	}
	o.partitionSpec, err = partitionSpecFromConfig(partitionBy, o.schema)
	return err
}

// tableLocation validates the names and returns the table location, or ""
// when the (REST) catalog should choose it.
func tableLocation(catType, warehouse, namespace, tableName string) (string, error) {
	for field, v := range map[string]string{fieldNamespace: namespace, fieldTable: tableName} {
		if v == "" || strings.ContainsAny(v, "/\\.") {
			return "", fmt.Errorf("%s %q must be a non-empty name without slashes or dots", field, v)
		}
	}
	if warehouse == "" {
		if catType == catalogGlue {
			return "", errors.New("warehouse is required with the glue catalog")
		}
		return "", nil
	}
	u, err := url.Parse(warehouse)
	if err != nil {
		return "", fmt.Errorf("invalid warehouse %q: %w", warehouse, err)
	}
	if u.Scheme == "" || (strings.HasPrefix(u.Scheme, "s3") && u.Host == "") {
		return "", fmt.Errorf("warehouse %q must be a URL with a bucket, such as s3://my-lake", warehouse)
	}
	return strings.TrimSuffix(warehouse, "/") + "/" + namespace + "/" + tableName, nil
}

// catalogFromConfig builds the Glue or REST catalog from the config.
func (o *icebergOutput) catalogFromConfig(ctx context.Context) (icebergConn, error) {
	awsConf, err := baws.GetSession(ctx, o.conf)
	if err != nil {
		return icebergConn{}, err
	}

	// Properties for iceberg-go's S3 file IO. Credentials come from awsConf,
	// passed through the context (see withAWS).
	ioProps := iceberg.Properties{}
	if forcePath, _ := o.conf.FieldBool(fieldForcePathStyleURLs); forcePath {
		ioProps[icebergio.S3ForceVirtualAddressing] = "false"
	}
	if awsConf.BaseEndpoint != nil {
		ioProps[icebergio.S3EndpointURL] = *awsConf.BaseEndpoint
	}

	catType, _ := o.conf.FieldString(fieldCatalog, fieldCatalogType)
	switch catType {
	case catalogGlue:
		props := glue.AwsProperties(ioProps)
		if id, _ := o.conf.FieldString(fieldCatalog, fieldCatalogGlue, fieldCatalogGlueCatalogID); id != "" {
			props[glue.CatalogIdKey] = id
		}
		cat, err := glue.NewCatalog(glue.WithAwsConfig(awsConf), glue.WithAwsProperties(props))
		if err != nil {
			return icebergConn{}, err
		}
		return icebergConn{catalog: cat, awsConf: &awsConf, ioProps: ioProps}, nil

	case catalogRest:
		restConf := o.conf.Namespace(fieldCatalog, fieldCatalogRest)
		uri, _ := restConf.FieldString(fieldCatalogRestURI)
		opts := []rest.Option{rest.WithAwsConfig(awsConf), rest.WithAdditionalProps(ioProps)}
		if v, _ := restConf.FieldString(fieldCatalogRestWarehouse); v != "" {
			opts = append(opts, rest.WithWarehouseLocation(v))
		}
		if v, _ := restConf.FieldString(fieldCatalogRestPrefix); v != "" {
			opts = append(opts, rest.WithPrefix(v))
		}
		if v, _ := restConf.FieldString(fieldCatalogRestCred); v != "" {
			opts = append(opts, rest.WithCredential(v))
		}
		if v, _ := restConf.FieldString(fieldCatalogRestToken); v != "" {
			opts = append(opts, rest.WithOAuthToken(v))
		}
		if v, _ := restConf.FieldString(fieldCatalogRestSigV4); v != "" {
			opts = append(opts, rest.WithSigV4RegionSvc(awsConf.Region, v))
		}
		cat, err := rest.NewCatalog(ctx, "bento", uri, opts...)
		if err != nil {
			return icebergConn{}, err
		}
		return icebergConn{catalog: cat, awsConf: &awsConf, ioProps: ioProps}, nil
	}
	return icebergConn{}, fmt.Errorf("unsupported catalog type %q", catType)
}

// withAWS makes the output's AWS config available to iceberg-go's S3 file IO,
// so data files use the same credentials (including assumed roles) as the
// catalog.
func (o *icebergOutput) withAWS(ctx context.Context) context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.conn.awsConf == nil {
		return ctx
	}
	return utils.WithAwsConfig(ctx, o.conn.awsConf)
}

func (o *icebergOutput) Connect(ctx context.Context) error {
	conn, err := o.newCatalog(ctx)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.conn = conn
	o.mu.Unlock()

	ctx = o.withAWS(ctx)
	tbl, err := o.loadOrCreateTable(ctx, conn)
	if err != nil {
		return fmt.Errorf("loading or creating table %s: %w", strings.Join(o.ident, "."), err)
	}
	// Fail now, rather than on every batch, if the table can't be written.
	enc, err := newRowEncoder(memory.DefaultAllocator, tbl.Schema(), o.rejectUnknown)
	if err != nil {
		return fmt.Errorf("table %s: %w", strings.Join(o.ident, "."), err)
	}
	enc.Release()

	// Back up the version just loaded too, so a table is recoverable even
	// before this output's first commit.
	o.writeCommitPointer(ctx, tbl)
	o.setTable(tbl)
	return nil
}

// loadOrCreateTable loads the table, or, if the catalog has no entry for it:
//
//   - re-registers it from its latest commit pointer when it already exists at
//     its location (the catalog entry was lost; the data was not);
//   - refuses when files at the location show committed data but no pointer
//     says which version is current;
//   - otherwise creates it from the configured schema.
//
// It is safe to run from many replicas at once: whoever loses the race to
// create the namespace or the table loads it instead.
func (o *icebergOutput) loadOrCreateTable(ctx context.Context, conn icebergConn) (*table.Table, error) {
	cat := conn.catalog
	tbl, err := cat.LoadTable(ctx, o.ident)
	if err == nil {
		return tbl, nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) && !errors.Is(err, catalog.ErrNoSuchNamespace) {
		return nil, err
	}

	// Without a configured location (a REST catalog choosing it), a new table
	// can't land on top of an old one, so there is nothing to recover.
	var metadataLocation string
	if o.location != "" {
		fsys, err := icebergio.LoadFS(ctx, conn.ioProps, o.location)
		if err != nil {
			return nil, err
		}
		if metadataLocation, err = findCommittedMetadata(fsys, o.location); err != nil {
			return nil, err
		}
	}
	if metadataLocation == "" && o.schema == nil {
		return nil, errors.New("the table doesn't exist and no schema is configured to create it")
	}

	ns := table.Identifier{o.ident[0]}
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := cat.CreateNamespace(ctx, ns, nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil, fmt.Errorf("creating namespace: %w", err)
		}
	}

	if metadataLocation != "" {
		o.log.Warnf("Table %s is missing from the catalog but exists at %s; registering it again from %s", strings.Join(o.ident, "."), o.location, metadataLocation)
		tbl, err = cat.RegisterTable(ctx, o.ident, metadataLocation)
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			return cat.LoadTable(ctx, o.ident)
		}
		if err != nil {
			return nil, fmt.Errorf("registering table from %s: %w", metadataLocation, err)
		}
		return tbl, nil
	}

	opts := []catalog.CreateTableOpt{
		catalog.WithPartitionSpec(&o.partitionSpec),
		catalog.WithProperties(o.tableProps),
	}
	if o.location != "" {
		opts = append(opts, catalog.WithLocation(o.location))
	}
	tbl, err = cat.CreateTable(ctx, o.ident, o.schema, opts...)
	if errors.Is(err, catalog.ErrTableAlreadyExists) {
		return cat.LoadTable(ctx, o.ident)
	}
	return tbl, err
}

func (o *icebergOutput) currentTable() *table.Table {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.tbl
}

func (o *icebergOutput) currentCatalog() icebergCatalog {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.conn.catalog
}

// writeCommitPointer backs up tbl's metadata location (see recovery.go). A
// failure is logged, not returned: the commit has already succeeded, and
// failing the batch would write its rows twice.
func (o *icebergOutput) writeCommitPointer(ctx context.Context, tbl *table.Table) {
	if err := writeCommitPointer(ctx, tbl); err != nil {
		o.mPointerErrors.Incr(1)
		o.log.Warnf("Failed to write the commit pointer for %s version %d: %v", strings.Join(o.ident, "."), tbl.Metadata().LastSequenceNumber(), err)
	}
}

func (o *icebergOutput) setTable(tbl *table.Table) {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Concurrent batches may commit out of order; keep the newest.
	if o.tbl == nil || tbl.Metadata().LastSequenceNumber() >= o.tbl.Metadata().LastSequenceNumber() {
		o.tbl = tbl
	}
}

// encodedBatch is a batch converted to rows of one table schema.
type encodedBatch struct {
	enc      *rowEncoder
	rows     int
	batchErr *service.BatchError
	// unknownField is set if a message was rejected for a field the table has
	// no column for.
	unknownField bool
}

func (o *icebergOutput) encode(tbl *table.Table, batch service.MessageBatch) (*encodedBatch, error) {
	enc, err := newRowEncoder(memory.DefaultAllocator, tbl.Schema(), o.rejectUnknown)
	if err != nil {
		return nil, err
	}
	eb := &encodedBatch{enc: enc}
	for i, msg := range batch {
		if err := appendMessage(enc, msg); err != nil {
			if eb.batchErr == nil {
				eb.batchErr = service.NewBatchError(batch, errors.New("some messages could not be written to the Iceberg table"))
			}
			eb.batchErr.Failed(i, err)
			eb.unknownField = eb.unknownField || errors.Is(err, errUnknownField)
			o.log.Debugf("Rejecting message %d: %v", i, err)
			continue
		}
		eb.rows++
	}
	return eb, nil
}

func (o *icebergOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	tbl := o.currentTable()
	if tbl == nil {
		return service.ErrNotConnected
	}
	ctx = o.withAWS(ctx)

	eb, err := o.encode(tbl, batch)
	if err != nil {
		return err
	}
	if eb.unknownField {
		// The table may have gained the column since it was loaded.
		fresh, err := o.currentCatalog().LoadTable(ctx, o.ident)
		if err != nil {
			o.log.Debugf("Could not reload the table to check for new columns: %v", err)
		} else if fresh.Schema().ID != tbl.Schema().ID {
			eb.enc.Release()
			o.setTable(fresh)
			tbl = fresh
			if eb, err = o.encode(tbl, batch); err != nil {
				return err
			}
		}
	}
	defer eb.enc.Release()

	if eb.batchErr != nil {
		o.mRowErrors.Incr(int64(eb.batchErr.IndexedErrors()))
	}
	if eb.rows > 0 {
		// WriteRecords releases the record batch once it's consumed.
		if err := o.writeAndCommit(ctx, tbl, eb.enc.NewRecordBatch()); err != nil {
			return err
		}
		o.mRows.Incr(int64(eb.rows))
	}
	if eb.batchErr != nil {
		return eb.batchErr
	}
	return nil
}

func appendMessage(enc *rowEncoder, msg *service.Message) error {
	v, err := msg.AsStructured()
	if err != nil {
		return fmt.Errorf("message is not structured: %w", err)
	}
	row, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("message is not an object, got %T", v)
	}
	return enc.Append(row)
}

// writeAndCommit writes the rows as Parquet files, then commits them in one
// snapshot. The files are written once; only the commit is retried.
func (o *icebergOutput) writeAndCommit(ctx context.Context, tbl *table.Table, rec arrow.RecordBatch) error {
	batchID := uuid.New()

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, rec.Schema(), records, table.WithWriteUUID(batchID)) {
		if err != nil {
			return fmt.Errorf("writing Parquet files: %w", err)
		}
		files = append(files, df)
	}
	if len(files) == 0 {
		return nil
	}

	start := time.Now()
	props := iceberg.Properties{"bento.batch-id": batchID.String()}
	for attempt := 1; ; attempt++ {
		tx := tbl.NewTransaction()
		// Each batch adds files no other batch has, so the (expensive) scan
		// of existing files for duplicates is unnecessary.
		err := tx.AddDataFiles(ctx, files, props, table.WithoutDuplicateCheck())
		if err == nil {
			var committed *table.Table
			if committed, err = tx.Commit(ctx); err == nil {
				o.mCommitLatency.Timing(time.Since(start).Nanoseconds())
				o.mFiles.Incr(int64(len(files)))
				o.writeCommitPointer(ctx, committed)
				o.setTable(committed)
				return nil
			}
		}
		if !errors.Is(err, table.ErrCommitFailed) || attempt >= maxCommitAttempts {
			// The Parquet files stay behind unreferenced. They are not deleted
			// because a commit that reported an error may still have landed.
			return fmt.Errorf("committing to %s: %w", strings.Join(o.ident, "."), err)
		}
		o.mCommitRetries.Incr(1)
		if tbl, err = o.currentCatalog().LoadTable(ctx, o.ident); err != nil {
			return fmt.Errorf("reloading table after commit conflict: %w", err)
		}
	}
}

func (o *icebergOutput) Close(context.Context) error {
	return nil
}
