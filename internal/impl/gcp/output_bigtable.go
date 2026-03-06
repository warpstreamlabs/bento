package gcp

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/bigtable"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	btoFieldProject     = "project"
	btoFieldInstance    = "instance"
	btoFieldTable       = "table"
	btoFieldColumn      = "column"
	btoFieldRowKey      = "row_key"
	btoFieldFamily      = "family"
	btoFieldBatching    = "batching"
	btoFieldMaxInFlight = "max_in_flight"
)

func init() {
	err := service.RegisterBatchOutput(
		"gcp_bigtable", gcpBigTableOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (output service.BatchOutput, batchPol service.BatchPolicy, maxInFlight int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(btoFieldBatching); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt(btoFieldMaxInFlight); err != nil {
				return
			}
			var gconf gcpBigTableOutputConfig
			if gconf, err = gcpBigTableOutputConfigFromParsed(conf); err != nil {
				return
			}
			output, err = newGCPBigTableOutput(gconf)
			return
		})
	if err != nil {
		panic(err)
	}
}

func gcpBigTableOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services", "GCP").
		Summary("Writes messages to a GCP BigTable instance.").
		Description(`
Each message is written as a [SetCell](https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2#google.bigtable.v2.Mutation.SetCell) mutation into the specified column family and column qualifier.

The `+"`table`"+`, `+"`row_key`"+`, `+"`column`"+`, and `+"`family`"+` fields support [interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing values to be resolved dynamically. This enables routing messages to different tables, rows, column families, or column qualifiers based on message content or metadata.

:::caution Interpolation of Message Batches
The first message in the batch will resolve the bloblang query for the field `+"`table`"+` and that value will be used for all messages in the batch.
:::

### Credentials

By default Bento will use a shared credentials file when connecting to GCP services. You can find out more [in this document](/docs/guides/cloud/gcp).

### Batching

This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more [in this doc](/docs/configuration/batching).`).
		Fields(
			service.NewStringField(btoFieldProject).
				Description("The GCP project ID that contains the Bigtable instance."),
			service.NewStringField(btoFieldInstance).
				Description("The Bigtable instance ID to connect to."),
			service.NewInterpolatedStringField(btoFieldTable).
				Description("The table to write messages to.").
				Example("my-table").
				Example(`${!metadata("table")}`),
			service.NewInterpolatedStringField(btoFieldRowKey).
				Description("The row key for each mutation. Row keys must be unique per message to avoid overwriting previous writes within the same batch.").
				Example(`${!metadata("kafka_key")}`).
				Example(`${!json("id")}`).
				Example(`${!metadata("type")}#${!uuid_v4()}`),
			service.NewInterpolatedStringField(btoFieldColumn).
				Description("The column qualifier to set within the column family.").
				Example("payload").
				Example(`${!metadata("column")}`),
			service.NewInterpolatedStringField(btoFieldFamily).
				Description("The column family to write into.").
				Example("cf1").
				Example(`${!metadata("family")}`),
			service.NewBatchPolicyField(btoFieldBatching),
			service.NewOutputMaxInFlightField(),
		)
}

type gcpBigTableOutputConfig struct {
	ProjectID  string
	InstanceID string

	TableID *service.InterpolatedString

	RowKey       *service.InterpolatedString
	ColumnName   *service.InterpolatedString
	ColumnFamily *service.InterpolatedString
}

func gcpBigTableOutputConfigFromParsed(conf *service.ParsedConfig) (gconf gcpBigTableOutputConfig, err error) {
	if gconf.ProjectID, err = conf.FieldString(btoFieldProject); err != nil {
		return
	}

	if gconf.InstanceID, err = conf.FieldString(btoFieldInstance); err != nil {
		return
	}

	if gconf.TableID, err = conf.FieldInterpolatedString(btoFieldTable); err != nil {
		return
	}

	if gconf.RowKey, err = conf.FieldInterpolatedString(btoFieldRowKey); err != nil {
		return
	}

	if gconf.ColumnName, err = conf.FieldInterpolatedString(btoFieldColumn); err != nil {
		return
	}

	if gconf.ColumnFamily, err = conf.FieldInterpolatedString(btoFieldFamily); err != nil {
		return
	}

	return
}

type gcpBigTableOutput struct {
	client *bigtable.Client
	mu     sync.RWMutex

	conf gcpBigTableOutputConfig
}

func newGCPBigTableOutput(conf gcpBigTableOutputConfig) (*gcpBigTableOutput, error) {
	return &gcpBigTableOutput{
		conf: conf,
		mu:   sync.RWMutex{},
	}, nil
}

func (g *gcpBigTableOutput) Connect(ctx context.Context) error {
	// TODO(gregfurman): We should use the bigtable.AdminClient to check if tables and column-families exist.
	// However, since this can be dynamic, we should either only check if static OR provide a field that accepts static tableIDs and
	// column-family names a-priori.

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.client == nil {
		client, err := bigtable.NewClient(ctx, g.conf.ProjectID, g.conf.InstanceID)
		if err != nil {
			return fmt.Errorf("could not create bigtable client: %w", err)
		}

		g.client = client
	}

	return g.client.PingAndWarm(ctx)
}

func (g *gcpBigTableOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	// assume that the first message in the batch has the table for all messages in the batch.

	g.mu.RLock()
	if g.client == nil {
		g.mu.RUnlock()
		return service.ErrNotConnected
	}

	tableName, err := g.conf.TableID.TryString(batch[0])
	if err != nil {
		g.mu.RUnlock()
		return fmt.Errorf("table key interpolation error: %w", err)
	}

	familyExec := batch.InterpolationExecutor(g.conf.ColumnFamily)
	columnExec := batch.InterpolationExecutor(g.conf.ColumnName)
	rowKeyExec := batch.InterpolationExecutor(g.conf.RowKey)

	tbl := g.client.Open(tableName)
	g.mu.RUnlock()

	muts := make([]*bigtable.Mutation, len(batch))
	rowKeys := make([]string, len(batch))

	for i, msg := range batch {
		mut := bigtable.NewMutation()

		contents, err := msg.AsBytes()
		if err != nil {
			return err
		}

		family, err := familyExec.TryString(i)
		if err != nil {
			return err
		}

		rowKey, err := rowKeyExec.TryString(i)
		if err != nil {
			return err
		}

		col, err := columnExec.TryString(i)
		if err != nil {
			return err
		}

		rowKeys[i] = rowKey
		mut.Set(family, col, bigtable.Now(), contents)
		muts[i] = mut
	}

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		// TODO(gregfurman): Consider forcing a client to be re-created when encountering certain errors.
		return fmt.Errorf("failed to apply bulk operation: %w", err)
	}
	if len(rowErrs) == 0 {
		return nil
	}

	batchErr := service.NewBatchError(batch, fmt.Errorf("failed to apply %d mutation(s) in ApplyBulk", len(rowErrs)))
	for i, rErr := range rowErrs {
		if rErr != nil {
			batchErr.Failed(i, rErr)
		}
	}

	return batchErr
}

func (g *gcpBigTableOutput) Close(_ context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.client == nil {
		return nil
	}

	err := g.client.Close()
	g.client = nil

	return err

}
