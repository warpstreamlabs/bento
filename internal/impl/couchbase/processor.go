package couchbase

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/impl/couchbase/client"
	"github.com/warpstreamlabs/bento/public/service"
)

// ProcessorConfig export couchbase processor specification.
func ProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// TODO Stable().
		Version("1.0.0").
		Categories("Integration").
		Summary("Performs operations against Couchbase for each message, allowing you to store or retrieve data within message payloads.").
		Description("When inserting, replacing or upserting documents, each must have the `content` property set.\n\n### Concurrent Document Mutations\nTo prevent read/write conflicts, Couchbase returns a [_Compare And Swap_ (CAS)](https://docs.couchbase.com/go-sdk/current/howtos/concurrent-document-mutations.html) value with each accessed document. Bento stores these as key/value pairs in metadata with the `couchbase_cas` field. Note: CAS checks are enabled by default. You can configure this by changing the value of `cas_enabled: false`.").
		Fields(CommonFields()...).
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationGet):       "Fetch a document.",
			string(client.OperationInsert):    "Insert a new document.",
			string(client.OperationRemove):    "Delete a document.",
			string(client.OperationReplace):   "Replace the contents of a document.",
			string(client.OperationUpsert):    "Creates a new document if it does not exist, if it does exist then it updates it.",
			string(client.OperationIncrement): "Increment a counter by the value in content, if it does not exist then it creates a counter with an initial value equal to the value in content. If the initial value is less than or equal to 0, a document not found error is returned.",
			string(client.OperationDecrement): "Decrement a counter by the value in content, if it does not exist then it creates a counter with an initial value equal to the negative of the value in content. If the initial value is less than or equal to 0, a document not found error is returned.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationGet))).
		LintRule(`root = if ((this.operation == "insert" || this.operation == "replace" || this.operation == "upsert" || this.operation == "increment" || this.operation == "decrement") && !this.exists("content")) { [ "content must be set for insert, replace, upsert, increment and decrement operations." ] }`)
}

func init() {
	err := service.RegisterBatchProcessor("couchbase", ProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewProcessor(context.Background(), conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

// NewProcessor returns a Couchbase processor.
func NewProcessor(ctx context.Context, conf *service.ParsedConfig, mgr *service.Resources) (*Couchbase, error) {
	return New(ctx, conf, mgr, false)
}

// ProcessBatch applies the processor to a message batch, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Couchbase) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()

	// execute
	ops, err := c.process(ctx, newMsg)
	if err != nil {
		return nil, err
	}

	// set results
	for index, part := range newMsg {
		out, cas, err := valueFromOp(ops[index])
		if err != nil {
			part.SetError(fmt.Errorf("couchbase operator failed: %w", err))
		}

		if data, ok := out.([]byte); ok {
			part.SetBytes(data)
		} else if out != nil {
			part.SetStructured(out)
		}

		part.MetaSetMut(MetaCASKey, cas)
	}

	return []service.MessageBatch{newMsg}, nil
}
