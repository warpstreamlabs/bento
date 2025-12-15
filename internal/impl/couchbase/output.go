package couchbase

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/impl/couchbase/client"
	"github.com/warpstreamlabs/bento/public/service"
)

// OutputConfig export couchbase output specification.
func OutputConfig() *service.ConfigSpec {
	return Config().
		Version("1.14.0").
		Summary("Performs operations against Couchbase for each message, allowing you to store data within message payloads.").
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationInsert):    "Insert a new document.",
			string(client.OperationRemove):    "Delete a document.",
			string(client.OperationReplace):   "Replace the contents of a document.",
			string(client.OperationUpsert):    "Creates a new document if it does not exist, if it does exist then it updates it.",
			string(client.OperationIncrement): "Increment a counter by the value in content, if it does not exist then it creates a counter with an initial value equal to the value in content. If the initial value is less than or equal to 0, a document not found error is returned.",
			string(client.OperationDecrement): "Decrement a counter by the value in content, if it does not exist then it creates a counter with an initial value equal to the negative of the value in content. If the initial value is less than or equal to 0, a document not found error is returned.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationUpsert))).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField("batching"))
}

// forbid get operation

func init() {
	err := service.RegisterBatchOutput(
		"couchbase",
		OutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = NewOutput(conf, mgr)
			return
		},
	)
	if err != nil {
		panic(err)
	}
}

// NewOutput returns a Couchbase output.
func NewOutput(conf *service.ParsedConfig, mgr *service.Resources) (*Couchbase, error) {
	return New(conf, mgr, true)
}

// WriteBatch writes a batch of messages to couchbase.
func (c *Couchbase) WriteBatch(ctx context.Context, inBatch service.MessageBatch) error {
	ops, err := c.process(ctx, inBatch)
	if err != nil {
		return err // nothing worked
	}

	// check for individual errors
	var batchErr *service.BatchError
	for index := range inBatch {
		_, _, err := valueFromOp(ops[index])
		if err != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(inBatch, ErrBatchError)
			}
			batchErr.Failed(index, fmt.Errorf("couchbase operator failed: %w", err))
		}
	}

	if batchErr != nil {
		return batchErr
	}

	return nil
}
