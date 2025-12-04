package couchbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/warpstreamlabs/bento/internal/impl/couchbase/client"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// MetaCASKey holds the CAS value of an entry.
	MetaCASKey = "couchbase_cas"
)

var (
	// ErrInvalidOperation specified operation is not supported.
	ErrInvalidOperation = errors.New("invalid operation")
	// ErrContentRequired content field is required.
	ErrContentRequired = errors.New("content required")
)

// ProcessorConfig export couchbase processor specification.
func ProcessorConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("1.0.0").
		Categories("Integration").
		Summary("Performs operations against Couchbase for each message, allowing you to store or retrieve data within message payloads.").
		Description("When inserting, replacing or upserting documents, each must have the `content` property set.\n\n### Concurrent Document Mutations\nTo prevent read/write conflicts, Couchbase returns a [_Compare And Swap_ (CAS)](https://docs.couchbase.com/go-sdk/current/howtos/concurrent-document-mutations.html) value with each accessed document. Bento stores these as key/value pairs in metadata with the `couchbase_cas` field. Note: CAS checks are enabled by default. You can configure this by changing the value of `cas_enabled: false`.").
		Field(service.NewInterpolatedStringField("id").Description("Document id.").Example(`${! json("id") }`)).
		Field(service.NewBloblangField("content").Description("Document content.").Optional()).
		Field(service.NewDurationField("ttl").Description("An optional TTL to set for items.").Optional().Advanced()).
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationGet):     "fetch a document.",
			string(client.OperationInsert):  "insert a new document.",
			string(client.OperationRemove):  "delete a document.",
			string(client.OperationReplace): "replace the contents of a document.",
			string(client.OperationUpsert):  "creates a new document if it does not exist, if it does exist then it updates it.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationGet))).
		Field(service.NewBoolField("cas_enabled").Description("Enable CAS validation.").Default(true).Version("1.3.0")). // TODO: Consider removal in next release?
		LintRule(`root = if ((this.operation == "insert" || this.operation == "replace" || this.operation == "upsert") && !this.exists("content")) { [ "content must be set for insert, replace and upsert operations." ] }`)
}

func init() {
	err := service.RegisterBatchProcessor("couchbase", ProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from couchbase for each message of a
// batch.
type Processor struct {
	*couchbaseClient
	id         *service.InterpolatedString
	content    *bloblang.Executor
	ttl        *time.Duration
	op         func(key string, data []byte, cas gocb.Cas, ttl *time.Duration) gocb.BulkOp
	casEnabled bool
}

// NewProcessor returns a Couchbase processor.
func NewProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*Processor, error) {
	cl, err := getClient(conf, mgr)
	if err != nil {
		return nil, err
	}
	p := &Processor{
		couchbaseClient: cl,
	}

	if p.id, err = conf.FieldInterpolatedString("id"); err != nil {
		return nil, err
	}

	if conf.Contains("content") {
		if p.content, err = conf.FieldBloblang("content"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("ttl") {
		ttlTmp, err := conf.FieldDuration("ttl")
		if err != nil {
			return nil, err
		}
		p.ttl = &ttlTmp
	}

	p.casEnabled, err = conf.FieldBool("cas_enabled")
	if err != nil {
		return nil, err
	}

	op, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	switch client.Operation(op) {
	case client.OperationGet:
		p.op = get
	case client.OperationRemove:
		p.op = remove
	case client.OperationInsert:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = insert
	case client.OperationReplace:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = replace
	case client.OperationUpsert:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = upsert
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	return p, nil
}

// ProcessBatch applies the processor to a message batch, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Processor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()
	ops := make([]gocb.BulkOp, len(inBatch))
	var executor *service.MessageBatchBloblangExecutor
	if p.content != nil {
		executor = inBatch.BloblangExecutor(p.content)
	}

	// generate query
	for index, msg := range newMsg {
		// generate id
		k, err := inBatch.TryInterpolatedString(index, p.id)
		if err != nil {
			return nil, fmt.Errorf("id interpolation error: %w", err)
		}

		// generate content
		var content []byte
		if p.content != nil {
			res, err := executor.Query(index)
			if err != nil {
				return nil, err
			}
			content, err = res.AsBytes()
			if err != nil {
				return nil, err
			}
		}

		var cas gocb.Cas // retrieve cas if set and enabled
		if p.casEnabled {
			if val, ok := msg.MetaGetMut(MetaCASKey); ok {
				if v, ok := val.(gocb.Cas); ok {
					cas = v
				}
			}
		}

		ops[index] = p.op(k, content, cas, p.ttl)
	}

	// execute
	err := p.collection.Do(ops, &gocb.BulkOpOptions{})
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
