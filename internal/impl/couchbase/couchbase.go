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
	// ErrBatchError batch error.
	ErrBatchError = errors.New("batch error")
)

// CommonFields returns the common config fields for the Couchbase client.
func CommonFields() []*service.ConfigField {
	return append(client.CommonFields(),
		service.NewInterpolatedStringField("id").Description("Document id.").Example(`${! json("id") }`),
		service.NewBloblangField("content").Description("Document content.").Optional(),
		service.NewDurationField("ttl").Description("An optional TTL to set for items.").Optional().Advanced(),
		service.NewBoolField("cas_enabled").Description("Enable CAS validation.").Default(true).Version("1.3.0"), // TODO: Consider removal in next release?
	)
}

// Couchbase stores or retrieves data from couchbase for each message of a
// batch.
type Couchbase struct {
	*couchbaseClient
	conf                   *service.ParsedConfig
	mgr                    *service.Resources
	id                     *service.InterpolatedString
	content                *bloblang.Executor
	ttl                    *time.Duration
	op                     func(key string, data []byte, cas gocb.Cas, ttl *time.Duration) (gocb.BulkOp, error)
	casEnabled, outputMode bool
}

// New returns a Couchbase instance.
func New(ctx context.Context, conf *service.ParsedConfig, mgr *service.Resources, outputMode bool) (*Couchbase, error) {
	var err error

	p := &Couchbase{
		conf:       conf,
		mgr:        mgr,
		outputMode: outputMode,
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
	case client.OperationIncrement:
		p.op = increment
	case client.OperationDecrement:
		p.op = decrement
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	if !p.outputMode {
		err = p.Connect(ctx)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (c *Couchbase) process(ctx context.Context, batch service.MessageBatch) ([]gocb.BulkOp, error) {
	ops := make([]gocb.BulkOp, len(batch))
	var executor *service.MessageBatchBloblangExecutor
	if c.content != nil {
		executor = batch.BloblangExecutor(c.content)
	}

	// generate query
	for index, msg := range batch {
		// generate id
		k, err := batch.TryInterpolatedString(index, c.id)
		if err != nil {
			return nil, fmt.Errorf("id interpolation error: %w", err)
		}

		// generate content
		var content []byte
		if c.content != nil {
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
		if c.casEnabled {
			if val, ok := msg.MetaGetMut(MetaCASKey); ok {
				if v, ok := val.(gocb.Cas); ok {
					cas = v
				}
			}
		}

		ops[index], err = c.op(k, content, cas, c.ttl)
		if err != nil {
			return nil, err
		}
	}

	// execute
	err := c.collection.Do(ops, &gocb.BulkOpOptions{})
	if err != nil {
		return nil, err
	}

	return ops, nil
}

// Connect connects to the couchbase cluster
func (c *Couchbase) Connect(ctx context.Context) (err error) {
	c.couchbaseClient, err = getClient(ctx, c.conf)
	if err != nil {
		return err
	}

	return nil
}
