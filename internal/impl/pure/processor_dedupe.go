package pure

import (
	"context"
	"errors"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	dedupFieldCache          = "cache"
	dedupFieldKey            = "key"
	dedupFieldDropOnCacheErr = "drop_on_err"
	dedupFieldStrategy       = "strategy"
)

func dedupeProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Deduplicates messages by storing a key value in a cache using the `+"`add`"+` operator. If the key already exists within the cache it is dropped.`).
		Description(`
Caches must be configured as resources, for more information check out the [cache documentation here](/docs/components/caches/about).

When using this processor with an output target that might fail you should always wrap the output within an indefinite `+"[`retry`](/docs/components/outputs/retry)"+` block. This ensures that during outages your messages aren't reprocessed after failures, which would result in messages being dropped.

## Batch Deduplication

This processor enacts on individual messages only, in order to perform a deduplication on behalf of a batch (or window) of messages instead use the `+"[`cache` processor](/docs/components/processors/cache#examples)"+`.

## Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any at-least-once guarantees that it previously had. This is because the cache will preserve message signatures even if the message fails to leave the Bento pipeline, which would cause message loss in the event of an outage at the output sink followed by a restart of the Bento instance (or a server crash, etc).

This problem can be mitigated by using an in-memory cache and distributing messages to horizontally scaled Bento pipelines partitioned by the deduplication key. However, in situations where at-least-once delivery guarantees are important it is worth avoiding deduplication in favour of implementing idempotent behaviour at the edge of your stream pipelines.`).
		Example(
			"Deduplicate based on Kafka key",
			"The following configuration demonstrates a pipeline that deduplicates messages based on the Kafka key.",
			`
pipeline:
  processors:
    - dedupe:
        cache: keycache
        key: ${! metadata("kafka_key") }

cache_resources:
  - label: keycache
    memory:
      default_ttl: 60s
`,
		).
		Fields(
			service.NewStringField(dedupFieldCache).
				Description("The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			service.NewInterpolatedStringField(dedupFieldKey).
				Description("An interpolated string yielding the key to deduplicate by for each message.").
				Examples(`${! metadata("kafka_key") }`, `${! content().hash("xxhash64") }`),
			service.NewBoolField(dedupFieldDropOnCacheErr).
				Description("Whether messages should be dropped when the cache returns a general error such as a network issue.").
				Default(true),
			service.NewStringAnnotatedEnumField(dedupFieldStrategy, map[string]string{
				"FIFO": "Keeps the first value seen for each key.",
				"LIFO": "Keeps the last value seen for each key.",
			}).
				Description("Controls how to handle duplicate values.").
				Default("FIFO").Advanced(),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"dedupe", dedupeProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			cache, err := conf.FieldString(dedupFieldCache)
			if err != nil {
				return nil, err
			}

			keyStr, err := conf.FieldString(dedupFieldKey)
			if err != nil {
				return nil, err
			}

			dropOnErr, err := conf.FieldBool(dedupFieldDropOnCacheErr)
			if err != nil {
				return nil, err
			}

			dedupeStrategy, err := conf.FieldString(dedupFieldStrategy)
			if err != nil {
				return nil, err
			}

			var isFifo bool
			switch dedupeStrategy {
			case "FIFO":
				isFifo = true
			case "LIFO":
				isFifo = false
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newDedupe(cache, keyStr, dropOnErr, isFifo, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("dedupe", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

// type cacheOperation func(cache cache.V1, ctx context.Context, key string, value []byte, ttl *time.Duration) error

type dedupeProc struct {
	log log.Modular

	dropOnErr bool
	key       *field.Expression
	mgr       bundle.NewManagement
	cacheName string
	isFifo    bool
	// cacheOp   cacheOperation
}

func newDedupe(cacheStr, keyStr string, dropOnErr, isFifo bool, mgr bundle.NewManagement) (*dedupeProc, error) {
	if keyStr == "" {
		return nil, errors.New("dedupe key must not be empty")
	}
	key, err := mgr.BloblEnvironment().NewField(keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	if !mgr.ProbeCache(cacheStr) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cacheStr)
	}

	return &dedupeProc{
		log:       mgr.Logger(),
		dropOnErr: dropOnErr,
		isFifo:    isFifo,
		key:       key,
		mgr:       mgr,
		cacheName: cacheStr,
	}, nil
}

func (d *dedupeProc) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	newBatch := message.QuickBatch(nil)
	_ = batch.Iter(func(i int, p *message.Part) error {
		if !d.isFifo {
			// reverse order if LIFO
			i = len(batch) - i - 1
			p = batch[i]
		}
		key, err := d.key.String(i, batch)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			ctx.OnError(err, i, nil)
			return nil
		}

		if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(cache cache.V1) {
			err = cache.Add(context.Background(), key, []byte{'t'}, nil)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if errors.Is(err, component.ErrKeyAlreadyExists) {
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			d.log.Error("Cache error: %v\n", err)
			if d.dropOnErr {
				ctx.Span(i).LogKV("event", "dropped", "type", "deduplicated")
				return nil
			}

			ctx.OnError(err, i, p)
		}

		if d.isFifo {
			// append if FIFO
			newBatch = append(newBatch, p)
		} else {
			// prepend if LIFO since we are reversing the order
			newBatch = append([]*message.Part{p}, newBatch...)
		}
		return nil
	})

	if newBatch.Len() == 0 {
		return nil, nil
	}
	return []message.Batch{newBatch}, nil
}

func (d *dedupeProc) Close(context.Context) error {
	return nil
}
