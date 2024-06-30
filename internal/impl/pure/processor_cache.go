package pure

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/warpstreamlabs/bento/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	cachePFieldResource = "resource"
	cachePFieldOperator = "operator"
	cachePFieldKey      = "key"
	cachePFieldValue    = "value"
	cachePFieldTTL      = "ttl"
)

func cacheProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Stable().
		Summary("Performs operations against a [cache resource](/docs/components/caches/about) for each message, allowing you to store or retrieve data within message payloads.").
		Description(`
For use cases where you wish to cache the result of processors consider using the `+"[`cached` processor](/docs/components/processors/cached)"+` instead.

This processor will interpolate functions within the `+"`key` and `value`"+` fields individually for each message. This allows you to specify dynamic keys and values based on the contents of the message payloads and metadata. You can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`).
		Footnotes(`
## Operators

### `+"`set`"+`

Set a key in the cache to a value. If the key already exists the contents are
overridden.

### `+"`add`"+`

Set a key in the cache to a value. If the key already exists the action fails
with a 'key already exists' error, which can be detected with
[processor error handling](/docs/configuration/error_handling).

### `+"`get`"+`

Retrieve the contents of a cached key and replace the original message payload
with the result. If the key does not exist the action fails with an error, which
can be detected with [processor error handling](/docs/configuration/error_handling).

### `+"`delete`"+`

Delete a key and its contents from the cache.  If the key does not exist the
action is a no-op and will not fail with an error.`).
		Example("Deduplication", `
Deduplication can be done using the add operator with a key extracted from the message payload, since it fails when a key already exists we can remove the duplicates using a [`+"`mapping` processor"+`](/docs/components/processors/mapping):`,
			`
pipeline:
  processors:
    - cache:
        resource: foocache
        operator: add
        key: '${! json("message.id") }'
        value: "storeme"
    - mapping: root = if errored() { deleted() }

cache_resources:
  - label: foocache
    redis:
      url: tcp://TODO:6379
`).
		Example("Deduplication Batch-Wide", `
Sometimes it's necessary to deduplicate a batch of messages (AKA a window) by a single identifying value. This can be done by introducing a `+"[`branch` processor](/docs/components/processors/branch)"+`, which executes the cache only once on behalf of the batch, in this case with a value make from a field extracted from the first and last messages of the batch:`,
			`
pipeline:
  processors:
    # Try and add one message to a cache that identifies the whole batch
    - branch:
        request_map: |
          root = if batch_index() == 0 {
            json("id").from(0) + json("meta.tail_id").from(-1)
          } else { deleted() }
        processors:
          - cache:
              resource: foocache
              operator: add
              key: ${! content() }
              value: t
    # Delete all messages if we failed
    - mapping: |
        root = if errored().from(0) {
          deleted()
        }
`).
		Example("Hydration", `
It's possible to enrich payloads with content previously stored in a cache by using the [`+"`branch`"+`](/docs/components/processors/branch) processor:`,
			`
pipeline:
  processors:
    - branch:
        processors:
          - cache:
              resource: foocache
              operator: get
              key: '${! json("message.document_id") }'
        result_map: 'root.message.document = this'

        # NOTE: If the data stored in the cache is not valid JSON then use
        # something like this instead:
        # result_map: 'root.message.document = content().string()'

cache_resources:
  - label: foocache
    memcached:
      addresses: [ "TODO:11211" ]
`).
		Fields(
			service.NewStringField(cachePFieldResource).
				Description("The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			service.NewStringEnumField(cachePFieldOperator, "set", "add", "get", "delete").
				Description("The [operation](#operators) to perform with the cache."),
			service.NewInterpolatedStringField(cachePFieldKey).
				Description("A key to use with the cache."),
			service.NewInterpolatedStringField(cachePFieldValue).
				Description("A value to use with the cache (when applicable).").
				Optional(),
			service.NewInterpolatedStringField(cachePFieldTTL).
				Description("The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, those that do will have a configuration field `default_ttl`, and those that do not will fall back to their generally configured TTL setting.").
				Examples("60s", "5m", "36h").
				Version("1.0.0").
				Advanced().
				Optional(),
		)
}

type cacheProcConfig struct {
	Resource string
	Operator string
	Key      string
	Value    string
	TTL      string
}

func init() {
	err := service.RegisterBatchProcessor(
		"cache", cacheProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			var cConf cacheProcConfig
			var err error

			if cConf.Resource, err = conf.FieldString(cachePFieldResource); err != nil {
				return nil, err
			}
			if cConf.Operator, err = conf.FieldString(cachePFieldOperator); err != nil {
				return nil, err
			}
			if cConf.Key, err = conf.FieldString(cachePFieldKey); err != nil {
				return nil, err
			}
			cConf.Value, _ = conf.FieldString(cachePFieldValue)
			cConf.TTL, _ = conf.FieldString(cachePFieldTTL)

			mgr := interop.UnwrapManagement(res)
			p, err := newCache(cConf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("cache", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type cacheProc struct {
	key   *field.Expression
	value *field.Expression
	ttl   *field.Expression

	mgr       bundle.NewManagement
	cacheName string
	operator  cacheOperator
}

func newCache(conf cacheProcConfig, mgr bundle.NewManagement) (*cacheProc, error) {
	cacheName := conf.Resource
	if cacheName == "" {
		return nil, errors.New("cache name must be specified")
	}

	op, err := cacheOperatorFromString(conf.Operator)
	if err != nil {
		return nil, err
	}

	key, err := mgr.BloblEnvironment().NewField(conf.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	value, err := mgr.BloblEnvironment().NewField(conf.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	ttl, err := mgr.BloblEnvironment().NewField(conf.TTL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if !mgr.ProbeCache(cacheName) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cacheName)
	}

	return &cacheProc{
		key:   key,
		value: value,
		ttl:   ttl,

		mgr:       mgr,
		cacheName: cacheName,
		operator:  op,
	}, nil
}

//------------------------------------------------------------------------------

type cacheOperator func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error)

func newCacheSetOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Set(ctx, key, value, ttl)
		return nil, false, err
	}
}

func newCacheAddOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Add(ctx, key, value, ttl)
		return nil, false, err
	}
}

func newCacheGetOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, _ []byte, _ *time.Duration) ([]byte, bool, error) {
		result, err := cache.Get(ctx, key)
		return result, true, err
	}
}

func newCacheDeleteOperator() cacheOperator {
	return func(ctx context.Context, cache cache.V1, key string, _ []byte, ttl *time.Duration) ([]byte, bool, error) {
		err := cache.Delete(ctx, key)
		return nil, false, err
	}
}

func cacheOperatorFromString(operator string) (cacheOperator, error) {
	switch operator {
	case "set":
		return newCacheSetOperator(), nil
	case "add":
		return newCacheAddOperator(), nil
	case "get":
		return newCacheGetOperator(), nil
	case "delete":
		return newCacheDeleteOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", operator)
}

//------------------------------------------------------------------------------

func (c *cacheProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	_ = msg.Iter(func(index int, part *message.Part) error {
		key, err := c.key.String(index, msg)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			ctx.OnError(err, index, nil)
			return nil
		}

		value, err := c.value.Bytes(index, msg)
		if err != nil {
			err = fmt.Errorf("value interpolation error: %w", err)
			ctx.OnError(err, index, nil)
			return nil
		}

		var ttl *time.Duration
		ttls, err := c.ttl.String(index, msg)
		if err != nil {
			err = fmt.Errorf("ttl interpolation error: %w", err)
			ctx.OnError(err, index, nil)
			return nil
		}

		if ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				err = fmt.Errorf("ttl must be a duration: %w", err)
				ctx.OnError(err, index, nil)
				return nil
			}
			ttl = &td
		}

		var result []byte
		var useResult bool
		if cerr := c.mgr.AccessCache(context.Background(), c.cacheName, func(cache cache.V1) {
			result, useResult, err = c.operator(context.Background(), cache, key, value, ttl)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if err != component.ErrKeyAlreadyExists {
				err = fmt.Errorf("operator failed for key '%s': %v", key, err)
			} else {
				err = fmt.Errorf("key already exists: %v", key)
			}
			ctx.OnError(err, index, nil)
			return nil
		}

		if useResult {
			part.SetBytes(result)
		}
		return nil
	})

	return []message.Batch{msg}, nil
}

func (c *cacheProc) Close(ctx context.Context) error {
	return nil
}
