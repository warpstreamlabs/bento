package pure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	cacheCollectorPFieldResource     = "resource"
	cacheCollectorPFieldKey          = "key"
	cacheCollectorPFieldInit         = "init"
	cacheCollectorPFieldAppendCheck  = "append_check"
	cacheCollectorPFieldAppendMap    = "append_map"
	cacheCollectorPFieldFlushCheck   = "flush_check"
	cacheCollectorPFieldFlushDeletes = "flush_deletes"
	cacheCollectorPFieldFlushMap     = "flush_map"
	cacheCollectorPFieldTTL          = "ttl"
)

func cacheCollectorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Stable().
		Summary("Accumulates messages across batch boundaries using a cache resource, allowing you to build up state before emitting a final result as structed data.").
		Description(`
This processor works by storing an accumulated value in a cache, which is updated on each message based on bloblang expressions. It supports three phases:

1. `+"`init`"+`: When the cache key doesn't exist, this bloblang expression is used to initialize the value.
2. `+"`append_check`"+`: For each message, if this expression evaluates to true, the value is updated using `+"`append_map`"+`.
3. `+"`flush_check`"+`: When this expression evaluates to true, the accumulated value is emitted as a new message and the cache is optionally cleared.

The `+"`append_map`"+` bloblang expression can access both the current cached value as `+"`this.cached`"+` and the current message as `+"`this.current`"+`.`).
		Fields(
			service.NewStringField(cacheCollectorPFieldResource).
				Description("The [`cache` resource](/docs/components/caches/about) to use for storing accumulated state."),
			service.NewInterpolatedStringField(cacheCollectorPFieldKey).
				Description("A key for the cache entry. This should be consistent across messages that should be grouped together."),
			service.NewBloblangField(cacheCollectorPFieldInit).
				Description("Bloblang expression to initialize the value when the cache key doesn't exist. Defaults to an empty array.").
				Default("root = []").
				Examples("root = []", `root = {"items": []}`, `root = {"count": 0, "total": 0}`),
			service.NewBloblangField(cacheCollectorPFieldAppendCheck).
				Description("Bloblang expression that must evaluate to `true` for a message to be appended to the accumulated value.").
				Examples(`this.process == "work"`, `batch_index() < 100`),
			service.NewBloblangField(cacheCollectorPFieldAppendMap).
				Description("Bloblang expression used to update the accumulated value. It receives the current value as `this.cached` and the new message as `this.current`.").
				Examples(`root = this.cached.append(this.current)`, `root = {"count": this.cached.count + 1, "total": this.cached.total + this.current.value}`),
			service.NewBloblangField(cacheCollectorPFieldFlushCheck).
				Description("Bloblang expression that must evaluate to `true` to emit the accumulated value and potentially clear the cache.").
				Examples(`this.process == "end"`, `batch_index() == 0 && message_index() > 0 && batch_index() % 100 == 0`),
			service.NewBoolField(cacheCollectorPFieldFlushDeletes).
				Description("When `true`, the cache entry is deleted after flushing. Defaults to `false`.").
				Default(false),
			service.NewBloblangField(cacheCollectorPFieldFlushMap).
				Description("Bloblang expression to transform the accumulated value before emitting. Defaults to `root`.").
				Default("root = this").
				Examples(`root = this`, `root = {"result": this}`, `root.items = this`),
			service.NewInterpolatedStringField(cacheCollectorPFieldTTL).
				Description("The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, those that do will have a configuration field `default_ttl`, and those that do not will fall back to their generally configured TTL setting.").
				Examples("60s", "5m", "36h").
				Advanced().
				Optional(),
		)
}

type cacheCollector struct {
	cacheName string

	key          *service.InterpolatedString
	init         *bloblang.Executor
	appendCheck  *bloblang.Executor
	appendMap    *bloblang.Executor
	flushCheck   *bloblang.Executor
	flushDeletes bool
	flushMap     *bloblang.Executor
	ttl          *service.InterpolatedString

	mgr *service.Resources
}

func init() {
	err := service.RegisterBatchProcessor(
		"cache_collector", cacheCollectorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newCacheCollector(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newCacheCollector(conf *service.ParsedConfig, mgr *service.Resources) (*cacheCollector, error) {
	resource, err := conf.FieldString(cacheCollectorPFieldResource)
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldInterpolatedString(cacheCollectorPFieldKey)
	if err != nil {
		return nil, err
	}

	init, err := conf.FieldBloblang(cacheCollectorPFieldInit)
	if err != nil {
		return nil, err
	}

	appendCheck, err := conf.FieldBloblang(cacheCollectorPFieldAppendCheck)
	if err != nil {
		return nil, err
	}

	appendMap, err := conf.FieldBloblang(cacheCollectorPFieldAppendMap)
	if err != nil {
		return nil, err
	}

	flushCheck, err := conf.FieldBloblang(cacheCollectorPFieldFlushCheck)
	if err != nil {
		return nil, err
	}

	flushDeletes, err := conf.FieldBool(cacheCollectorPFieldFlushDeletes)
	if err != nil {
		return nil, err
	}

	flushMap, err := conf.FieldBloblang(cacheCollectorPFieldFlushMap)
	if err != nil {
		return nil, err
	}

	ttl, err := conf.FieldInterpolatedString(cacheCollectorPFieldTTL)
	if err != nil {
		return nil, err
	}

	return &cacheCollector{
		key:         key,
		init:        init,
		appendCheck: appendCheck,
		appendMap:   appendMap,
		flushCheck:  flushCheck,
		flushMap:    flushMap,

		cacheName:    resource,
		flushDeletes: flushDeletes,

		ttl: ttl,

		mgr: mgr,
	}, nil
}

type cacheCollectorAppendMessageData struct {
	cached  json.RawMessage
	current json.RawMessage
}

func (cc *cacheCollector) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var newMsgs []*service.Message

	var keyInterp *service.MessageBatchInterpolationExecutor
	if cc.key != nil {
		keyInterp = batch.InterpolationExecutor(cc.key)
	}

	var ttlInterp *service.MessageBatchInterpolationExecutor
	if cc.ttl != nil {
		keyInterp = batch.InterpolationExecutor(cc.ttl)
	}

	var appendCheck *service.MessageBatchBloblangExecutor
	if cc.appendCheck != nil {
		appendCheck = batch.BloblangExecutor(cc.appendCheck)
	}

	var flushCheck *service.MessageBatchBloblangExecutor
	if cc.flushCheck != nil {
		flushCheck = batch.BloblangExecutor(cc.flushCheck)
	}

	var init *service.MessageBatchBloblangExecutor
	if cc.init != nil {
		init = batch.BloblangExecutor(cc.init)
	}

	for i, msg := range batch {
		key, err := keyInterp.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("key evaluation error: %w", err)
		}

		ttls, err := ttlInterp.TryString(i)
		if err != nil {
			return nil, err
		}

		var ttl *time.Duration

		if ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				return nil, fmt.Errorf("ttl must be a duration: %w", err)
			}
			ttl = &td
		}

		var processAppend bool
		var processFlush bool

		processAppend, err = appendCheck.QueryBool(i)
		if err != nil {
			return nil, fmt.Errorf("append_check evaluation error: %w", err)
		}

		processFlush, err = flushCheck.QueryBool(i)
		if err != nil {
			return nil, fmt.Errorf("flush_check evaluation error: %w", err)
		}

		if processAppend || processFlush {
			var cachedValue []byte
			if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
				cachedValue, err = cache.Get(ctx, key)
				if err != nil {
					if errors.Is(err, service.ErrKeyNotFound) {
						cachedValue = nil
					} else {
						err = fmt.Errorf("failed to get cache key '%s': %v", key, err)
					}
				}
			}); cerr != nil {
				err = cerr
			}

			if err != nil {
				return nil, err
			}

			if cachedValue == nil {
				initMsg, err := init.Query(i)
				if err != nil {
					return nil, fmt.Errorf("init evaluation error: %w", err)
				}
				initData, err := initMsg.AsBytes()
				if err != nil {
					return nil, fmt.Errorf("init data error: %w", err)
				}
				cachedValue = initData
			}

			if processAppend {
				currentValue, err := msg.AsBytes()
				if err != nil {
					return nil, err
				}

				appendMsg := service.NewMessage(nil)
				appendMsg.SetStructured(cacheCollectorAppendMessageData{
					cached:  cachedValue,
					current: currentValue,
				})

				appendResult, err := appendMsg.BloblangQuery(cc.appendMap)
				if err != nil {
					return nil, err
				}

				cachedValue, err = appendResult.AsBytes()
				if err != nil {
					return nil, err
				}

				if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
					err = cache.Set(ctx, key, cachedValue, ttl)
					if err != nil {
						err = fmt.Errorf("failed to set cache key '%s': %v", key, err)
					}
				}); cerr != nil {
					err = cerr
				}

				if err != nil {
					return nil, err
				}
			}

			if processFlush {
				flushMsg := service.NewMessage(cachedValue)

				flushResult, err := flushMsg.BloblangQuery(cc.appendMap)
				if err != nil {
					return nil, err
				}

				if cc.flushDeletes {
					if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
						err = cache.Delete(ctx, key)
						if err != nil {
							err = fmt.Errorf("failed to delete cache key '%s': %v", key, err)
						}
					}); cerr != nil {
						err = cerr
					}

					if err != nil {
						return nil, err
					}
				}

				newMsgs = append(newMsgs, flushResult)
			}
		}
	}

	return []service.MessageBatch{newMsgs}, nil
}

func (cc *cacheCollector) Close(ctx context.Context) error {
	return nil
}
