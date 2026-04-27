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
	cacheCollectorPFieldResource        = "resource"
	cacheCollectorPFieldKey             = "key"
	cacheCollectorPFieldInitCheck       = "init_check"
	cacheCollectorPFieldInitMap         = "init_map"
	cacheCollectorPFieldInitMode        = "init_mode"
	cacheCollectorPFieldAppendCheck     = "append_check"
	cacheCollectorPFieldAppendMap       = "append_map"
	cacheCollectorPFieldFlushCheck      = "flush_check"
	cacheCollectorPFieldFlushKeepCache  = "flush_keep_cache"
	cacheCollectorPFieldFlushMap        = "flush_map"
	cacheCollectorPFieldPassthroughMode = "passthrough_mode"
	cacheCollectorPFieldTTL             = "ttl"
)

func CacheCollectorProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Beta().
		Summary("Accumulates messages across batch boundaries using a cache resource, allowing you to build up state before emitting a final result as structed data.").
		Description(`
This processor works by storing an accumulated value in a cache, which is updated on each message based on bloblang expressions. It supports three phases:

1. `+"`init_check`"+`: When the cache key doesn't exist, if this expression evaluates to true, the value is initialize using `+"`init_map`"+`.
2. `+"`append_check`"+`: For each message, if this expression evaluates to true and the cache was initialized, the value is updated using `+"`append_map`"+`.
3. `+"`flush_check`"+`: When this expression evaluates to true and the cache was initialized, the accumulated value is emitted as a new message and the cache is optionally cleared.

The `+"`append_map`"+` and `+"`flush_map`"+` bloblang expressions can access both the current cached value as `+"`this.cached`"+` and the current message as `+"`this.current`"+`.`).
		Fields(
			service.NewStringField(cacheCollectorPFieldResource).
				Description("The [`cache` resource](/docs/components/caches/about) to use for storing accumulated state."),
			service.NewInterpolatedStringField(cacheCollectorPFieldKey).
				Description("A key for the cache entry. This should be consistent across messages that should be grouped together."),
			service.NewBloblangField(cacheCollectorPFieldInitCheck).
				Description("Bloblang expression that must evaluate to `true` for a message to initialize the cache.").
				Default("true").
				Examples(`this.process == "start"`),
			service.NewBloblangField(cacheCollectorPFieldInitMap).
				Description("Bloblang expression to initialize the value when the cache key doesn't exist. Defaults to an empty array.").
				Default("root = []").
				Examples("root = []", `root = {"items": []}`, `root = {"count": 0, "total": 0}`),
			service.NewInterpolatedStringEnumField(cacheCollectorPFieldInitMode, "check", "ignore", "replace").
				Description("Option to change the behavior of the initialisation. `check` will check if the cache key already exists and returns to an error, `ignore` just ignores the current message and keeps the cached value and `replace` replaces the cached value.").
				Advanced().
				Default("check"),
			service.NewBloblangField(cacheCollectorPFieldAppendCheck).
				Description("Bloblang expression that must evaluate to `true` for a message to be appended to the accumulated value.").
				Examples(`this.process == "work"`, `batch_index() < 100`),
			service.NewBloblangField(cacheCollectorPFieldAppendMap).
				Description("Bloblang expression used to update the accumulated value. It receives the current value as `this.cached` and the new message as `this.current`.").
				Examples(`root = this.cached.append(this.current)`, `root = {"count": this.cached.count + 1, "total": this.cached.total + this.current.value}`),
			service.NewBloblangField(cacheCollectorPFieldFlushCheck).
				Description("Bloblang expression that must evaluate to `true` to emit the accumulated value and potentially clear the cache.").
				Examples(`this.process == "end"`, `batch_index() == 0 && message_index() > 0 && batch_index() % 100 == 0`),
			service.NewBloblangField(cacheCollectorPFieldFlushMap).
				Description("Bloblang expression to transform the accumulated value before emitting. It receives the current value as `this.cached` and the new message as `this.current`. Defaults to `this.cached`.").
				Default("root = this.cached").
				Examples(`root = this.cached`, `root = {"result": this.current.append(this.cached)}`, `root.items = this.cached`),
			service.NewBoolField(cacheCollectorPFieldFlushKeepCache).
				Description("When `true`, the cache entry is not deleted after flushing. Defaults to `false`.").
				Advanced().
				Default(false),
			service.NewInterpolatedStringEnumField(cacheCollectorPFieldPassthroughMode, "none", "unprocessed", "processed", "all").
				Description("Option to change the behavior of the messages after handled (flush always creates a new message). `none` will just filter messages alle messages out, `unprocessed` will passtrought messages not handled by init or append, `processed` will passtrought only messages handled by init or append and `all` will passtrought all messages.").
				Advanced().
				Default("none"),
			service.NewInterpolatedStringField(cacheCollectorPFieldTTL).
				Description("The TTL of each individual item as a duration string. After this period an item will be eligible for removal during the next compaction. Not all caches support per-key TTLs, those that do will have a configuration field `default_ttl`, and those that do not will fall back to their generally configured TTL setting.").
				Examples("60s", "5m", "36h").
				Advanced().
				Optional(),
		)
}

type cacheCollectorProcessor struct {
	cacheName string

	key             *service.InterpolatedString
	initCheck       *bloblang.Executor
	initMap         *bloblang.Executor
	initMode        *service.InterpolatedString
	appendCheck     *bloblang.Executor
	appendMap       *bloblang.Executor
	flushCheck      *bloblang.Executor
	flushKeepCache  bool
	flushMap        *bloblang.Executor
	passthroughMode *service.InterpolatedString

	ttl *service.InterpolatedString

	mgr *service.Resources
}

func init() {
	err := service.RegisterBatchProcessor("cache_collector", CacheCollectorProcessorSpec(), NewCacheCollectorFromConfig)
	if err != nil {
		panic(err)
	}
}

func NewCacheCollectorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	resource, err := conf.FieldString(cacheCollectorPFieldResource)
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldInterpolatedString(cacheCollectorPFieldKey)
	if err != nil {
		return nil, err
	}

	initCheck, err := conf.FieldBloblang(cacheCollectorPFieldInitCheck)
	if err != nil {
		return nil, err
	}

	initMap, err := conf.FieldBloblang(cacheCollectorPFieldInitMap)
	if err != nil {
		return nil, err
	}

	initMode, err := conf.FieldInterpolatedString(cacheCollectorPFieldInitMode)
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

	flushKeepCache, err := conf.FieldBool(cacheCollectorPFieldFlushKeepCache)
	if err != nil {
		return nil, err
	}

	flushMap, err := conf.FieldBloblang(cacheCollectorPFieldFlushMap)
	if err != nil {
		return nil, err
	}

	passthroughMode, err := conf.FieldInterpolatedString(cacheCollectorPFieldPassthroughMode)
	if err != nil {
		return nil, err
	}

	ttl, _ := conf.FieldInterpolatedString(cacheCollectorPFieldTTL)

	return &cacheCollectorProcessor{
		key:         key,
		initCheck:   initCheck,
		initMap:     initMap,
		initMode:    initMode,
		appendCheck: appendCheck,
		appendMap:   appendMap,
		flushCheck:  flushCheck,
		flushMap:    flushMap,

		passthroughMode: passthroughMode,

		cacheName:      resource,
		flushKeepCache: flushKeepCache,

		ttl: ttl,

		mgr: mgr,
	}, nil
}

type cacheCollectorMessageData struct {
	Cached  json.RawMessage `json:"cached"`
	Current json.RawMessage `json:"current"`
}

func (cc *cacheCollectorProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var newMsgs []*service.Message

	var keyInterp *service.MessageBatchInterpolationExecutor
	if cc.key != nil {
		keyInterp = batch.InterpolationExecutor(cc.key)
	}

	var ttlInterp *service.MessageBatchInterpolationExecutor
	if cc.ttl != nil {
		ttlInterp = batch.InterpolationExecutor(cc.ttl)
	}

	var initCheck *service.MessageBatchBloblangExecutor
	if cc.initCheck != nil {
		initCheck = batch.BloblangExecutor(cc.initCheck)
	}

	var initMap *service.MessageBatchBloblangExecutor
	if cc.initMap != nil {
		initMap = batch.BloblangExecutor(cc.initMap)
	}

	var initModeInterp *service.MessageBatchInterpolationExecutor
	if cc.key != nil {
		initModeInterp = batch.InterpolationExecutor(cc.initMode)
	}

	var appendCheck *service.MessageBatchBloblangExecutor
	if cc.appendCheck != nil {
		appendCheck = batch.BloblangExecutor(cc.appendCheck)
	}

	var appendMap *service.MessageBatchBloblangExecutor
	if cc.appendMap != nil {
		appendMap = batch.BloblangExecutor(cc.appendMap)
	}

	var flushCheck *service.MessageBatchBloblangExecutor
	if cc.flushCheck != nil {
		flushCheck = batch.BloblangExecutor(cc.flushCheck)
	}

	var flushMap *service.MessageBatchBloblangExecutor
	if cc.flushMap != nil {
		flushMap = batch.BloblangExecutor(cc.flushMap)
	}

	var passthroughModeInterp *service.MessageBatchInterpolationExecutor
	if cc.key != nil {
		passthroughModeInterp = batch.InterpolationExecutor(cc.passthroughMode)
	}

	for i, msg := range batch {
		key, err := keyInterp.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("key evaluation error: %w", err)
		}

		passthroughMode, err := passthroughModeInterp.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("passthrough evaluation error: %w", err)
		}

		var ttls string
		if ttlInterp != nil {
			ttls, err = ttlInterp.TryString(i)
			if err != nil {
				return nil, err
			}
		}

		var ttl *time.Duration

		if ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				return nil, fmt.Errorf("ttl must be a duration: %w", err)
			}
			ttl = &td
		}

		processInit, err := initCheck.QueryBool(i)
		if err != nil {
			return nil, fmt.Errorf("init_check evaluation error: %w", err)
		}

		processAppend, err := appendCheck.QueryBool(i)
		if err != nil {
			return nil, fmt.Errorf("append_check evaluation error: %w", err)
		}

		processFlush, err := flushCheck.QueryBool(i)
		if err != nil {
			return nil, fmt.Errorf("flush_check evaluation error: %w", err)
		}

		if processAppend || processFlush {
			var cachedValue []byte
			cacheValueExists := true

			if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
				cachedValue, err = cache.Get(ctx, key)
				if err != nil {
					cacheValueExists = false
					if errors.Is(err, service.ErrKeyNotFound) {
						cachedValue = nil
						err = nil
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

			if processInit {
				if cacheValueExists {
					initMode, err := initModeInterp.TryString(i)
					if err != nil {
						return nil, fmt.Errorf("init_mode evaluation error: %w", err)
					}

					switch initMode {
					case "check":
						return nil, fmt.Errorf("failed to set cache key '%s': %v", key, service.ErrKeyAlreadyExists)
					case "ignore":
						// ignore the init message
					case "replace":
						initMsg, err := initMap.Query(i)
						if err != nil {
							return nil, fmt.Errorf("init_map evaluation error: %w", err)
						}
						initData, err := initMsg.AsBytes()
						if err != nil {
							return nil, fmt.Errorf("init data error: %w", err)
						}
						cachedValue = initData
					default:
						return nil, fmt.Errorf("init_mode unsupported mode %s", initMode)
					}
				} else {
					initMsg, err := initMap.Query(i)
					if err != nil {
						return nil, fmt.Errorf("init_map evaluation error: %w", err)
					}
					initData, err := initMsg.AsBytes()
					if err != nil {
						return nil, fmt.Errorf("init data error: %w", err)
					}
					cachedValue = initData
					cacheValueExists = true
				}
			}

			if cacheValueExists {
				currentValue, err := msg.AsBytes()
				if err != nil {
					return nil, err
				}

				if processAppend {
					appendMsgJson, err := json.Marshal(cacheCollectorMessageData{
						Cached:  json.RawMessage(cachedValue),
						Current: json.RawMessage(currentValue),
					})

					if err != nil {
						return nil, err
					}

					msg.SetBytes(appendMsgJson)

					appendResult, err := appendMap.Query(i)

					msg.SetBytes(currentValue)

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
					flushMsgJson, err := json.Marshal(cacheCollectorMessageData{
						Cached:  json.RawMessage(cachedValue),
						Current: json.RawMessage(currentValue),
					})

					if err != nil {
						return nil, err
					}

					msg.SetBytes(flushMsgJson)

					msg, err = flushMap.Query(i)

					if err != nil {
						return nil, err
					}

					if !cc.flushKeepCache {
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

					if msg != nil {
						newMsgs = append(newMsgs, msg)
					}
				} else if passthroughMode == "processed" || passthroughMode == "all" {
					newMsgs = append(newMsgs, msg)
				}
			} else if passthroughMode == "unprocessed" || passthroughMode == "all" {
				newMsgs = append(newMsgs, msg)
			}
		} else if processInit {
			initMode, err := initModeInterp.TryString(i)
			if err != nil {
				return nil, fmt.Errorf("init_mode evaluation error: %w", err)
			}

			initMsg, err := initMap.Query(i)
			if err != nil {
				return nil, fmt.Errorf("init_map evaluation error: %w", err)
			}
			initData, err := initMsg.AsBytes()
			if err != nil {
				return nil, fmt.Errorf("init data error: %w", err)
			}

			switch initMode {
			case "check":
				if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
					err = cache.Add(ctx, key, initData, ttl)
					if err != nil {
						err = fmt.Errorf("failed to set cache key '%s': %v", key, err)
					}
				}); cerr != nil {
					err = cerr
				}

				if err != nil {
					return nil, err
				}
			case "ignore":
				if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
					err = cache.Add(ctx, key, initData, ttl)
					if err != nil {
						if errors.Is(err, service.ErrKeyAlreadyExists) {
							err = nil
						} else {
							err = fmt.Errorf("failed to set cache key '%s': %v", key, err)
						}
					}
				}); cerr != nil {
					err = cerr
				}

				if err != nil {
					return nil, err
				}
			case "replace":
				if cerr := cc.mgr.AccessCache(ctx, cc.cacheName, func(cache service.Cache) {
					err = cache.Set(ctx, key, initData, ttl)
					if err != nil {
						err = fmt.Errorf("failed to set cache key '%s': %v", key, err)
					}
				}); cerr != nil {
					err = cerr
				}

				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("init_mode unsupported mode %s", initMode)
			}

			if passthroughMode == "processed" || passthroughMode == "all" {
				newMsgs = append(newMsgs, msg)
			}
		} else if passthroughMode == "unprocessed" || passthroughMode == "all" {
			newMsgs = append(newMsgs, msg)
		}
	}

	if len(newMsgs) > 0 {
		return []service.MessageBatch{newMsgs}, nil
	}

	return nil, nil
}

func (cc *cacheCollectorProcessor) Close(ctx context.Context) error {
	return nil
}
