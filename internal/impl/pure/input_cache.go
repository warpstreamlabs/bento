package pure

import (
	"context"
	"errors"
	"fmt"

	"github.com/warpstreamlabs/bento/public/service"
)

const ciFieldResource = "resource"

func cacheInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Utility").
		Version("1.20.0").
		Summary("Reads items stored within a [cache resource](/docs/components/caches/about), emitting each item as a message and shutting down once the contents of the cache have been fully consumed.").
		Description(`
The target cache must support listing its keys, which is an optional capability. Caches that currently support this are: `+"`aws_s3`, `file`, `gcp_cloud_storage` and `memory`"+`. Attempting to consume from any other cache will result in a connection error.

The set of keys to read is captured when the input connects, after which each item is read individually. Items deleted after the keys were captured are skipped, and items added after the keys were captured are not consumed. The order in which items are read is undefined. If the input disconnects mid-stream due to an error it will capture the set of keys again once it reconnects, in which case items may be emitted a second time.

### Metadata

This input adds the following metadata fields to each message:

- cache_key

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(ciFieldResource).
				Description("The [`cache` resource](/docs/components/caches/about) to read from."),
			service.NewAutoRetryNacksToggleField(),
		).
		Example(
			"Migrate cache items",
			"Reads all items from a `file` cache and writes them into a `redis` cache under the same keys.",
			`
input:
  cache:
    resource: source

output:
  cache:
    target: destination
    key: ${! metadata("cache_key") }

cache_resources:
  - label: source
    file:
      directory: /tmp/cache_items
  - label: destination
    redis:
      url: tcp://localhost:6379
`,
		)
}

func init() {
	err := service.RegisterInput(
		"cache", cacheInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newCacheInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

func newCacheInputFromConfig(conf *service.ParsedConfig, res *service.Resources) (*cacheInput, error) {
	cacheName, err := conf.FieldString(ciFieldResource)
	if err != nil {
		return nil, err
	}
	if !res.HasCache(cacheName) {
		return nil, fmt.Errorf("cache resource '%v' was not found", cacheName)
	}
	return &cacheInput{
		res:       res,
		cacheName: cacheName,
	}, nil
}

//------------------------------------------------------------------------------

type cacheInput struct {
	res       *service.Resources
	cacheName string

	keys []string
}

func (c *cacheInput) Connect(ctx context.Context) error {
	var keys []string
	var listErr error
	if err := c.res.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		lister, ok := cache.(service.ListableCache)
		if !ok {
			listErr = fmt.Errorf("cache resource '%v' does not support listing keys", c.cacheName)
			return
		}
		keys, listErr = lister.ListKeys(ctx)
	}); err != nil {
		return err
	}
	if listErr != nil {
		return listErr
	}
	c.keys = keys
	return nil
}

func (c *cacheInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for len(c.keys) > 0 {
		key := c.keys[0]
		c.keys = c.keys[1:]

		var value []byte
		var getErr error
		if err := c.res.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
			value, getErr = cache.Get(ctx, key)
		}); err != nil {
			return nil, nil, err
		}
		if errors.Is(getErr, service.ErrKeyNotFound) {
			// The item was deleted after the keys were listed.
			continue
		}
		if getErr != nil {
			return nil, nil, getErr
		}

		msg := service.NewMessage(value)
		msg.MetaSetMut("cache_key", key)
		return msg, func(context.Context, error) error { return nil }, nil
	}
	return nil, nil, service.ErrEndOfInput
}

func (c *cacheInput) Close(ctx context.Context) error {
	return nil
}
