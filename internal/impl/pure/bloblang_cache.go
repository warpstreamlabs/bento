package pure

import (
	"context"
	"sync/atomic"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	// HACK(gregfurman): The actual bloblang function spec should be registered
	// even before the ManagedConstructor has been run, circumventing both linting errors
	// as well as allowing docs to actually be generated.
	container := atomic.Value{}
	container.Store(service.MockResources())

	getManager := func() service.LimitedResources {
		return container.Load().(service.LimitedResources)
	}

	if err := registerBloblangFunctions(getManager); err != nil {
		panic(err)
	}

	if err := service.RegisterManagedConstructor(func(mgr service.LimitedResources) error {
		container.Store(mgr)
		return nil
	}); err != nil {
		panic(err)
	}

}

func registerBloblangFunctions(getMgr func() service.LimitedResources) error {
	if err := bloblang.RegisterFunctionV2("cache_get",
		bloblang.NewPluginSpec().
			Experimental().
			Impure().
			Category(query.FunctionCategoryEnvironment).
			Description("Used to retrieve a cached value from a cache resource.").
			Param(bloblang.NewStringParam("resource").Description("The name of the `cache` resource to target.")).
			Param(bloblang.NewStringParam("key").Description("The key of the value to retrieve from the `cache` resource.")),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			resource, err := args.GetString("resource")
			if err != nil {
				return nil, err
			}
			key, err := args.GetString("key")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				var (
					output []byte
					cerr   error
				)
				ctx := context.Background()
				if err := getMgr().AccessCache(ctx, resource, func(c service.Cache) {
					output, cerr = c.Get(ctx, key)
				}); err != nil {
					return nil, err
				}
				if cerr != nil {
					return nil, cerr
				}
				return output, nil
			}, nil
		}); err != nil {
		return err
	}

	if err := bloblang.RegisterFunctionV2("cache_set",
		bloblang.NewPluginSpec().
			Experimental().
			Impure().
			Category(query.FunctionCategoryEnvironment).
			Description("Set a key in the cache resource to a value. If the key already exists the contents are overridden.").
			Param(bloblang.NewStringParam("resource").Description("The name of the `cache` resource to target.")).
			Param(bloblang.NewStringParam("key").Description("A key to use with the `cache`.")).
			Param(bloblang.NewStringParam("value").Description("A value to use with the `cache`.")),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			resource, err := args.GetString("resource")
			if err != nil {
				return nil, err
			}
			key, err := args.GetString("key")
			if err != nil {
				return nil, err
			}
			value, err := args.GetString("value")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				var cerr error
				ctx := context.Background()
				if err := getMgr().AccessCache(ctx, resource, func(c service.Cache) {
					cerr = c.Set(ctx, key, []byte(value), nil)
				}); err != nil {
					return nil, err
				}
				return nil, cerr
			}, nil
		}); err != nil {
		return err
	}

	if err := bloblang.RegisterFunctionV2("cache_delete",
		bloblang.NewPluginSpec().
			Experimental().
			Impure().
			Category(query.FunctionCategoryEnvironment).
			Description("Delete a key and its contents from the cache. If the key does not exist the action is a no-op and will not fail with an error.").
			Param(bloblang.NewStringParam("resource").Description("The name of the `cache` resource to target.")).
			Param(bloblang.NewStringParam("key").Description("A key to use with the `cache`.")),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			resource, err := args.GetString("resource")
			if err != nil {
				return nil, err
			}
			key, err := args.GetString("key")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				var cerr error
				ctx := context.Background()
				if err := getMgr().AccessCache(ctx, resource, func(c service.Cache) {
					cerr = c.Delete(ctx, key)
				}); err != nil {
					return nil, err
				}
				return nil, cerr
			}, nil
		}); err != nil {
		return err
	}

	if err := bloblang.RegisterFunctionV2("cache_add",
		bloblang.NewPluginSpec().
			Experimental().
			Impure().
			Category(query.FunctionCategoryEnvironment).
			Description("Set a key in the cache resource to a value. If the key already exists the action fails with a 'key already exists' error.").
			Param(bloblang.NewStringParam("resource").Description("The name of the `cache` resource to target.")).
			Param(bloblang.NewStringParam("key").Description("A key to use with the `cache`.")).
			Param(bloblang.NewStringParam("value").Description("A value to use with the `cache`.")),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			resource, err := args.GetString("resource")
			if err != nil {
				return nil, err
			}
			key, err := args.GetString("key")
			if err != nil {
				return nil, err
			}
			value, err := args.GetString("value")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				var cerr error
				ctx := context.Background()
				if err := getMgr().AccessCache(ctx, resource, func(c service.Cache) {
					cerr = c.Add(ctx, key, []byte(value), nil)
				}); err != nil {
					return nil, err
				}
				return nil, cerr
			}, nil
		}); err != nil {
		return err
	}

	return nil
}
