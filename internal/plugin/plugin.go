package plugin

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
	"golang.org/x/sync/errgroup"
)

const concurrentRegistrationLimit = 5

type Plugin[T any] interface {
	Name() string
	RegisterWith(env *bundle.Environment) error
	Spec() docs.ComponentSpec
}

type Runtime[T any] interface {
	Load(ctx context.Context, conf RuntimeConfig) error
	Register(ctx context.Context, conf ModuleConfig) (Plugin[T], error)
	Close(ctx context.Context) error
}

type Pool[T any] interface {
	Get(ctx context.Context) (T, error)
	Put(instance T)
	Close(ctx context.Context) error
}

func InitPluginsFromConfig[T any](ctx context.Context, runtime Runtime[T], path string) ([]string, error) {
	conf, lints, err := ReadConfigFile(path)
	if err != nil {
		return nil, err
	}

	if err := runtime.Load(ctx, conf.Runtime); err != nil {
		return nil, err
	}

	return lints, InitPlugins(bundle.GlobalEnvironment, runtime, conf)
}

func InitPlugins[T any](env *bundle.Environment, runtime Runtime[T], conf Config) error {
	ctx := context.Background()

	if len(conf.Modules) == 0 {
		return nil
	}

	log.Printf("Initializing %d plugins.", len(conf.Modules))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrentRegistrationLimit)

	mu := sync.Mutex{}
	for _, module := range conf.Modules {
		// Compilation can take a bit of time, so parallelize this while
		// limiting in-flight compilations to 5 at a time.
		group.Go(func() error {
			log.Printf("Compiling [%s]\n", module.Name)

			plugin, err := runtime.Register(groupCtx, module)
			if err != nil {
				return fmt.Errorf("failed to register plugin %s: %w", module.Name, err)
			}

			mu.Lock()
			defer mu.Unlock()

			if err := plugin.RegisterWith(env); err != nil {
				return fmt.Errorf("failed to register plugin '%s' with environment: %w", module.Name, err)
			}

			log.Printf("Registered [%s]\n", module.Name)
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}
