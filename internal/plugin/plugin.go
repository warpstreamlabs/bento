package plugin

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
	"golang.org/x/sync/errgroup"
)

const concurrentRegistrationLimit = 10

func InitPlugins[T any](ctx context.Context, env *bundle.Environment, rt runtime.Runtime[T], pluginPaths ...string) ([]string, error) {
	if len(pluginPaths) == 0 {
		return nil, nil
	}

	log.Printf("Initializing %d plugins.", len(pluginPaths))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrentRegistrationLimit)

	var lints []string
	var lintsMu sync.Mutex

	for _, path := range pluginPaths {
		group.Go(func() error {
			manifestPath := filepath.Join(path, "plugin.yaml")
			manifestBytes, err := ifs.ReadFile(ifs.OS(), manifestPath)
			if err != nil {
				return fmt.Errorf("plugin %v: failed to read manifest: %w", path, err)
			}

			manifest, manifestLints, err := runtime.ReadManifestYAML(manifestBytes)
			if err != nil {
				return fmt.Errorf("plugin %v: %w", path, err)
			}

			if len(manifestLints) > 0 {
				lintsMu.Lock()
				for _, l := range manifestLints {
					lints = append(lints, fmt.Sprintf("plugin %v: %v", path, l))
				}
				lintsMu.Unlock()
			}

			log.Printf("Compiling plugin [%s]", manifest.Name)

			plugin, err := rt.Register(groupCtx, manifest, runtime.DirSource(path))
			if err != nil {
				return fmt.Errorf("plugin %v failed to register: %w", manifest.Name, err)
			}

			if err := plugin.RegisterWith(env); err != nil {
				return fmt.Errorf("plugin %v: failed to register with environment: %w", manifest.Name, err)
			}

			log.Printf("Registered plugin [%s]", manifest.Name)
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return lints, nil
}
