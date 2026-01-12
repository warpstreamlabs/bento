package extismv1

import (
	"errors"

	"github.com/tetratelabs/wazero"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

const wasmPageSize uint64 = 65_536 // 64KB

var errInvalidPath = errors.New("cannot mount an empty path")

func toRuntimeConfig(conf *runtime.Manifest) wazero.RuntimeConfig {
	runtimeSettings := wazero.NewRuntimeConfig().WithCloseOnContextDone(true).WithDebugInfoEnabled(true)
	if conf.Runtime.Wasm.Memory.MaxPages > 0 {
		runtimeSettings = runtimeSettings.WithMemoryLimitPages(uint32(conf.Runtime.Wasm.Memory.MaxPages)).WithMemoryCapacityFromMax(true)
	}

	return runtimeSettings
}

func toModuleConfig(conf *runtime.Manifest) (wazero.ModuleConfig, error) {
	moduleConf := wazero.NewModuleConfig().
		WithName("") // Anonymous for multiple instances
		// WithStartFunctions("_initialize")

	for k, v := range conf.Runtime.Wasm.Env {
		moduleConf = moduleConf.WithEnv(k, v)
	}

	fsConf := wazero.NewFSConfig()
	for _, mount := range conf.Runtime.Wasm.Mounts {
		if mount.HostPath == "" || mount.GuestPath == "" {
			return nil, errInvalidPath
		}

		if mount.ReadOnly {
			fsConf = fsConf.WithReadOnlyDirMount(mount.HostPath, mount.GuestPath)
		} else {
			fsConf = fsConf.WithDirMount(mount.HostPath, mount.GuestPath)
		}
	}

	moduleConf = moduleConf.WithFSConfig(fsConf)
	return moduleConf, nil
}
