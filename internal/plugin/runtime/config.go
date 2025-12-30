//go:build !wasm

package runtime

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/experimental/sock"
	"github.com/warpstreamlabs/bento/internal/plugin"
)

var errInvalidPath = errors.New("cannot mount an empty path")

func toModuleConfig(conf plugin.ModuleConfig) (wazero.ModuleConfig, error) {
	moduleConf := wazero.NewModuleConfig().
		WithName(""). // Anonymous for multiple instances
		WithStartFunctions("_initialize")

	for k, v := range conf.Env {
		moduleConf = moduleConf.WithEnv(k, v)
	}

	fsConf := wazero.NewFSConfig()
	for _, mount := range conf.Mounts {
		if mount.HostPath == "" || mount.GuestPath == "" {
			// TODO: check if paths exist or log error if empty?
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

func toSocketConfig(sockets []string) (sock.Config, error) {
	if len(sockets) == 0 {
		return nil, nil
	}

	sockCfg := sock.NewConfig()
	for _, addr := range sockets {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid socket address '%s': %w", addr, err)
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid socket port in '%s': %w", addr, err)
		}

		if port < 0 || port > 65535 {
			return nil, fmt.Errorf("invalid socket port %d in '%s': must be between 0-65535", port, addr)
		}

		sockCfg = sockCfg.WithTCPListener(host, port)
	}

	return sockCfg, nil
}
