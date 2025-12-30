package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"gopkg.in/yaml.v3"
)

const (
	fieldPlugins = "plugins"

	fieldRuntime          = "runtime"
	fieldRuntimeMaxMemory = "max_memory"

	fieldModules    = "modules"
	fieldModulePath = "path"
	fieldModuleName = "name"
	fieldModuleEnv  = "env"

	fieldMounts         = "mounts"
	fieldMountHostPath  = "host_path"
	fieldMountGuestPath = "guest_path"
	fieldMountReadOnly  = "read_only"

	fieldSockets = "sockets"
)

const defaultMaxMemory uint64 = 250 * 1024 * 1024

type Config struct {
	Runtime RuntimeConfig  `yaml:"runtime,omitempty"`
	Modules []ModuleConfig `yaml:"modules,omitempty"`
}

type ModuleConfig struct {
	Path    string            `yaml:"path"`
	Name    string            `yaml:"name"`
	Mounts  []MountConfig     `yaml:"mounts,omitempty"`
	Sockets []string          `yaml:"sockets,omitempty"`
	Env     map[string]string `yaml:"env,omitempty"`
}

type MountConfig struct {
	HostPath  string `yaml:"host_path"`
	GuestPath string `yaml:"guest_path"`
	ReadOnly  bool   `yaml:"read_only"`
}

type RuntimeConfig struct {
	MaxMemory uint64 `yaml:"max_memory"`
}

func NewConfig() Config {
	return Config{
		Runtime: RuntimeConfig{
			MaxMemory: defaultMaxMemory,
		},
		Modules: []ModuleConfig{},
	}
}

func FromParsed(pConf *docs.ParsedConfig) (Config, error) {
	conf := NewConfig()

	if pConf.Contains(fieldRuntime) {
		runtimeConf, err := parseRuntimeConfig(pConf.Namespace(fieldRuntime))
		if err != nil {
			return Config{}, err
		}
		conf.Runtime = runtimeConf
	}

	if pConf.Contains(fieldModules) {
		modules, err := parseModules(pConf)
		if err != nil {
			return Config{}, err
		}
		conf.Modules = modules
	}

	return conf, nil
}

func parseRuntimeConfig(rConf *docs.ParsedConfig) (RuntimeConfig, error) {
	var rc RuntimeConfig

	maxBytesStr, err := rConf.FieldString(fieldRuntimeMaxMemory)
	if err != nil {
		return RuntimeConfig{}, err
	}

	rc.MaxMemory, err = humanize.ParseBytes(maxBytesStr)
	if err != nil {
		return RuntimeConfig{}, fmt.Errorf("failed to parse max_memory: %w", err)
	}

	return rc, nil
}

func parseModules(pConf *docs.ParsedConfig) ([]ModuleConfig, error) {
	moduleList, err := pConf.FieldObjectList(fieldModules)
	if err != nil {
		return nil, err
	}

	modules := make([]ModuleConfig, 0, len(moduleList))
	for _, mConf := range moduleList {
		mod, err := parseModuleConfig(mConf)
		if err != nil {
			return nil, err
		}
		modules = append(modules, mod)
	}

	return modules, nil
}

func parseModuleConfig(mConf *docs.ParsedConfig) (ModuleConfig, error) {
	var mod ModuleConfig

	var err error
	if mod.Path, err = mConf.FieldString(fieldModulePath); err != nil {
		return ModuleConfig{}, err
	}

	if mod.Name, err = mConf.FieldString(fieldModuleName); err != nil {
		return ModuleConfig{}, err
	}

	if mConf.Contains(fieldModuleEnv) {
		if mod.Env, err = mConf.FieldStringMap(fieldModuleEnv); err != nil {
			return ModuleConfig{}, err
		}
	}

	if mConf.Contains(fieldMounts) {
		mounts, err := parseMounts(mConf)
		if err != nil {
			return ModuleConfig{}, err
		}
		mod.Mounts = mounts
	}

	if mConf.Contains(fieldSockets) {
		if mod.Sockets, err = mConf.FieldStringList(fieldSockets); err != nil {
			return ModuleConfig{}, err
		}
	}

	return mod, nil
}

func parseMounts(mConf *docs.ParsedConfig) ([]MountConfig, error) {
	mountList, err := mConf.FieldObjectList(fieldMounts)
	if err != nil {
		return nil, err
	}

	mounts := make([]MountConfig, 0, len(mountList))
	for _, mtConf := range mountList {
		mount, err := parseMountConfig(mtConf)
		if err != nil {
			return nil, err
		}
		mounts = append(mounts, mount)
	}

	return mounts, nil
}

func parseMountConfig(mtConf *docs.ParsedConfig) (MountConfig, error) {
	var mount MountConfig

	var err error
	if mount.HostPath, err = mtConf.FieldString(fieldMountHostPath); err != nil {
		return MountConfig{}, err
	}

	if mount.GuestPath, err = mtConf.FieldString(fieldMountGuestPath); err != nil {
		return MountConfig{}, err
	}

	if mount.ReadOnly, err = mtConf.FieldBool(fieldMountReadOnly); err != nil {
		return MountConfig{}, err
	}

	return mount, nil
}

func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldObject(fieldRuntime, "WASM runtime limits and behavior.").WithChildren(
			docs.FieldString(fieldRuntimeMaxMemory, "The maximum memory allowed per instance. Supports units (e.g. '10MB', '512KB').").HasDefault("250MB"),
		).Optional(),
		docs.FieldObject(fieldModules, "A list of WASM modules to load.").WithChildren(
			docs.FieldString(fieldModulePath, "The file path pointing to plugin binary (.wasm)."),
			docs.FieldString(fieldModuleName, "The name of the plugin."),
			docs.FieldString(fieldModuleEnv, "Environment variables to inject into the guest.").Map().Optional(),
			docs.FieldObject(fieldMounts, "A list of directory mounts.").WithChildren(
				docs.FieldString(fieldMountHostPath, "The path on the host machine to mount."),
				docs.FieldString(fieldMountGuestPath, "The path inside the WASM container."),
				docs.FieldBool(fieldMountReadOnly, "If true, the mount will be read-only.").HasDefault(true),
			).Array().Optional(),
			docs.FieldString(fieldSockets, "A list of TCP sockets to listen on in the form `host:port`.").Array().Optional(),
		).Array().Optional(),
	}
}

func ConfigSpec() docs.FieldSpec {
	return docs.FieldObject(
		fieldPlugins,
		"Plugin configuration for extending Bento with WebAssembly modules.",
	).WithChildren(FieldSpecs()...)
}

func ReadConfigYAML(confBytes []byte) (Config, []docs.Lint, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(confBytes))
	conf := NewConfig()

	for {
		var node yaml.Node
		err := decoder.Decode(&node)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return conf, nil, err
		}

		pConf, err := ConfigSpec().ParsedConfigFromAny(&node)
		if err != nil {
			return conf, nil, err
		}

		if !pConf.Contains(fieldPlugins) {
			continue
		}

		conf, err = FromParsed(pConf.Namespace(fieldPlugins))
		return conf, nil, err
	}

	return conf, nil, nil
}

// ReadConfigFile attempts to read a template configuration file.
func ReadConfigFile(path string) (Config, []string, error) {
	templateBytes, err := ifs.ReadFile(ifs.OS(), path)
	if err != nil {
		return Config{}, nil, err
	}
	conf, docLints, err := ReadConfigYAML(templateBytes)
	if err != nil {
		return Config{}, nil, err
	}

	var lints []string
	for _, lint := range docLints {
		lints = append(lints, lint.Error())
	}

	return conf, lints, nil

}
