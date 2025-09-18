package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
)

// NewProcessorField defines a new processor field, it is then possible to
// extract an OwnedProcessor from the resulting parsed config with the method
// FieldProcessor.
func NewProcessorField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldProcessor(name, ""),
	}
}

// FieldProcessor accesses a field from a parsed config that was defined with
// NewProcessorField and returns an OwnedProcessor, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldProcessor(path ...string) (*OwnedProcessor, error) {
	v, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	conf, err := processor.FromAny(p.mgr.Environment(), v)
	if err != nil {
		return nil, err
	}

	iproc, err := p.mgr.IntoPath(path...).NewProcessor(conf)
	if err != nil {
		return nil, err
	}
	return &OwnedProcessor{iproc}, nil
}

// NewProcessorListField defines a new processor list field, it is then possible
// to extract a list of OwnedProcessor from the resulting parsed config with the
// method FieldProcessorList.
func NewProcessorListField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldProcessor(name, "").Array(),
	}
}

func (p *ParsedConfig) fieldProcessorListConfigs(path ...string) ([]processor.Config, error) {
	proc, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	procsArray, ok := proc.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected array, got %T", proc)
	}

	var procConfigs []processor.Config
	for i, iConf := range procsArray {
		pconf, err := processor.FromAny(p.mgr.Environment(), iConf)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", i, err)
		}
		procConfigs = append(procConfigs, pconf)
	}
	return procConfigs, nil
}

// FieldProcessorList accesses a field from a parsed config that was defined
// with NewProcessorListField and returns a slice of OwnedProcessor, or an error
// if the configuration was invalid.
func (p *ParsedConfig) FieldProcessorList(path ...string) ([]*OwnedProcessor, error) {
	procConfigs, err := p.fieldProcessorListConfigs(path...)
	if err != nil {
		return nil, err
	}

	tmpMgr := p.mgr.IntoPath(path...)
	procs := make([]*OwnedProcessor, len(procConfigs))
	for i, c := range procConfigs {
		iproc, err := tmpMgr.IntoPath(strconv.Itoa(i)).NewProcessor(c)
		if err != nil {
			return nil, fmt.Errorf("processor %v: %w", i, err)
		}
		procs[i] = &OwnedProcessor{iproc}
	}

	return procs, nil
}

// NewProcessorMapField defines a new processor map field, it is then possible to
// extract a map of OwnedProcessor from the resulting parsed config with the
// method FieldProcessorMap.
func NewProcessorMapField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldProcessor(name, "").Map(),
	}
}

// FieldProcessorMap accesses a field from a parsed config that was defined
// with NewProcessorMapField and returns a map of OwnedProcessor, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldProcessorMap(path ...string) (map[string]*OwnedProcessor, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldMap, ok := field.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected object, got %T", field)
	}

	tmpMgr := p.mgr.IntoPath(path...)
	procs := make(map[string]*OwnedProcessor, len(fieldMap))
	for k, v := range fieldMap {
		conf, err := processor.FromAny(p.mgr.Environment(), v)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", k, err)
		}

		iproc, err := tmpMgr.IntoPath(k).NewProcessor(conf)
		if err != nil {
			return nil, fmt.Errorf("processor %v: %w", k, err)
		}
		procs[k] = &OwnedProcessor{iproc}
	}

	return procs, nil
}
