package manager

import (
	"github.com/warpstreamlabs/bento/v1/internal/component/cache"
	"github.com/warpstreamlabs/bento/v1/internal/component/input"
	"github.com/warpstreamlabs/bento/v1/internal/component/output"
	"github.com/warpstreamlabs/bento/v1/internal/component/processor"
	"github.com/warpstreamlabs/bento/v1/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
)

const (
	fieldResourceInputs     = "input_resources"
	fieldResourceProcessors = "processor_resources"
	fieldResourceOutputs    = "output_resources"
	fieldResourceCaches     = "cache_resources"
	fieldResourceRateLimits = "rate_limit_resources"
)

// ResourceConfig contains fields for specifying resource components at the root
// of a Bento config.
type ResourceConfig struct {
	ResourceInputs     []input.Config     `yaml:"input_resources,omitempty"`
	ResourceProcessors []processor.Config `yaml:"processor_resources,omitempty"`
	ResourceOutputs    []output.Config    `yaml:"output_resources,omitempty"`
	ResourceCaches     []cache.Config     `yaml:"cache_resources,omitempty"`
	ResourceRateLimits []ratelimit.Config `yaml:"rate_limit_resources,omitempty"`
}

// NewResourceConfig creates a ResourceConfig with default values.
func NewResourceConfig() ResourceConfig {
	return ResourceConfig{
		ResourceInputs:     []input.Config{},
		ResourceProcessors: []processor.Config{},
		ResourceOutputs:    []output.Config{},
		ResourceCaches:     []cache.Config{},
		ResourceRateLimits: []ratelimit.Config{},
	}
}

// AddFrom takes another Config and adds all of its resources to itself. If
// there are any resource name collisions an error is returned.
func (r *ResourceConfig) AddFrom(extra *ResourceConfig) error {
	r.ResourceInputs = append(r.ResourceInputs, extra.ResourceInputs...)
	r.ResourceProcessors = append(r.ResourceProcessors, extra.ResourceProcessors...)
	r.ResourceOutputs = append(r.ResourceOutputs, extra.ResourceOutputs...)
	r.ResourceCaches = append(r.ResourceCaches, extra.ResourceCaches...)
	r.ResourceRateLimits = append(r.ResourceRateLimits, extra.ResourceRateLimits...)
	return nil
}

func FromAny(prov docs.Provider, v any) (conf ResourceConfig, err error) {
	var pConf *docs.ParsedConfig
	if pConf, err = Spec().ParsedConfigFromAny(v); err != nil {
		return
	}
	return FromParsed(prov, pConf)
}

func FromParsed(prov docs.Provider, pConf *docs.ParsedConfig) (conf ResourceConfig, err error) {
	conf = NewResourceConfig()

	var l []*docs.ParsedConfig
	var v any

	if l, err = pConf.FieldAnyList(fieldResourceInputs); err != nil {
		return
	}
	for _, p := range l {
		if v, err = p.FieldAny(); err != nil {
			return
		}
		var c input.Config
		if c, err = input.FromAny(prov, v); err != nil {
			return
		}
		conf.ResourceInputs = append(conf.ResourceInputs, c)
	}

	if l, err = pConf.FieldAnyList(fieldResourceProcessors); err != nil {
		return
	}
	for _, p := range l {
		if v, err = p.FieldAny(); err != nil {
			return
		}
		var c processor.Config
		if c, err = processor.FromAny(prov, v); err != nil {
			return
		}
		conf.ResourceProcessors = append(conf.ResourceProcessors, c)
	}

	if l, err = pConf.FieldAnyList(fieldResourceOutputs); err != nil {
		return
	}
	for _, p := range l {
		if v, err = p.FieldAny(); err != nil {
			return
		}
		var c output.Config
		if c, err = output.FromAny(prov, v); err != nil {
			return
		}
		conf.ResourceOutputs = append(conf.ResourceOutputs, c)
	}

	if l, err = pConf.FieldAnyList(fieldResourceCaches); err != nil {
		return
	}
	for _, p := range l {
		if v, err = p.FieldAny(); err != nil {
			return
		}
		var c cache.Config
		if c, err = cache.FromAny(prov, v); err != nil {
			return
		}
		conf.ResourceCaches = append(conf.ResourceCaches, c)
	}

	if l, err = pConf.FieldAnyList(fieldResourceRateLimits); err != nil {
		return
	}
	for _, p := range l {
		if v, err = p.FieldAny(); err != nil {
			return
		}
		var c ratelimit.Config
		if c, err = ratelimit.FromAny(prov, v); err != nil {
			return
		}
		conf.ResourceRateLimits = append(conf.ResourceRateLimits, c)
	}
	return
}
