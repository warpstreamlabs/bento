package bundle

import (
	"github.com/warpstreamlabs/bento/internal/docs"
)

// Environment is a collection of Bento component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This is useful for sandboxing, testing, etc.
type Environment struct {
	buffers    *BufferSet
	caches     *CacheSet
	inputs     *InputSet
	outputs    *OutputSet
	processors *ProcessorSet
	rateLimits *RateLimitSet
	metrics    *MetricsSet
	tracers    *TracerSet

	scanners *ScannerSet

	constructors *ConstructorSet
}

// NewEnvironment creates an empty environment.
func NewEnvironment() *Environment {
	return &Environment{
		buffers:      &BufferSet{},
		caches:       &CacheSet{},
		inputs:       &InputSet{},
		outputs:      &OutputSet{},
		processors:   &ProcessorSet{},
		rateLimits:   &RateLimitSet{},
		metrics:      &MetricsSet{},
		tracers:      &TracerSet{},
		scanners:     &ScannerSet{},
		constructors: &ConstructorSet{},
	}
}

// Clone an existing environment to a new one that can be modified
// independently.
func (e *Environment) Clone() *Environment {
	newEnv := NewEnvironment()
	for _, v := range e.constructors.ctors {
		_ = newEnv.constructors.Add(v)
	}
	for _, v := range e.buffers.specs {
		_ = newEnv.buffers.Add(v.constructor, v.spec)
	}
	for _, v := range e.caches.specs {
		_ = newEnv.caches.Add(v.constructor, v.spec)
	}
	for _, v := range e.inputs.specs {
		_ = newEnv.inputs.Add(v.constructor, v.spec)
	}
	for _, v := range e.outputs.specs {
		_ = newEnv.outputs.Add(v.constructor, v.spec)
	}
	for _, v := range e.processors.specs {
		_ = newEnv.processors.Add(v.constructor, v.spec)
	}
	for _, v := range e.rateLimits.specs {
		_ = newEnv.rateLimits.Add(v.constructor, v.spec)
	}
	for _, v := range e.metrics.specs {
		_ = newEnv.metrics.Add(v.constructor, v.spec)
	}
	for _, v := range e.tracers.specs {
		_ = newEnv.tracers.Add(v.constructor, v.spec)
	}
	for _, v := range e.scanners.specs {
		_ = newEnv.scanners.Add(v.constructor, v.spec)
	}
	return newEnv
}

// GetDocs returns a documentation spec for an implementation of a component.
func (e *Environment) GetDocs(name string, ctype docs.Type) (docs.ComponentSpec, bool) {
	var spec docs.ComponentSpec
	var ok bool

	switch ctype {
	case docs.TypeBuffer:
		spec, ok = e.buffers.DocsFor(name)
	case docs.TypeCache:
		spec, ok = e.caches.DocsFor(name)
	case docs.TypeInput:
		spec, ok = e.inputs.DocsFor(name)
	case docs.TypeOutput:
		spec, ok = e.outputs.DocsFor(name)
	case docs.TypeProcessor:
		spec, ok = e.processors.DocsFor(name)
	case docs.TypeRateLimit:
		spec, ok = e.rateLimits.DocsFor(name)
	case docs.TypeMetrics:
		spec, ok = e.metrics.DocsFor(name)
	case docs.TypeTracer:
		spec, ok = e.tracers.DocsFor(name)
	case docs.TypeScanner:
		spec, ok = e.scanners.DocsFor(name)
	}

	return spec, ok
}

// GlobalEnvironment contains service-wide singleton bundles.
var GlobalEnvironment = &Environment{
	buffers:      AllBuffers,
	caches:       AllCaches,
	inputs:       AllInputs,
	outputs:      AllOutputs,
	processors:   AllProcessors,
	rateLimits:   AllRateLimits,
	metrics:      AllMetrics,
	tracers:      AllTracers,
	scanners:     AllScanners,
	constructors: AllConstructors,
}

// WithBuffers returns a copy of Environment with a cloned plugin registry of
// buffers, where only the specified plugins are included.
func (e *Environment) WithBuffers(names ...string) *Environment {
	newEnv := *e
	newEnv.buffers = e.buffers.With(names...)
	return &newEnv
}

// WithCaches returns a copy of Environment with a cloned plugin registry of
// caches, where only the specified plugins are included.
func (e *Environment) WithCaches(names ...string) *Environment {
	newEnv := *e
	newEnv.caches = e.caches.With(names...)
	return &newEnv
}

// WithInputs returns a copy of Environment with a cloned plugin registry of
// inputs, where only the specified plugins are included.
func (e *Environment) WithInputs(names ...string) *Environment {
	newEnv := *e
	newEnv.inputs = e.inputs.With(names...)
	return &newEnv
}

// WithOutputs returns a copy of Environment with a cloned plugin registry of
// outputs, where only the specified plugins are included.
func (e *Environment) WithOutputs(names ...string) *Environment {
	newEnv := *e
	newEnv.outputs = e.outputs.With(names...)
	return &newEnv
}

// WithProcessors returns a copy of Environment with a cloned plugin registry
// of processors, where only the specified plugins are included.
func (e *Environment) WithProcessors(names ...string) *Environment {
	newEnv := *e
	newEnv.processors = e.processors.With(names...)
	return &newEnv
}

// WithRateLimits returns a copy of Environment with a cloned plugin registry
// of rate limits, where only the specified plugins are included.
func (e *Environment) WithRateLimits(names ...string) *Environment {
	newEnv := *e
	newEnv.rateLimits = e.rateLimits.With(names...)
	return &newEnv
}

// WithMetrics returns a copy of Environment with a cloned plugin registry of
// metrics, where only the specified plugins are included.
func (e *Environment) WithMetrics(names ...string) *Environment {
	newEnv := *e
	newEnv.metrics = e.metrics.With(names...)
	return &newEnv
}

// WithTracers returns a copy of Environment with a cloned plugin registry of
// tracers, where only the specified plugins are included.
func (e *Environment) WithTracers(names ...string) *Environment {
	newEnv := *e
	newEnv.tracers = e.tracers.With(names...)
	return &newEnv
}

// WithScanners returns a copy of Environment with a cloned plugin registry
// of scanners, where only the specified plugins are included.
func (e *Environment) WithScanners(names ...string) *Environment {
	newEnv := *e
	newEnv.scanners = e.scanners.With(names...)
	return &newEnv
}
