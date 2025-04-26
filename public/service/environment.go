package service

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	ibloblang "github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	iprocessors "github.com/warpstreamlabs/bento/internal/component/input/processors"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/output/batcher"
	oprocessors "github.com/warpstreamlabs/bento/internal/component/output/processors"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/component/scanner"
	"github.com/warpstreamlabs/bento/internal/component/tracer"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/template"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

// Environment is a collection of Bento component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This can be useful for sandboxing, testing, etc, but most plugin
// authors do not need to create an Environment and can simply use the global
// environment.
type Environment struct {
	internal    *bundle.Environment
	bloblangEnv *bloblang.Environment
	fs          ifs.FS
}

var globalEnvironment = &Environment{
	internal:    bundle.GlobalEnvironment,
	bloblangEnv: bloblang.GlobalEnvironment(),
	fs:          ifs.OS(),
}

// GlobalEnvironment returns a reference to the global environment, adding
// plugins to this environment is the equivalent to adding plugins using global
// Functions.
func GlobalEnvironment() *Environment {
	return globalEnvironment
}

// NewEnvironment creates a new environment that inherits all globally defined
// plugins, but can have plugins defined on it that are isolated.
func NewEnvironment() *Environment {
	return globalEnvironment.Clone()
}

// NewEmptyEnvironment creates a new environment with zero registered plugins.
func NewEmptyEnvironment() *Environment {
	return &Environment{
		internal:    bundle.NewEnvironment(),
		bloblangEnv: bloblang.NewEmptyEnvironment(),
		fs:          ifs.OS(),
	}
}

// Clone an environment, creating a new environment containing the same plugins
// that can be modified independently of the source.
func (e *Environment) Clone() *Environment {
	return &Environment{
		internal:    e.internal.Clone(),
		bloblangEnv: e.bloblangEnv.WithoutFunctions().WithoutMethods(),
		fs:          e.fs,
	}
}

// UseBloblangEnvironment configures the service environment to restrict
// components constructed with it to a specific Bloblang environment.
func (e *Environment) UseBloblangEnvironment(bEnv *bloblang.Environment) {
	e.bloblangEnv = bEnv
}

// UseFS configures the service environment to use an instantiation of *FS as
// its filesystem. This provides extra control over the file access of all
// Bento components within the stream. However, this functionality is opt-in
// and there is no guarantee that plugin implementations will use this method
// of file access.
//
// The underlying bloblang environment will also be configured to import
// mappings and other files via this file access method. In order to avoid this
// behaviour add a fresh bloblang environment via UseBloblangEnvironment _after_
// setting this file access.
func (e *Environment) UseFS(fs *FS) {
	e.fs = fs
	e.bloblangEnv = e.bloblangEnv.WithCustomImporter(func(name string) ([]byte, error) {
		return ifs.ReadFile(fs, name)
	})
}

// NewStreamBuilder creates a new StreamBuilder upon the defined environment,
// only components known to this environment will be available to the stream
// builder.
func (e *Environment) NewStreamBuilder() *StreamBuilder {
	sb := NewStreamBuilder()
	sb.env = e
	return sb
}

//------------------------------------------------------------------------------

func (e *Environment) getBloblangParserEnv() *ibloblang.Environment {
	if unwrapper, ok := e.bloblangEnv.XUnwrapper().(interface {
		Unwrap() *ibloblang.Environment
	}); ok {
		return unwrapper.Unwrap()
	}
	return ibloblang.GlobalEnvironment()
}

//------------------------------------------------------------------------------

// RegisterBatchBuffer attempts to register a new buffer plugin by providing a
// description of the configuration for the buffer and a constructor for the
// buffer processor. The constructor will be called for each instantiation of
// the component within a config.
//
// Consumed message batches must be created by upstream components (inputs, etc)
// otherwise this buffer will simply receive batches containing single
// messages.
func (e *Environment) RegisterBatchBuffer(name string, spec *ConfigSpec, ctor BatchBufferConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeBuffer
	return e.internal.BufferAdd(func(conf buffer.Config, nm bundle.NewManagement) (buffer.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		b, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return buffer.NewStream(conf.Type, newAirGapBatchBuffer(b), nm), nil
	}, componentSpec)
}

// WalkBuffers executes a provided function argument for every buffer component
// that has been registered to the environment.
func (e *Environment) WalkBuffers(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.BufferDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetBufferConfig attempts to obtain a buffer configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetBufferConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllBuffers.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterCache attempts to register a new cache plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the cache itself. The constructor will be called for each instantiation of
// the component within a config.
func (e *Environment) RegisterCache(name string, spec *ConfigSpec, ctor CacheConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeCache
	return e.internal.CacheAdd(func(conf cache.Config, nm bundle.NewManagement) (cache.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		c, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapCache(c, nm.Metrics()), nil
	}, componentSpec)
}

// WalkCaches executes a provided function argument for every cache component
// that has been registered to the environment.
func (e *Environment) WalkCaches(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.CacheDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetCacheConfig attempts to obtain a cache configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetCacheConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllCaches.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterInput attempts to register a new input plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the input itself. The constructor will be called for each instantiation of
// the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacks to get automatic retries.
func (e *Environment) RegisterInput(name string, spec *ConfigSpec, ctor InputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeInput
	return e.internal.InputAdd(iprocessors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		i, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		rdr := newAirGapReader(i)
		return input.NewAsyncReader(conf.Type, rdr, nm)
	}), componentSpec)
}

// RegisterBatchInput attempts to register a new batched input plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the input itself. The constructor will be called for each
// instantiation of the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacksBatched to get automatic
// retries.
func (e *Environment) RegisterBatchInput(name string, spec *ConfigSpec, ctor BatchInputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeInput
	return e.internal.InputAdd(iprocessors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		i, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		if u, ok := i.(interface {
			Unwrap() input.Streamed
		}); ok {
			return u.Unwrap(), nil
		}
		rdr := newAirGapBatchReader(i)
		return input.NewAsyncReader(conf.Type, rdr, nm)
	}), componentSpec)
}

// WalkInputs executes a provided function argument for every input component
// that has been registered to the environment.
func (e *Environment) WalkInputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.InputDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetInputConfig attempts to obtain an input configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetInputConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllInputs.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
func (e *Environment) RegisterOutput(name string, spec *ConfigSpec, ctor OutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return e.internal.OutputAdd(oprocessors.WrapConstructor(
		func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
			pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
			if err != nil {
				return nil, err
			}
			op, maxInFlight, err := ctor(pluginConf, newResourcesFromManager(nm))
			if err != nil {
				return nil, err
			}
			if maxInFlight < 1 {
				return nil, fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)
			}
			w := newAirGapWriter(op)
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm)
			if err != nil {
				return nil, err
			}
			return output.OnlySinglePayloads(o), nil
		},
	), componentSpec)
}

// RegisterBatchOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
//
// The constructor of a batch output is able to return a batch policy to be
// applied before calls to write are made, creating batches from the stream of
// messages. However, batches can also be created by upstream components
// (inputs, buffers, etc).
//
// If a batch has been formed upstream it is possible that its size may exceed
// the policy specified in your constructor.
func (e *Environment) RegisterBatchOutput(name string, spec *ConfigSpec, ctor BatchOutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return e.internal.OutputAdd(oprocessors.WrapConstructor(
		func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
			pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
			if err != nil {
				return nil, err
			}
			op, batchPolicy, maxInFlight, err := ctor(pluginConf, newResourcesFromManager(nm))
			if err != nil {
				return nil, err
			}
			if u, ok := op.(interface {
				Unwrap() output.Streamed
			}); ok {
				return u.Unwrap(), nil
			}

			if maxInFlight < 1 {
				return nil, fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)
			}

			w := newAirGapBatchWriter(op)
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm)
			if err != nil {
				return nil, err
			}
			return batcher.NewFromConfig(batchPolicy.toInternal(), o, nm)
		},
	), componentSpec)
}

// WalkOutputs executes a provided function argument for every output component
// that has been registered to the environment.
func (e *Environment) WalkOutputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.OutputDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetOutputConfig attempts to obtain an output configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetOutputConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllOutputs.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterProcessor attempts to register a new processor plugin by providing
// a description of the configuration for the processor and a constructor for
// the processor itself. The constructor will be called for each instantiation
// of the component within a config.
//
// For simple transformations consider implementing a Bloblang plugin method
// instead.
func (e *Environment) RegisterProcessor(name string, spec *ConfigSpec, ctor ProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return e.internal.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapProcessor(conf.Type, r, nm), nil
	}, componentSpec)
}

// RegisterBatchProcessor attempts to register a new processor plugin by
// providing a description of the configuration for the processor and a
// constructor for the processor itself. The constructor will be called for each
// instantiation of the component within a config.
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise this processor will simply receive batches containing single
// messages.
func (e *Environment) RegisterBatchProcessor(name string, spec *ConfigSpec, ctor BatchProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return e.internal.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		if u, ok := r.(interface {
			Unwrap() processor.V1
		}); ok {
			return u.Unwrap(), nil
		}
		return newAirGapBatchProcessor(conf.Type, r, nm), nil
	}, componentSpec)
}

// WalkProcessors executes a provided function argument for every processor
// component that has been registered to the environment.
func (e *Environment) WalkProcessors(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.ProcessorDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetProcessorConfig attempts to obtain a processor configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetProcessorConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllProcessors.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterRateLimit attempts to register a new rate limit plugin by providing
// a description of the configuration for the plugin as well as a constructor
// for the rate limit itself. The constructor will be called for each
// instantiation of the component within a config.
func (e *Environment) RegisterRateLimit(name string, spec *ConfigSpec, ctor RateLimitConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeRateLimit
	return e.internal.RateLimitAdd(func(conf ratelimit.Config, nm bundle.NewManagement) (ratelimit.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		// TODO: This MessageAwareRateLimit should eventually replace V1
		// Try to upgrade to message aware rate-limiter if possible.

		// When registering from an internal rate limit (local rate limit, redis rate limit)
		if rl, ok := r.(ratelimit.MessageAwareRateLimit); ok {
			agrl := newReverseAirGapMessageAwareRateLimit(rl)
			return newAirGapMessageAwareRateLimit(agrl, nm.Metrics()), nil
		}

		// When registering an external rate limit via the plugin
		if rl, ok := r.(MessageAwareRateLimit); ok {
			return newAirGapMessageAwareRateLimit(rl, nm.Metrics()), nil
		}

		return newAirGapRateLimit(r, nm.Metrics()), nil
	}, componentSpec)
}

// WalkRateLimits executes a provided function argument for every rate limit
// component that has been registered to the environment.
func (e *Environment) WalkRateLimits(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.RateLimitDocs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetRateLimitConfig attempts to obtain a rate limit configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetRateLimitConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllRateLimits.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterMetricsExporter attempts to register a new metrics exporter plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the metrics exporter itself.
func (e *Environment) RegisterMetricsExporter(name string, spec *ConfigSpec, ctor MetricsExporterConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeMetrics
	return e.internal.MetricsAdd(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		m, err := ctor(pluginConf, newReverseAirGapLogger(nm.Logger()))
		if err != nil {
			return nil, err
		}
		return newAirGapMetrics(m), nil
	}, componentSpec)
}

// WalkMetrics executes a provided function argument for every metrics component
// that has been registered to the environment. Note that metrics components
// available to an environment cannot be modified.
func (e *Environment) WalkMetrics(fn func(name string, config *ConfigView)) {
	for _, v := range bundle.AllMetrics.Docs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetMetricsConfig attempts to obtain a metrics exporter configuration spec by
// the component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetMetricsConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllMetrics.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterOtelTracerProvider attempts to register a new open telemetry tracer
// provider plugin by providing a description of the configuration for the
// plugin as well as a constructor for the metrics exporter itself. The
// constructor will be called for each instantiation of the component within a
// config.
//
// Experimental: This type signature is experimental and therefore subject to
// change outside of major version releases.
func (e *Environment) RegisterOtelTracerProvider(name string, spec *ConfigSpec, ctor OtelTracerProviderConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeTracer
	return e.internal.TracersAdd(func(conf tracer.Config, nm bundle.NewManagement) (trace.TracerProvider, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		t, err := ctor(pluginConf)
		if err != nil {
			return nil, err
		}
		return t, nil
	}, componentSpec)
}

// WalkTracers executes a provided function argument for every tracer component
// that has been registered to the environment. Note that tracer components
// available to an environment cannot be modified.
func (e *Environment) WalkTracers(fn func(name string, config *ConfigView)) {
	for _, v := range bundle.AllTracers.Docs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetTracerConfig attempts to obtain a tracer configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetTracerConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllTracers.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterBatchScannerCreator attempts to register a new batched scanner plugin
// by providing a description of the configuration for the plugin as well as a
// constructor for the scanner creator itself. The constructor will be called
// for each instantiation of the component within a config.
func (e *Environment) RegisterBatchScannerCreator(name string, spec *ConfigSpec, ctor BatchScannerCreatorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeScanner
	return e.internal.ScannerAdd(func(conf scanner.Config, nm bundle.NewManagement) (scanner.Creator, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin)
		if err != nil {
			return nil, err
		}
		c, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapBatchScannerCreator(c), nil
	}, componentSpec)
}

// WalkScanners executes a provided function argument for every scanner
// component that has been registered to the environment. Note that scanner
// components available to an environment cannot be modified.
func (e *Environment) WalkScanners(fn func(name string, config *ConfigView)) {
	for _, v := range bundle.AllScanners.Docs() {
		fn(v.Name, &ConfigView{
			prov:      e.internal,
			component: v,
		})
	}
}

// GetScannerConfig attempts to obtain a scanner configuration spec by the
// component name. Returns a nil ConfigView and false if the component is
// unknown.
func (e *Environment) GetScannerConfig(name string) (*ConfigView, bool) {
	c, exists := bundle.AllScanners.DocsFor(name)
	if !exists {
		return nil, false
	}
	return &ConfigView{
		prov:      e.internal,
		component: c,
	}, true
}

// RegisterTemplateYAML attempts to register a template, defined as a YAML
// document, to the environment such that it may be used similarly to any other
// component plugin.
func (e *Environment) RegisterTemplateYAML(yamlStr string) error {
	return template.RegisterTemplateYAML(e.internal, []byte(yamlStr))
}

// XFormatConfigJSON returns a byte slice of the Bento configuration spec
// formatted as a JSON object. The schema of this method is undocumented and is
// not intended for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func XFormatConfigJSON() ([]byte, error) {
	return json.Marshal(config.Spec())
}

// XRateLimitInitForTest is a helper specifically for testing the internal rate
// limit initialization process based on the environment's registered components.
// DO NOT USE OUTSIDE OF TESTS.
func (e *Environment) XRateLimitInitForTest(conf ratelimit.Config, mgr bundle.NewManagement) (ratelimit.V1, error) {
	return e.internal.RateLimitInit(conf, mgr)
}
