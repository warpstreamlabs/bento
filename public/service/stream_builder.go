package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/api"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/bundle/strict"
	"github.com/warpstreamlabs/bento/internal/bundle/tracing"
	"github.com/warpstreamlabs/bento/internal/cli"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/component/tracer"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/errorhandling"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/stream"
)

// StreamBuilder provides methods for building a Bento stream configuration.
// When parsing Bento configs this builder follows the schema and field
// defaults of a standard Bento configuration. Environment variable
// interpolations are also parsed and resolved the same as regular configs.
//
// Streams built with a stream builder have the HTTP server for exposing metrics
// and ready checks disabled by default, which is the only deviation away from a
// standard Bento default configuration. In order to enable the server set the
// configuration field `http.enabled` to `true` explicitly, or use `SetHTTPMux`
// in order to provide an explicit HTTP multiplexer for registering those
// endpoints.
type StreamBuilder struct {
	engineVersion string

	http       api.Config
	threads    int
	inputs     []input.Config
	buffer     buffer.Config
	processors []processor.Config
	outputs    []output.Config
	resources  manager.ResourceConfig
	metrics    metrics.Config
	tracer     tracer.Config
	logger     log.Config
	errHandler errorhandling.Config

	producerChan chan message.Transaction
	producerID   string
	consumerFunc MessageBatchHandlerFunc
	consumerID   string

	apiMut       manager.APIReg
	customLogger log.Modular

	configSpec      docs.FieldSpecs
	env             *Environment
	lintingDisabled bool
	envVarLookupFn  func(string) (string, bool)
}

// NewStreamBuilder creates a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	httpConf := api.NewConfig()
	httpConf.Enabled = false

	tmpSpec := config.Spec()
	tmpSpec.SetDefault(false, "http", "enabled")

	return &StreamBuilder{
		http:           httpConf,
		buffer:         buffer.NewConfig(),
		resources:      manager.NewResourceConfig(),
		metrics:        metrics.NewConfig(),
		tracer:         tracer.NewConfig(),
		logger:         log.NewConfig(),
		errHandler:     errorhandling.NewConfig(),
		configSpec:     tmpSpec,
		env:            globalEnvironment,
		envVarLookupFn: os.LookupEnv,
	}
}

func (s *StreamBuilder) getLintContext() docs.LintContext {
	conf := docs.NewLintConfig(s.env.internal)
	conf.DocsProvider = s.env.internal
	conf.BloblangEnv = s.env.bloblangEnv.Deactivated()
	return docs.NewLintContext(conf)
}

//------------------------------------------------------------------------------

// SetEngineVersion sets the version string representing the Bento engine that
// components will see. By default a best attempt will be made to determine a
// version either from the bento module import or a build-time flag.
func (s *StreamBuilder) SetEngineVersion(ev string) {
	s.engineVersion = ev
}

// SetSchema overrides the default config schema used when linting and parsing
// full configs with the SetYAML method. Other XYAML methods will not use this
// schema as they parse individual component configs rather than a larger
// configuration.
//
// This method is useful as a mechanism for modifying the default top-level
// settings, such as metrics types and so on.
func (s *StreamBuilder) SetSchema(schema *ConfigSchema) {
	if s.engineVersion == "" {
		s.engineVersion = schema.version
	}
	s.configSpec = schema.fields
}

// DisableLinting configures the stream builder to no longer lint YAML configs,
// allowing you to add snippets of config to the builder without failing on
// linting rules.
func (s *StreamBuilder) DisableLinting() {
	s.lintingDisabled = true
}

// SetEnvVarLookupFunc changes the behaviour of the stream builder so that the
// value of environment variable interpolations (of the form `${FOO}`) are
// obtained via a provided function rather than the default of os.LookupEnv.
//
// TODO V5: Add context here, Travis is onto us.
func (s *StreamBuilder) SetEnvVarLookupFunc(fn func(string) (string, bool)) {
	s.envVarLookupFn = fn
}

// SetThreads configures the number of pipeline processor threads should be
// configured. By default the number will be zero, which means the thread count
// will match the number of logical CPUs on the machine.
func (s *StreamBuilder) SetThreads(n int) {
	s.threads = n
}

// PrintLogger is a simple Print based interface implemented by custom loggers.
type PrintLogger interface {
	Printf(format string, v ...any)
	Println(v ...any)
}

// SetPrintLogger sets a custom logger supporting a simple Print based interface
// to be used by stream components. This custom logger will override any logging
// fields set via config.
func (s *StreamBuilder) SetPrintLogger(l PrintLogger) {
	s.customLogger = log.Wrap(l)
}

// SetLogger sets a customer logger via Go's standard logging interface,
// allowing you to replace the default Bento logger with your own.
func (s *StreamBuilder) SetLogger(l *slog.Logger) {
	s.customLogger = log.NewBentoLogAdapter(l)
}

// HTTPMultiplexer is an interface supported by most HTTP multiplexers.
type HTTPMultiplexer interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

type muxWrapper struct {
	m HTTPMultiplexer
}

func (w *muxWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	w.m.HandleFunc(path, h)
}

// SetHTTPMux sets an HTTP multiplexer to be used by stream components when
// registering endpoints instead of a new server spawned following the `http`
// fields of a Bento config.
func (s *StreamBuilder) SetHTTPMux(m HTTPMultiplexer) {
	s.apiMut = &muxWrapper{m}
}

//------------------------------------------------------------------------------

// AddProducerFunc adds an input to the builder that allows you to write
// messages directly into the stream with a closure function. If any other input
// has or will be added to the stream builder they will be automatically
// composed within a broker when the pipeline is built.
//
// The returned MessageHandlerFunc can be called concurrently from any number of
// goroutines, and each call will block until the message is successfully
// delivered downstream, was rejected (or otherwise could not be delivered) or
// the context is cancelled.
//
// Only one producer func can be added to a stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddProducerFunc() (MessageHandlerFunc, error) {
	if s.producerChan != nil {
		return nil, errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate a producer uuid: %w", err)
	}

	tChan := make(chan message.Transaction)
	s.producerChan = tChan
	s.producerID = uuid.String()

	conf := input.NewConfig()
	conf.Type = "inproc"
	conf.Plugin = s.producerID
	s.inputs = append(s.inputs, conf)

	return func(ctx context.Context, m *Message) error {
		tmpMsg := message.Batch{m.part}
		resChan := make(chan error)
		select {
		case tChan <- message.NewTransaction(tmpMsg, resChan):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case res := <-resChan:
			return res
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// AddBatchProducerFunc adds an input to the builder that allows you to write
// message batches directly into the stream with a closure function. If any
// other input has or will be added to the stream builder they will be
// automatically composed within a broker when the pipeline is built.
//
// The returned MessageBatchHandlerFunc can be called concurrently from any
// number of goroutines, and each call will block until all messages within the
// batch are successfully delivered downstream, were rejected (or otherwise
// could not be delivered) or the context is cancelled.
//
// Only one producer func can be added to a stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddBatchProducerFunc() (MessageBatchHandlerFunc, error) {
	if s.producerChan != nil {
		return nil, errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate a producer uuid: %w", err)
	}

	tChan := make(chan message.Transaction)
	s.producerChan = tChan
	s.producerID = uuid.String()

	conf := input.NewConfig()
	conf.Type = "inproc"
	conf.Plugin = s.producerID
	s.inputs = append(s.inputs, conf)

	return func(ctx context.Context, b MessageBatch) error {
		tmpMsg := make(message.Batch, len(b))
		for i, m := range b {
			tmpMsg[i] = m.part
		}
		resChan := make(chan error)
		select {
		case tChan <- message.NewTransaction(tmpMsg, resChan):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case res := <-resChan:
			return res
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// AddInputYAML parses an input YAML configuration and adds it to the builder.
// If more than one input configuration is added they will automatically be
// composed within a broker when the pipeline is built.
func (s *StreamBuilder) AddInputYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeInput); err != nil {
		return err
	}

	iconf, err := input.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.inputs = append(s.inputs, iconf)
	return nil
}

// AddProcessorYAML parses a processor YAML configuration and adds it to the
// builder to be executed within the pipeline.processors section, after all
// prior added processor configs.
func (s *StreamBuilder) AddProcessorYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeProcessor); err != nil {
		return err
	}

	pconf, err := processor.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.processors = append(s.processors, pconf)
	return nil
}

// AddConsumerFunc adds an output to the builder that executes a closure
// function argument for each message. If more than one output configuration is
// added they will automatically be composed within a fan out broker when the
// pipeline is built.
//
// The provided MessageHandlerFunc may be called from any number of goroutines,
// and therefore it is recommended to implement some form of throttling or mutex
// locking in cases where the call is non-blocking.
//
// Only one consumer can be added to a stream builder, and subsequent calls will
// return an error.
func (s *StreamBuilder) AddConsumerFunc(fn MessageHandlerFunc) error {
	if s.consumerFunc != nil {
		return errors.New("unable to add multiple consumer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate a consumer uuid: %w", err)
	}

	s.consumerFunc = func(c context.Context, mb MessageBatch) error {
		for _, m := range mb {
			if err := fn(c, m); err != nil {
				return err
			}
		}
		return nil
	}
	s.consumerID = uuid.String()

	conf := output.NewConfig()
	conf.Type = "inproc"
	conf.Plugin = s.consumerID
	s.outputs = append(s.outputs, conf)

	return nil
}

// AddBatchConsumerFunc adds an output to the builder that executes a closure
// function argument for each message batch. If more than one output
// configuration is added they will automatically be composed within a fan out
// broker when the pipeline is built.
//
// The provided MessageBatchHandlerFunc may be called from any number of
// goroutines, and therefore it is recommended to implement some form of
// throttling or mutex locking in cases where the call is non-blocking.
//
// Only one consumer can be added to a stream builder, and subsequent calls will
// return an error.
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise message batches received by this consumer will have a single
// message contents.
func (s *StreamBuilder) AddBatchConsumerFunc(fn MessageBatchHandlerFunc) error {
	if s.consumerFunc != nil {
		return errors.New("unable to add multiple consumer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate a consumer uuid: %w", err)
	}

	s.consumerFunc = fn
	s.consumerID = uuid.String()

	conf := output.NewConfig()
	conf.Type = "inproc"
	conf.Plugin = s.consumerID
	s.outputs = append(s.outputs, conf)

	return nil
}

// AddOutputYAML parses an output YAML configuration and adds it to the builder.
// If more than one output configuration is added they will automatically be
// composed within a fan out broker when the pipeline is built.
func (s *StreamBuilder) AddOutputYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeOutput); err != nil {
		return err
	}

	oconf, err := output.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.outputs = append(s.outputs, oconf)
	return nil
}

// AddCacheYAML parses a cache YAML configuration and adds it to the builder as
// a resource.
func (s *StreamBuilder) AddCacheYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeCache); err != nil {
		return err
	}

	cconf, err := cache.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}
	if cconf.Label == "" {
		return errors.New("a label must be specified for cache resources")
	}
	for _, cc := range s.resources.ResourceCaches {
		if cc.Label == cconf.Label {
			return fmt.Errorf("label %v collides with a previously defined resource", cc.Label)
		}
	}

	s.resources.ResourceCaches = append(s.resources.ResourceCaches, cconf)
	return nil
}

// AddRateLimitYAML parses a rate limit YAML configuration and adds it to the
// builder as a resource.
func (s *StreamBuilder) AddRateLimitYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeRateLimit); err != nil {
		return err
	}

	rconf, err := ratelimit.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}
	if rconf.Label == "" {
		return errors.New("a label must be specified for rate limit resources")
	}
	for _, rl := range s.resources.ResourceRateLimits {
		if rl.Label == rconf.Label {
			return fmt.Errorf("label %v collides with a previously defined resource", rl.Label)
		}
	}

	s.resources.ResourceRateLimits = append(s.resources.ResourceRateLimits, rconf)
	return nil
}

// AddResourcesYAML parses resource configurations and adds them to the config.
func (s *StreamBuilder) AddResourcesYAML(conf string) error {
	node, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	spec := manager.Spec()
	if err := s.lintYAMLSpec(spec, node); err != nil {
		return err
	}

	pConf, err := spec.ParsedConfigFromAny(node)
	if err != nil {
		return convertDocsLintErr(err)
	}

	rconf, err := manager.FromParsed(s.env.internal, pConf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	return s.resources.AddFrom(&rconf)
}

//------------------------------------------------------------------------------

// SetYAML parses a full Bento config and uses it to configure the builder. If
// any inputs, processors, outputs, resources, etc, have previously been added
// to the builder they will be overridden by this new config.
func (s *StreamBuilder) SetYAML(conf string) error {
	if s.producerChan != nil {
		return errors.New("attempted to override inputs config after adding a func producer")
	}
	if s.consumerFunc != nil {
		return errors.New("attempted to override outputs config after adding a func consumer")
	}

	node, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	spec := s.configSpec
	if err := s.lintYAMLSpec(spec, node); err != nil {
		return err
	}

	pConf, err := spec.ParsedConfigFromAny(node)
	if err != nil {
		return convertDocsLintErr(err)
	}

	sconf, err := config.FromParsed(s.env.internal, pConf, nil)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.setFromConfig(sconf)
	return nil
}

// SetFields modifies the config by setting one or more fields identified by a
// dot path to a value. The argument must be a variadic list of pairs, where the
// first element is a string containing the target field dot path, and the
// second element is a typed value to set the field to.
func (s *StreamBuilder) SetFields(pathValues ...any) error {
	if s.producerChan != nil {
		return errors.New("attempted to override config after adding a func producer")
	}
	if s.consumerFunc != nil {
		return errors.New("attempted to override config after adding a func consumer")
	}
	if len(pathValues)%2 != 0 {
		return errors.New("invalid odd number of pathValues provided")
	}

	var rootNode yaml.Node
	if err := rootNode.Encode(s.buildConfig()); err != nil {
		return err
	}

	sanitConf := docs.NewSanitiseConfig(s.env.internal)
	sanitConf.RemoveTypeField = true
	sanitConf.RemoveDeprecated = false
	sanitConf.DocsProvider = s.env.internal

	if err := s.configSpec.SanitiseYAML(&rootNode, sanitConf); err != nil {
		return err
	}

	for i := 0; i < len(pathValues)-1; i += 2 {
		var valueNode yaml.Node
		if err := valueNode.Encode(pathValues[i+1]); err != nil {
			return err
		}
		pathString, ok := pathValues[i].(string)
		if !ok {
			return fmt.Errorf("variadic pair element %v should be a string, got a %T", i, pathValues[i])
		}
		if err := s.configSpec.SetYAMLPath(s.env.internal, &rootNode, &valueNode, gabs.DotPathToSlice(pathString)...); err != nil {
			return err
		}
	}

	spec := s.configSpec
	if err := s.lintYAMLSpec(spec, &rootNode); err != nil {
		return err
	}

	pConf, err := spec.ParsedConfigFromAny(&rootNode)
	if err != nil {
		return err
	}

	sconf, err := config.FromParsed(s.env.internal, pConf, nil)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.setFromConfig(sconf)
	return nil
}

func (s *StreamBuilder) setFromConfig(sconf config.Type) {
	s.http = sconf.HTTP
	s.inputs = []input.Config{sconf.Input}
	s.buffer = sconf.Buffer
	s.processors = sconf.Pipeline.Processors
	s.threads = sconf.Pipeline.Threads
	s.outputs = []output.Config{sconf.Output}
	s.resources = sconf.ResourceConfig
	s.logger = sconf.Logger
	s.metrics = sconf.Metrics
	s.tracer = sconf.Tracer
	s.errHandler = sconf.ErrorHandling
}

// SetBufferYAML parses a buffer YAML configuration and sets it to the builder
// to be placed between the input and the pipeline (processors) sections. This
// config will replace any prior configured buffer.
func (s *StreamBuilder) SetBufferYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeBuffer); err != nil {
		return err
	}

	bconf, err := buffer.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.buffer = bconf
	return nil
}

// SetMetricsYAML parses a metrics YAML configuration and adds it to the builder
// such that all stream components emit metrics through it.
func (s *StreamBuilder) SetMetricsYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeMetrics); err != nil {
		return err
	}

	mconf, err := metrics.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.metrics = mconf
	return nil
}

// SetTracerYAML parses a tracer YAML configuration and adds it to the builder
// such that all stream components emit tracing spans through it.
func (s *StreamBuilder) SetTracerYAML(conf string) error {
	nconf, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeTracer); err != nil {
		return err
	}

	tconf, err := tracer.FromAny(s.env.internal, nconf)
	if err != nil {
		return convertDocsLintErr(err)
	}

	s.tracer = tconf
	return nil
}

// SetLoggerYAML parses a logger YAML configuration and adds it to the builder
// such that all stream components emit logs through it.
func (s *StreamBuilder) SetLoggerYAML(conf string) error {
	node, err := s.getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	spec := log.Spec()
	if err := s.lintYAMLSpec(spec, node); err != nil {
		return err
	}

	pConf, err := spec.ParsedConfigFromAny(node)
	if err != nil {
		return convertDocsLintErr(err)
	}

	lconf, err := log.FromParsed(pConf)
	if err != nil {
		return err
	}

	s.logger = lconf
	return nil
}

//------------------------------------------------------------------------------

// AsYAML prints a YAML representation of the stream config as it has been
// currently built.
func (s *StreamBuilder) AsYAML() (string, error) {
	conf := s.buildConfig()

	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return "", err
	}

	sanitConf := docs.NewSanitiseConfig(s.env.internal)
	sanitConf.RemoveTypeField = true
	sanitConf.RemoveDeprecated = false
	sanitConf.DocsProvider = s.env.internal

	if err := s.configSpec.SanitiseYAML(&node, sanitConf); err != nil {
		return "", err
	}

	b, err := yaml.Marshal(node)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// WalkedComponent is a struct containing information about a component yielded
// via the WalkComponents method.
type WalkedComponent struct {
	ComponentType string
	Name          string
	Label         string
	confYAML      string
}

// ConfigYAML returns the configuration of a walked component in YAML form.
func (w *WalkedComponent) ConfigYAML() string {
	return w.confYAML
}

// WalkComponents walks the Bento configuration as it is currently built and
// for each component type (input, processor, output, etc) calls a provided
// function with a struct containing information about the component.
//
// This can be useful for taking an inventory of the contents of a config.
func (s *StreamBuilder) WalkComponents(fn func(w *WalkedComponent) error) error {
	conf := s.buildConfig()

	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return err
	}

	sanitConf := docs.NewSanitiseConfig(s.env.internal)
	sanitConf.RemoveTypeField = true
	sanitConf.RemoveDeprecated = false
	sanitConf.DocsProvider = s.env.internal

	spec := s.configSpec
	if err := spec.SanitiseYAML(&node, sanitConf); err != nil {
		return err
	}

	return spec.WalkYAML(&node, s.env.internal,
		func(c docs.WalkedYAMLComponent) error {
			yamlBytes, err := yaml.Marshal(c.Conf)
			if err != nil {
				return err
			}
			return fn(&WalkedComponent{
				ComponentType: string(c.ComponentType),
				Name:          c.Name,
				Label:         c.Label,
				confYAML:      string(yamlBytes),
			})
		})
}

//------------------------------------------------------------------------------

func (s *StreamBuilder) runConsumerFunc(mgr *manager.Type) error {
	if s.consumerFunc == nil {
		return nil
	}
	tChan, err := mgr.GetPipe(s.consumerID)
	if err != nil {
		return err
	}
	go func() {
		for {
			tran, open := <-tChan
			if !open {
				return
			}
			batch := make(MessageBatch, tran.Payload.Len())
			_ = tran.Payload.Iter(func(i int, part *message.Part) error {
				batch[i] = NewInternalMessage(part)
				return nil
			})
			err := s.consumerFunc(context.Background(), batch)
			_ = tran.Ack(context.Background(), err)
		}
	}()
	return nil
}

// Build a Bento stream pipeline according to the components specified by this
// stream builder.
func (s *StreamBuilder) Build() (*Stream, error) {
	return s.buildWithEnv(s.env.internal, false)
}

// BuildTraced creates a Bento stream pipeline according to the components
// specified by this stream builder, where each major component (input,
// processor, output) is wrapped with a tracing module that, during the lifetime
// of the stream, aggregates tracing events into the returned *TracingSummary.
// Once the stream has ended the TracingSummary can be queried for events that
// occurred.
//
// Experimental: The behaviour of this method could change outside of major
// version releases.
func (s *StreamBuilder) BuildTraced() (*Stream, *TracingSummary, error) {
	tenv, summary := tracing.TracedBundle(s.env.internal)
	strm, err := s.buildWithEnv(tenv, false)
	return strm, &TracingSummary{summary}, err
}

// BuildStrict creates a Bento stream pipeline according to the components
// specified by this stream builder, where each processor components is wrapped
// to cause any message-level error to fail an entire batch. These failed batches
// are nacked and/or reprocessed depending on your input.
//
// Experimental: The behaviour of this method could change outside of major
// version releases.
func (s *StreamBuilder) BuildStrict() (*Stream, error) {
	senv := strict.StrictBundle(s.env.internal)
	strm, err := s.buildWithEnv(senv, true, strict.OptSetStrictModeFromManager()...)
	return strm, err
}

func (s *StreamBuilder) buildWithEnv(env *bundle.Environment, isStrictBuild bool, opts ...manager.OptFunc) (*Stream, error) {
	conf := s.buildConfig()

	logger := s.customLogger
	if logger == nil {
		var err error
		if logger, err = log.New(os.Stdout, s.env.fs, s.logger); err != nil {
			return nil, err
		}
	}

	engVer := s.engineVersion
	if engVer == "" {
		engVer = cli.Version
	}

	defaultOpts := []manager.OptFunc{
		manager.OptSetEngineVersion(engVer),
		manager.OptSetLogger(logger),
		manager.OptSetEnvironment(env),
		manager.OptSetBloblangEnvironment(s.env.getBloblangParserEnv()),
		manager.OptSetFS(s.env.fs),
	}

	managerOpts := append(defaultOpts, opts...)

	// This temporary manager is a very lazy way of instantiating a manager that
	// restricts the bloblang and component environments to custom plugins.
	// Ideally we would break out the constructor for our general purpose
	// manager to allow for a two-tier initialisation where we can defer
	// resource constructors until after this metrics exporter is initialised.
	tmpMgr, err := manager.New(
		manager.NewResourceConfig(),
		defaultOpts...,
	)
	if err != nil {
		return nil, err
	}

	tracer, err := env.TracersInit(s.tracer, tmpMgr)
	if err != nil {
		return nil, err
	}

	stats, err := env.MetricsInit(s.metrics, tmpMgr)
	if err != nil {
		return nil, err
	}

	apiMut := s.apiMut
	var apiType *api.Type
	if apiMut == nil {
		var sanitNode yaml.Node
		err := sanitNode.Encode(conf)
		if err == nil {
			sanitConf := docs.NewSanitiseConfig(s.env.internal)
			sanitConf.RemoveTypeField = true
			sanitConf.ScrubSecrets = true
			sanitConf.DocsProvider = env
			_ = s.configSpec.SanitiseYAML(&sanitNode, sanitConf)
		}
		if apiType, err = api.New("", "", s.http, sanitNode, logger, stats); err != nil {
			return nil, fmt.Errorf("unable to create stream HTTP server due to: %w. Tip: you can disable the server with `http.enabled` set to `false`, or override the configured server with SetHTTPMux", err)
		}
		apiMut = apiType
	} else if hler := stats.HandlerFunc(); hler != nil {
		apiMut.RegisterEndpoint("/stats", "Exposes service-wide metrics in the format configured.", hler)
		apiMut.RegisterEndpoint("/metrics", "Exposes service-wide metrics in the format configured.", hler)
	}

	managerOpts = append(managerOpts,
		manager.OptSetAPIReg(apiMut),
		manager.OptSetMetrics(stats),
		manager.OptSetTracer(tracer),
	)

	// Let's read the strategy from the config, but ONLY IF we are not already in a strict build mode
	// (because in a strict build, we don't want to override the strategy from the config).
	if !isStrictBuild {
		if s.errHandler.Strategy == "reject" {
			managerOpts = append(managerOpts, strict.OptSetStrictModeFromManager()...)
		} else if s.errHandler.Strategy == "retry" {
			managerOpts = append(managerOpts, manager.OptSetPipelineCtor(strict.NewRetryFeedbackPipelineCtor()))
			managerOpts = append(managerOpts, strict.OptSetRetryModeFromManager()...)
		}
	}

	mgr, err := manager.New(
		conf.ResourceConfig,
		managerOpts...,
	)
	if err != nil {
		return nil, err
	}

	if s.producerChan != nil {
		mgr.SetPipe(s.producerID, s.producerChan)
	}

	return newStream(conf.Config, apiType, mgr, stats, tracer, logger, func() {
		if err := s.runConsumerFunc(mgr); err != nil {
			logger.Error("Failed to run func consumer: %v", err)
		}
	}), nil
}

type builderConfig struct {
	HTTP                   *api.Config `yaml:"http,omitempty"`
	stream.Config          `yaml:",inline"`
	manager.ResourceConfig `yaml:",inline"`
	Metrics                metrics.Config `yaml:"metrics"`
	Logger                 *log.Config    `yaml:"logger,omitempty"`
	Tracer                 tracer.Config  `yaml:"tracer"`
}

func (s *StreamBuilder) buildConfig() builderConfig {
	conf := builderConfig{}

	if s.apiMut == nil {
		conf.HTTP = &s.http
	}

	if len(s.inputs) == 1 {
		conf.Input = s.inputs[0]
	} else if len(s.inputs) > 1 {
		conf.Input.Type = "broker"
		iSlice := make([]any, len(s.inputs))
		for i, v := range s.inputs {
			iSlice[i] = v
		}
		conf.Input.Plugin = map[string]any{
			"inputs": iSlice,
		}
	} else {
		// TODO: V5 Prevent default input/output
		conf.Input = input.NewConfig()
	}

	conf.Buffer = s.buffer

	conf.Pipeline.Threads = s.threads
	conf.Pipeline.Processors = s.processors

	if len(s.outputs) == 1 {
		conf.Output = s.outputs[0]
	} else if len(s.outputs) > 1 {
		conf.Output.Type = "broker"
		iSlice := make([]any, len(s.outputs))
		for i, v := range s.outputs {
			iSlice[i] = v
		}
		conf.Output.Plugin = map[string]any{
			"outputs": iSlice,
		}
	} else {
		// TODO: V5 Prevent default input/output
		conf.Output = output.NewConfig()
	}

	conf.ResourceConfig = s.resources
	conf.Metrics = s.metrics
	conf.Tracer = s.tracer
	if s.customLogger == nil {
		conf.Logger = &s.logger
	}
	return conf
}

//------------------------------------------------------------------------------

func (s *StreamBuilder) getYAMLNode(b []byte) (*yaml.Node, error) {
	var err error
	if b, err = config.ReplaceEnvVariables(b, s.envVarLookupFn); err != nil {
		// TODO: Allow users to specify whether they care about env variables
		// missing, in which case we error or not based on that.
		var errEnvMissing *config.ErrMissingEnvVars
		if errors.As(err, &errEnvMissing) {
			b = errEnvMissing.BestAttempt
		} else {
			return nil, err
		}
	}
	return docs.UnmarshalYAML(b)
}

func (s *StreamBuilder) lintYAMLSpec(spec docs.FieldSpecs, node *yaml.Node) error {
	if s.lintingDisabled {
		return nil
	}
	return lintsToErr(spec.LintYAML(s.getLintContext(), node))
}

func (s *StreamBuilder) lintYAMLComponent(node *yaml.Node, ctype docs.Type) error {
	if s.lintingDisabled {
		return nil
	}
	return lintsToErr(docs.LintYAML(s.getLintContext(), ctype, node))
}
