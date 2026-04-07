package bundle

import (
	"fmt"
	"sort"

	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/tracer"
	"github.com/warpstreamlabs/bento/internal/docs"
)

// AllTracers is a set containing every single tracer that has been imported.
var AllTracers = &TracerSet{
	specs: map[string]tracerSpec{},
}

//------------------------------------------------------------------------------

// TracersAdd adds a new tracers exporter to this environment by providing a
// constructor and documentation.
func (e *Environment) TracersAdd(constructor TracerConstructor, spec docs.ComponentSpec) error {
	return e.tracers.Add(constructor, spec)
}

// TracersInit attempts to initialise a tracers exporter from a config.
func (e *Environment) TracersInit(conf tracer.Config, nm NewManagement) (trace.TracerProvider, error) {
	return e.tracers.Init(conf, nm)
}

// TracersDocs returns a slice of tracers exporter specs.
func (e *Environment) TracersDocs() []docs.ComponentSpec {
	return e.tracers.Docs()
}

//------------------------------------------------------------------------------

// TracerConstructor constructs an tracer component.
type TracerConstructor func(tracer.Config, NewManagement) (trace.TracerProvider, error)

type tracerSpec struct {
	constructor TracerConstructor
	spec        docs.ComponentSpec
}

// TracerEnvFunc is a function that attempts to create a TracerProvider from
// environment variables. It returns (nil, nil) when no relevant env vars are
// set.
type TracerEnvFunc func() (trace.TracerProvider, error)

// TracerSet contains an explicit set of tracers available to a Bento service.
type TracerSet struct {
	specs  map[string]tracerSpec
	envFns []TracerEnvFunc
}

// Add a new tracer to this set by providing a spec (name, documentation, and
// constructor).
func (s *TracerSet) Add(constructor TracerConstructor, spec docs.ComponentSpec) error {
	if !nameRegexp.MatchString(spec.Name) {
		return fmt.Errorf("component name '%v' does not match the required regular expression /%v/", spec.Name, nameRegexpRaw)
	}
	if s.specs == nil {
		s.specs = map[string]tracerSpec{}
	}
	spec.Type = docs.TypeTracer
	s.specs[spec.Name] = tracerSpec{
		constructor: constructor,
		spec:        spec,
	}
	return nil
}

// RegisterEnvFn registers a function that can create a TracerProvider from
// environment variables. Called as a fallback when no tracer is explicitly
// configured.
func (s *TracerSet) RegisterEnvFn(fn TracerEnvFunc) {
	s.envFns = append(s.envFns, fn)
}

// Init attempts to initialise a tracer from a config. When the default noop
// tracer is selected, it tries registered env functions (e.g. OTel env vars)
// first.
func (s *TracerSet) Init(conf tracer.Config, nm NewManagement) (trace.TracerProvider, error) {
	if conf.Type == "none" {
		for _, fn := range s.envFns {
			tp, err := fn()
			if err != nil {
				return nil, fmt.Errorf("auto-configuring tracer from env: %w", err)
			}
			if tp != nil {
				return tp, nil
			}
		}
	}

	spec, exists := s.specs[conf.Type]
	if !exists {
		return nil, component.ErrInvalidType("tracer", conf.Type)
	}
	return spec.constructor(conf, nm)
}

// Docs returns a slice of tracer specs, which document each method.
func (s *TracerSet) Docs() []docs.ComponentSpec {
	var docs []docs.ComponentSpec
	for _, v := range s.specs {
		docs = append(docs, v.spec)
	}
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Name < docs[j].Name
	})
	return docs
}

// DocsFor returns the documentation for a given component name, returns a
// boolean indicating whether the component name exists.
func (s *TracerSet) DocsFor(name string) (docs.ComponentSpec, bool) {
	c, ok := s.specs[name]
	if !ok {
		return docs.ComponentSpec{}, false
	}
	return c.spec, true
}
