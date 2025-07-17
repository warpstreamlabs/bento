package service

import (
	"context"
	"io/fs"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

// Resources provides access to service-wide resources.
type Resources struct {
	mgr bundle.NewManagement
}

func newResourcesFromManager(nm bundle.NewManagement) *Resources {
	return &Resources{mgr: nm}
}

// MockResources returns an instantiation of a resources struct that provides
// valid but ineffective methods and observability components. It is possible to
// instantiate this with mocked (in-memory) cache and rate limit types for
// testing purposed.
func MockResources(opts ...MockResourcesOptFn) *Resources {
	m := mock.NewManager()
	for _, o := range opts {
		o(m)
	}
	return newResourcesFromManager(m)
}

// MockResourcesOptFn provides a func based optional argument to MockResources.
type MockResourcesOptFn func(*mock.Manager)

// MockResourcesOptUseLogger sets the logger to be used by components
// referencing these resources.
func MockResourcesOptUseLogger(l *Logger) MockResourcesOptFn {
	return func(m *mock.Manager) {
		if l != nil {
			m.L = l.m
		}
	}
}

// MockResourcesOptUseSlogger sets the logger by converting the provided
// slog.Logger to comply with Bento's log.Modular interface
func MockResourcesOptUseSlogger(l *slog.Logger) MockResourcesOptFn {
	return func(m *mock.Manager) {
		if l != nil {
			m.L = log.NewBentoLogAdapter(l)
		}
	}
}

// MockResourcesOptAddCache instantiates the resources type with a mock cache
// with a given name. Cached items are held in memory.
func MockResourcesOptAddCache(name string) MockResourcesOptFn {
	return func(m *mock.Manager) {
		m.Caches[name] = map[string]mock.CacheItem{}
	}
}

// MockResourcesOptAddRateLimit instantiates the resources type with a mock rate
// limit with a given name, the provided closure will be called for each
// invocation of the rate limit.
func MockResourcesOptAddRateLimit(name string, fn func(context.Context) (time.Duration, error)) MockResourcesOptFn {
	return func(m *mock.Manager) {
		m.RateLimits[name] = mock.RateLimit(fn)
	}
}

// EngineVersion returns the version stamp associated with the underlying
// bento engine. The version string is not guaranteed to match any particular
// scheme.
func (r *Resources) EngineVersion() string {
	return r.mgr.EngineVersion()
}

// Label returns a label that identifies the component instantiation. This could
// be an explicit label set in config, or is otherwise a generated label based
// on the position of the component within a config.
func (r *Resources) Label() string {
	return r.mgr.Label()
}

// Logger returns a logger preset with context about the component the resources
// were provided to.
func (r *Resources) Logger() *Logger {
	return newReverseAirGapLogger(r.mgr.Logger())
}

// Metrics returns a mechanism for creating custom metrics.
func (r *Resources) Metrics() *Metrics {
	return newReverseAirGapMetrics(r.mgr.Metrics())
}

// OtelTracer returns an open telemetry tracer provider that can be used to
// create new tracers.
//
// Experimental: This type signature is experimental and therefore subject to
// change outside of major version releases.
func (r *Resources) OtelTracer() trace.TracerProvider {
	return r.mgr.Tracer()
}

// wrapperFS provides extra methods support around a bare fs.FS that does
// fully implement ifs.FS, this allows us to keep some clean interfaces while
// also ensuring backward compatibility.
type wrapperFS struct {
	fs       fs.FS
	fallback ifs.FS
}

// Open opens the named file for reading.
func (f *wrapperFS) Open(name string) (fs.File, error) {
	return f.fs.Open(name)
}

// OpenFile is the generalized open call.
func (f *wrapperFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return f.fallback.OpenFile(name, flag, perm)
}

// Stat returns a FileInfo describing the named file.
func (f *wrapperFS) Stat(name string) (fs.FileInfo, error) {
	return f.fallback.Stat(name)
}

// Remove removes the named file or (empty) directory.
func (f *wrapperFS) Remove(name string) error {
	return f.fallback.Remove(name)
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error.
func (f *wrapperFS) MkdirAll(path string, perm fs.FileMode) error {
	return f.fallback.MkdirAll(path, perm)
}

// FS implements a superset of fs.FS and includes goodies that bento
// components specifically need.
type FS struct {
	i ifs.FS
}

// NewFS provides a new instance of a filesystem. The fs.FS passed in can
// optionally implement methods from bento ifs.FS
func NewFS(filesystem fs.FS) *FS {
	if fsimpl, ok := filesystem.(ifs.FS); ok {
		return &FS{fsimpl}
	}
	return &FS{&wrapperFS{filesystem, ifs.OS()}}
}

// Open opens the named file for reading.
func (f *FS) Open(name string) (fs.File, error) {
	return f.i.Open(name)
}

// OpenFile is the generalized open call.
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return f.i.OpenFile(name, flag, perm)
}

// Stat returns a FileInfo describing the named file.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	return f.i.Stat(name)
}

// Remove removes the named file or (empty) directory.
func (f *FS) Remove(name string) error {
	return f.i.Remove(name)
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error.
func (f *FS) MkdirAll(path string, perm fs.FileMode) error {
	return f.i.MkdirAll(path, perm)
}

// FS returns an fs.FS implementation that provides isolation or customised
// behaviour for components that access the filesystem. For example, this might
// be used to tally files being accessed by components for observability
// purposes, or to customise where relative paths are resolved from.
//
// Components should use this instead of accessing the os directly. However, the
// default behaviour of an environment FS is to access the OS from the directory
// the process is running from, which matches calling the os package directly.
func (r *Resources) FS() *FS {
	if r == nil || r.mgr == nil {
		return &FS{i: ifs.OS()}
	}
	return &FS{i: r.mgr.FS()}
}

// AccessCache attempts to access a cache resource by name. This action can
// block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessCache(ctx context.Context, name string, fn func(c Cache)) error {
	return r.mgr.AccessCache(ctx, name, func(c cache.V1) {
		fn(newReverseAirGapCache(c))
	})
}

// HasCache confirms whether a cache with a given name has been registered as a
// resource. This method is useful during component initialisation as it is
// defensive against ordering.
func (r *Resources) HasCache(name string) bool {
	return r.mgr.ProbeCache(name)
}

// AccessInput attempts to access a input resource by name.
func (r *Resources) AccessInput(ctx context.Context, name string, fn func(i *ResourceInput)) error {
	return r.mgr.AccessInput(ctx, name, func(in input.Streamed) {
		fn(newResourceInput(in))
	})
}

// HasInput confirms whether an input with a given name has been registered as a
// resource. This method is useful during component initialisation as it is
// defensive against ordering.
func (r *Resources) HasInput(name string) bool {
	return r.mgr.ProbeInput(name)
}

// AccessOutput attempts to access an output resource by name. This action can
// block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessOutput(ctx context.Context, name string, fn func(o *ResourceOutput)) error {
	return r.mgr.AccessOutput(ctx, name, func(o output.Sync) {
		fn(newResourceOutput(o))
	})
}

// HasOutput confirms whether an output with a given name has been registered as
// a resource. This method is useful during component initialisation as it is
// defensive against ordering.
func (r *Resources) HasOutput(name string) bool {
	return r.mgr.ProbeOutput(name)
}

// AccessRateLimit attempts to access a rate limit resource by name. This action
// can block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessRateLimit(ctx context.Context, name string, fn func(r RateLimit)) error {
	return r.mgr.AccessRateLimit(ctx, name, func(r ratelimit.V1) {
		// TODO: This MessageAwareRateLimit shoud eventually replace V1
		// Try to upgrade to message aware rate-limiter if possible.
		if mar, ok := r.(ratelimit.MessageAwareRateLimit); ok {
			fn(newReverseAirGapMessageAwareRateLimit(mar))
		} else {
			fn(newReverseAirGapRateLimit(r))
		}
	})
}

// HasRateLimit confirms whether a rate limit with a given name has been
// registered as a resource. This method is useful during component
// initialisation as it is defensive against ordering.
func (r *Resources) HasRateLimit(name string) bool {
	return r.mgr.ProbeRateLimit(name)
}

// GetGeneric queries the resources for a generic key value, potentially set by
// another plugin or instantiation of this plugin.
func (r *Resources) GetGeneric(key any) (any, bool) {
	return r.mgr.GetGeneric(key)
}

// GetOrSetGeneric attempts to obtain an existing generic value for a given key
// if present. Otherwise, it stores and returns the provided value. The loaded
// result is true if the value was loaded, false if stored.
func (r *Resources) GetOrSetGeneric(key, value any) (actual any, loaded bool) {
	return r.mgr.GetOrSetGeneric(key, value)
}

// SetGeneric sets a generic key/value pair, which can be accessed by other
// plugin implementations with access to the same resources.
//
// The provided key must be comparable and should not be of type string or any
// other built-in type to avoid collisions between packages using resources.
// Users of SetGeneric should define their own types for keys. To avoid
// allocating when assigning to an any type, keys often have concrete type
// struct{}.
func (r *Resources) SetGeneric(key, value any) {
	r.mgr.SetGeneric(key, value)
}

//------------------------------------------------------------------------------

type resourcesUnwrapper struct {
	mgr bundle.NewManagement
}

func (r resourcesUnwrapper) Unwrap() bundle.NewManagement {
	return r.mgr
}

// XUnwrapper is for internal use only, do not use this.
func (r *Resources) XUnwrapper() any {
	return resourcesUnwrapper{mgr: r.mgr}
}

//------------------------------------------------------------------------------

// ManagedBatchOutput takes a BatchOutput implementation and wraps it within a
// mechanism that automatically manages QOL details such as connect/reconnect
// looping, max in flight, back pressure, and so on. This is similar to how an
// output would be executed within a standard Bento pipeline.
func (r *Resources) ManagedBatchOutput(typeName string, maxInFlight int, b BatchOutput) (*OwnedOutput, error) {
	w := newAirGapBatchWriter(b)
	o, err := output.NewAsyncWriter(typeName, maxInFlight, w, r.mgr)
	if err != nil {
		return nil, err
	}
	return newOwnedOutput(o)
}
