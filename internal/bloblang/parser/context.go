package parser

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// Context contains context used throughout a Bloblang parser for
// accessing function and method constructors.
type Context struct {
	Functions    *query.FunctionSet
	Methods      *query.MethodSet
	namedContext *namedContext
	importer     Importer

	// cache holds parsers built for a given Context identity (Functions,
	// Methods and namedContext chain) so that recursive/repeated calls to
	// queryParser don't rebuild the same combinator tree on every
	// invocation. It is shared across Context values derived from one
	// another via WithNamedContext, since those calls don't change
	// Functions/Methods. Contexts that swap Functions/Methods (e.g.
	// Deactivated) get a fresh cache to avoid stale/incorrect reuse.
	//
	// Safety note: this cache is only sound because every parser closure
	// built by this package treats its captured Context as immutable after
	// construction (no closure reassigns a captured Context variable at
	// call time). See lambdaExpressionParser for the one historical
	// exception, fixed to use a local variable instead.
	cache *parserCache
}

// cacheKeyT is a comparable identity for a Context, derived from the fields
// that actually influence how a parser is constructed: the Functions/
// Methods set identities (compared by pointer value, not formatted into a
// string) and the namedContext chain flattened into a string. Using a
// struct instead of a formatted string avoids the cost of fmt.Fprintf's
// reflection-based pointer formatting on every cache lookup, which shows up
// as measurable overhead even on cache hits.
type cacheKeyT struct {
	functions unsafe.Pointer
	methods   unsafe.Pointer
	namedChain string
}

// parserCache is a concurrency-safe store of built parsers keyed by a
// Context's identity. It's intentionally untyped (map[cacheKeyT]any) so it
// can hold parsers of different result types (e.g. Func[query.Function])
// without requiring a cache per type.
type parserCache struct {
	mu    sync.Mutex
	byKey map[cacheKeyT]any
}

func newParserCache() *parserCache {
	return &parserCache{byKey: make(map[cacheKeyT]any)}
}

// cacheKey returns a comparable identity for the Context derived from the
// fields that actually influence how a parser is constructed: the
// Functions/Methods set identities (compared by pointer) and the
// namedContext chain. The importer is deliberately excluded since it only
// affects how `import` statements read files, not how query expressions are
// parsed.
func (pCtx Context) cacheKey() cacheKeyT {
	key := cacheKeyT{
		functions: unsafe.Pointer(pCtx.Functions),
		methods:   unsafe.Pointer(pCtx.Methods),
	}
	if pCtx.namedContext != nil {
		var sb strings.Builder
		for nc := pCtx.namedContext; nc != nil; nc = nc.next {
			sb.WriteByte('|')
			sb.WriteString(nc.name)
		}
		key.namedChain = sb.String()
	}
	return key
}

// cachedParser looks up a previously built parser for the given cache key.
func cachedParser[T any](pCtx Context, key cacheKeyT) (Func[T], bool) {
	if pCtx.cache == nil {
		return nil, false
	}
	pCtx.cache.mu.Lock()
	defer pCtx.cache.mu.Unlock()
	v, ok := pCtx.cache.byKey[key]
	if !ok {
		return nil, false
	}
	fn, ok := v.(Func[T])
	return fn, ok
}

// storeParser stores a built parser under the given cache key for later
// reuse. If the Context has no cache (e.g. a zero-value Context constructed
// directly rather than via EmptyContext/GlobalContext) this is a no-op.
func storeParser[T any](pCtx Context, key cacheKeyT, fn Func[T]) {
	if pCtx.cache == nil {
		return
	}
	pCtx.cache.mu.Lock()
	defer pCtx.cache.mu.Unlock()
	pCtx.cache.byKey[key] = fn
}

// EmptyContext returns a parser context with no functions, methods or import
// capabilities.
func EmptyContext() Context {
	return Context{
		Functions: query.NewFunctionSet(),
		Methods:   query.NewMethodSet(),
		importer:  newOSImporter(),
		cache:     newParserCache(),
	}
}

// GlobalContext returns a parser context with globally defined functions and
// methods.
func GlobalContext() Context {
	return Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
		importer:  newOSImporter(),
		cache:     newParserCache(),
	}
}

type namedContext struct {
	name string
	next *namedContext
}

// WithNamedContext returns a Context with a named execution context.
func (pCtx Context) WithNamedContext(name string) Context {
	next := pCtx.namedContext
	pCtx.namedContext = &namedContext{name: name, next: next}
	return pCtx
}

// HasNamedContext returns true if a given name exists as a named context.
func (pCtx Context) HasNamedContext(name string) bool {
	tmp := pCtx.namedContext
	for tmp != nil {
		if tmp.name == name {
			return true
		}
		tmp = tmp.next
	}
	return false
}

// InitFunction attempts to initialise a function from the available
// constructors of the parser context.
func (pCtx Context) InitFunction(name string, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Functions.Init(name, args)
}

// InitMethod attempts to initialise a method from the available constructors of
// the parser context.
func (pCtx Context) InitMethod(name string, target query.Function, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Methods.Init(name, target, args)
}

// WithImporter returns a Context where imports are made from the provided
// Importer implementation.
func (pCtx Context) WithImporter(importer Importer) Context {
	pCtx.importer = importer
	return pCtx
}

// WithImporterRelativeToFile returns a Context where any relative imports will
// be made from the directory of the provided file path. The provided path can
// itself be relative (to the current importer directory) or absolute.
func (pCtx Context) WithImporterRelativeToFile(pathStr string) Context {
	pCtx.importer = pCtx.importer.RelativeToFile(pathStr)
	return pCtx
}

// Deactivated returns a version of the parser context where all functions and
// methods exist but can no longer be instantiated. This means it's possible to
// parse and validate mappings but not execute them. If the context also has an
// importer then it will also be replaced with an implementation that always
// returns empty files.
func (pCtx Context) Deactivated() Context {
	nextCtx := pCtx
	nextCtx.Functions = pCtx.Functions.Deactivated()
	nextCtx.Methods = pCtx.Methods.Deactivated()
	// Functions/Methods identities changed, so any parser cached under the
	// old identity is not valid for this context (and vice versa). Use a
	// fresh cache to avoid cross-contamination between active/deactivated
	// contexts.
	nextCtx.cache = newParserCache()
	return nextCtx
}

// CustomImporter returns a version of the parser context where file imports are
// done exclusively through a provided closure function, which takes an import
// path (relative or absolute).
func (pCtx Context) CustomImporter(fn func(name string) ([]byte, error)) Context {
	nextCtx := pCtx
	nextCtx.importer = newCustomImporter(fn)
	return nextCtx
}

// DisabledImports returns a version of the parser context where file imports
// are entirely disabled. Any import statement within parsed mappings will
// return parse errors explaining that file imports are disabled.
func (pCtx Context) DisabledImports() Context {
	nextCtx := pCtx
	nextCtx.importer = disabledImporter{}
	return nextCtx
}

// ImportFile attempts to read a file for import via the customised Importer.
func (pCtx Context) ImportFile(name string) ([]byte, error) {
	return pCtx.importer.Import(name)
}

//------------------------------------------------------------------------------

// Importer represents a repository of bloblang files that can be imported by
// mappings. It's possible for mappings to import files using relative paths, if
// the import is from a mapping which was itself imported then the path should
// be interpretted as relative to that file.
type Importer interface {
	// Import a file from a relative or absolute path.
	Import(pathStr string) ([]byte, error)

	// Derive a new importer where relative import paths are resolved from the
	// directory of the provided file path. The provided path could be absolute,
	// or relative itself in which case it should be resolved from the
	// pre-existing relative directory.
	RelativeToFile(filePath string) Importer
}

//------------------------------------------------------------------------------

type osImporter struct {
	relativePath string
}

func newOSImporter() Importer {
	pwd, _ := os.Getwd()
	return &osImporter{
		relativePath: pwd,
	}
}

func (i *osImporter) Import(pathStr string) ([]byte, error) {
	if !filepath.IsAbs(pathStr) {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	f, err := os.Open(pathStr)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

func (i *osImporter) RelativeToFile(filePath string) Importer {
	dir := filepath.Dir(filePath)
	if dir == "" || dir == "." {
		return i
	}

	pathStr := filepath.Dir(filePath)
	if !filepath.IsAbs(pathStr) && i.relativePath != "" {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	newI := *i
	newI.relativePath = pathStr
	return &newI
}

//------------------------------------------------------------------------------

type customImporter struct {
	relativePath string
	readFn       func(name string) ([]byte, error)
}

func newCustomImporter(readFn func(name string) ([]byte, error)) Importer {
	return &customImporter{
		relativePath: ".",
		readFn:       readFn,
	}
}

func (i *customImporter) Import(pathStr string) ([]byte, error) {
	if !filepath.IsAbs(pathStr) {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	return i.readFn(pathStr)
}

func (i *customImporter) RelativeToFile(filePath string) Importer {
	dir := filepath.Dir(filePath)
	if dir == "" || dir == "." {
		return i
	}

	pathStr := filepath.Dir(filePath)
	if !filepath.IsAbs(pathStr) && i.relativePath != "" {
		pathStr = filepath.Join(i.relativePath, pathStr)
	}

	newI := *i
	newI.relativePath = pathStr
	return &newI
}

//------------------------------------------------------------------------------

type disabledImporter struct{}

func (d disabledImporter) Import(pathStr string) ([]byte, error) {
	return nil, errors.New("imports are disabled in this context")
}

func (d disabledImporter) RelativeToFile(filePath string) Importer {
	return d
}
