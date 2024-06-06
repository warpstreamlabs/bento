package integration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
)

// CacheTestConfigVars exposes some variables injected into template configs for
// cache unit tests.
type CacheTestConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	ID string

	// A Port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	Port string

	// General variables.
	General map[string]string
}

// CachePreTestFn is an optional closure to be called before tests are run, this
// is an opportunity to mutate test config variables and mess with the
// environment.
type CachePreTestFn func(t testing.TB, ctx context.Context, vars *CacheTestConfigVars)

type cacheTestEnvironment struct {
	configTemplate string
	configVars     CacheTestConfigVars

	preTest CachePreTestFn

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   *metrics.Namespaced
}

func newCacheTestEnvironment(t *testing.T, confTemplate string) cacheTestEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return cacheTestEnvironment{
		configTemplate: confTemplate,
		configVars: CacheTestConfigVars{
			ID:      u4.String(),
			General: map[string]string{},
		},
		timeout: time.Second * 90,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e cacheTestEnvironment) RenderConfig() string {
	vars := []string{
		"$ID", e.configVars.ID,
		"$PORT", e.configVars.Port,
	}
	for k, v := range e.configVars.General {
		vars = append(vars, "$"+k, v)
	}
	return strings.NewReplacer(vars...).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

// CacheTestOptFunc is an opt func for customizing the behaviour of cache tests,
// these are useful for things that are integration environment specific, such
// as the port of the service being interacted with.
type CacheTestOptFunc func(*cacheTestEnvironment)

// CacheTestOptTimeout describes an optional timeout spanning the entirety of
// the test suite.
func CacheTestOptTimeout(timeout time.Duration) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.timeout = timeout
	}
}

// CacheTestOptLogging allows components to log with the given log level. This
// is useful for diagnosing issues.
func CacheTestOptLogging(level string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		var err error
		env.log, err = log.New(os.Stdout, ifs.OS(), logConf)
		if err != nil {
			panic(err)
		}
	}
}

// CacheTestOptPort defines the port of the integration service.
func CacheTestOptPort(port string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.configVars.Port = port
	}
}

// CacheTestOptVarSet sets an arbitrary variable for the test that can be
// injected into templated configs.
func CacheTestOptVarSet(key, value string) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.configVars.General[key] = value
	}
}

// CacheTestOptPreTest adds a closure to be executed before each test.
func CacheTestOptPreTest(fn CachePreTestFn) CacheTestOptFunc {
	return func(env *cacheTestEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type cacheTestDefinitionFn func(*testing.T, *cacheTestEnvironment)

// CacheTestDefinition encompasses a unit test to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type CacheTestDefinition struct {
	fn func(*testing.T, *cacheTestEnvironment)
}

// CacheTestList is a list of cache test definitions that can be run with a
// single template and function args.
type CacheTestList []CacheTestDefinition

// CacheTests creates a list of tests from variadic arguments.
func CacheTests(tests ...CacheTestDefinition) CacheTestList {
	return tests
}

// Run all the tests against a config template. Tests are run in parallel.
func (i CacheTestList) Run(t *testing.T, configTemplate string, opts ...CacheTestOptFunc) {
	for _, test := range i {
		env := newCacheTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		var done func()
		env.ctx, done = context.WithTimeout(env.ctx, env.timeout)
		t.Cleanup(done)

		test.fn(t, &env)
	}
}

//------------------------------------------------------------------------------

func namedCacheTest(name string, test cacheTestDefinitionFn) CacheTestDefinition {
	return CacheTestDefinition{
		fn: func(t *testing.T, env *cacheTestEnvironment) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				if env.preTest != nil {
					env.preTest(t, env.ctx, &env.configVars)
				}
				test(t, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

func initCache(t *testing.T, env *cacheTestEnvironment) cache.V1 {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(env.RenderConfig()))
	require.NoError(t, err)

	spec := manager.Spec()
	lints := spec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	assert.Empty(t, lints)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	conf, err := manager.FromParsed(bundle.GlobalEnvironment, pConf)
	require.NoError(t, err)

	manager, err := manager.New(conf, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
	require.NoError(t, err)

	var c cache.V1
	require.NoError(t, manager.AccessCache(env.ctx, "testcache", func(v cache.V1) {
		c = v
	}))
	return c
}

func closeCache(t *testing.T, cache cache.V1) {
	require.NoError(t, cache.Close(context.Background()))
}
