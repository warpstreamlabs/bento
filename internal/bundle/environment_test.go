package bundle_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/buffer"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/component/scanner"
	"github.com/warpstreamlabs/bento/internal/component/tracer"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

func TestEnvironmentIndividualWith(t *testing.T) {
	env := bundle.NewEnvironment()

	errTest := errors.New("exists")

	for _, k := range []string{"foo", "bar", "baz"} {
		require.NoError(t, env.BufferAdd(func(c buffer.Config, nm bundle.NewManagement) (buffer.Streamed, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "buffer"}))

		require.NoError(t, env.CacheAdd(func(c cache.Config, nm bundle.NewManagement) (cache.V1, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "cache"}))

		require.NoError(t, env.InputAdd(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "input"}))

		require.NoError(t, env.OutputAdd(func(c output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "output"}))

		require.NoError(t, env.ProcessorAdd(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "processor"}))

		require.NoError(t, env.RateLimitAdd(func(c ratelimit.Config, nm bundle.NewManagement) (ratelimit.V1, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "ratelimit"}))

		require.NoError(t, env.MetricsAdd(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "metric"}))

		require.NoError(t, env.TracersAdd(func(c tracer.Config, nm bundle.NewManagement) (trace.TracerProvider, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "tracer"}))

		require.NoError(t, env.ScannerAdd(func(c scanner.Config, nm bundle.NewManagement) (scanner.Creator, error) {
			return nil, errTest
		}, docs.ComponentSpec{Name: k + "scanner"}))
	}

	env = env.
		WithBuffers("barbuffer").
		WithCaches("barcache").
		WithInputs("barinput").
		WithOutputs("baroutput").
		WithProcessors("barprocessor").
		WithRateLimits("barratelimit").
		WithMetrics("barmetric").
		WithTracers("bartracer").
		WithScanners("barscanner")

	for _, k := range []string{"bar"} {
		bconf := buffer.NewConfig()
		bconf.Type = k + "buffer"
		_, err := env.BufferInit(bconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		cconf := cache.NewConfig()
		cconf.Type = k + "cache"
		_, err = env.CacheInit(cconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		iconf := input.NewConfig()
		iconf.Type = k + "input"
		_, err = env.InputInit(iconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		oconf := output.NewConfig()
		oconf.Type = k + "output"
		_, err = env.OutputInit(oconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		pconf := processor.NewConfig()
		pconf.Type = k + "processor"
		_, err = env.ProcessorInit(pconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		rconf := ratelimit.NewConfig()
		rconf.Type = k + "ratelimit"
		_, err = env.RateLimitInit(rconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		mconf := metrics.NewConfig()
		mconf.Type = k + "metric"
		_, err = env.MetricsInit(mconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		tconf := tracer.NewConfig()
		tconf.Type = k + "tracer"
		_, err = env.TracersInit(tconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)

		sconf := scanner.Config{}
		sconf.Type = k + "scanner"
		_, err = env.ScannerInit(sconf, mock.NewManager())
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	}

	for _, k := range []string{"foo", "baz"} {
		bconf := buffer.NewConfig()
		bconf.Type = k + "buffer"
		_, err := env.BufferInit(bconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		cconf := cache.NewConfig()
		cconf.Type = k + "cache"
		_, err = env.CacheInit(cconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		iconf := input.NewConfig()
		iconf.Type = k + "input"
		_, err = env.InputInit(iconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		oconf := output.NewConfig()
		oconf.Type = k + "output"
		_, err = env.OutputInit(oconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		pconf := processor.NewConfig()
		pconf.Type = k + "processor"
		_, err = env.ProcessorInit(pconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		rconf := ratelimit.NewConfig()
		rconf.Type = k + "ratelimit"
		_, err = env.RateLimitInit(rconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		mconf := metrics.NewConfig()
		mconf.Type = k + "metric"
		_, err = env.MetricsInit(mconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		tconf := tracer.NewConfig()
		tconf.Type = k + "tracer"
		_, err = env.TracersInit(tconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")

		sconf := scanner.Config{}
		sconf.Type = k + "scanner"
		_, err = env.ScannerInit(sconf, mock.NewManager())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "was not recognised")
	}
}
