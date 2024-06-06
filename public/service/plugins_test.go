package service_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/component/testutil"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
	"github.com/warpstreamlabs/bento/v1/internal/manager"
	"github.com/warpstreamlabs/bento/v1/public/service"
)

func testSanitConf() docs.SanitiseConfig {
	sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
	sanitConf.RemoveTypeField = true
	sanitConf.RemoveDeprecated = true
	return sanitConf
}

func TestCachePluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().Field(
		service.NewIntField("a").Default(100),
	)

	var aValue int
	var errValue error
	var initLabel string

	require.NoError(t, service.RegisterCache("test_cache_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			aValue, errValue = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	cacheConfStr := `label: foo
test_cache_plugin_with_config:
    a: 20
`

	cacheConf, err := testutil.CacheFromYAML(cacheConfStr)
	require.NoError(t, err)

	var cacheNode yaml.Node
	require.NoError(t, cacheNode.Encode(cacheConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeCache, &cacheNode, testSanitConf()))

	cacheConfOutBytes, err := yaml.Marshal(cacheNode)
	require.NoError(t, err)
	assert.Equal(t, cacheConfStr, string(cacheConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewCache(cacheConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.NoError(t, errValue)
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestCachePluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterCache("test_cache_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	cacheConfStr := `label: foo
test_cache_plugin_without_config: null # No default (required)
`

	cacheConf, err := testutil.CacheFromYAML(cacheConfStr)
	require.NoError(t, err)

	var cacheNode yaml.Node
	require.NoError(t, cacheNode.Encode(cacheConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeCache, &cacheNode, testSanitConf()))

	cacheConfOutBytes, err := yaml.Marshal(cacheNode)
	require.NoError(t, err)
	assert.Equal(t, cacheConfStr, string(cacheConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewCache(cacheConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestInputPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().Field(
		service.NewIntField("a").Default(100),
	)

	var aValue int
	var errValue error
	var initLabel string

	require.NoError(t, service.RegisterInput("test_input_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			aValue, errValue = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_input_plugin_with_config:
    a: 20
`

	inConf, err := testutil.InputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeInput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewInput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.NoError(t, errValue)
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestInputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterInput("test_input_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_input_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.InputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeInput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewInput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestOutputPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().
		Field(service.NewIntField("a").Default(100))

	var aValue int
	var errValue error
	var initLabel string

	require.NoError(t, service.RegisterOutput("test_output_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			aValue, errValue = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_output_plugin_with_config:
    a: 20
`

	inConf, err := testutil.OutputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.NoError(t, errValue)
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestOutputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterOutput("test_output_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_output_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.OutputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestBatchOutputPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().
		Field(service.NewIntField("a").Default(100)).
		Field(service.NewIntField("count").Default(10))

	var aValue, countValue int
	var initLabel string

	require.NoError(t, service.RegisterBatchOutput("test_batch_output_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			aValue, _ = conf.FieldInt("a")
			countValue, _ = conf.FieldInt("count")
			initLabel = mgr.Label()
			batchPolicy := service.BatchPolicy{}
			return nil, batchPolicy, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_output_plugin_with_config:
    a: 20
    count: 21
`

	inConf, err := testutil.OutputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, 20, aValue)
	assert.Equal(t, 21, countValue)
	assert.Equal(t, "foo", initLabel)
}

func TestBatchOutputPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterOutput("test_batch_output_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			initLabel = mgr.Label()
			return nil, 1, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_output_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.OutputFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeOutput, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewOutput(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestProcessorPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().
		Field(service.NewIntField("a").Default(100))

	var aValue int
	var initLabel string

	require.NoError(t, service.RegisterProcessor("test_processor_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			aValue, _ = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_processor_plugin_with_config:
    a: 20
`

	inConf, err := testutil.ProcessorFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestProcessorPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterProcessor("test_processor_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_processor_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.ProcessorFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestBatchProcessorPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().
		Field(service.NewIntField("a").Default(100))

	var aValue int
	var initLabel string

	require.NoError(t, service.RegisterBatchProcessor("test_batch_processor_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			aValue, _ = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_processor_plugin_with_config:
    a: 20
`

	inConf, err := testutil.ProcessorFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestBatchProcessorPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterBatchProcessor("test_batch_processor_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_batch_processor_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.ProcessorFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeProcessor, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewProcessor(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}

func TestRateLimitPluginWithConfig(t *testing.T) {
	configSpec := service.NewConfigSpec().
		Field(service.NewIntField("a").Default(100))

	var aValue int
	var initLabel string

	require.NoError(t, service.RegisterRateLimit("test_rate_limit_plugin_with_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			aValue, _ = conf.FieldInt("a")
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_rate_limit_plugin_with_config:
    a: 20
`

	inConf, err := testutil.RateLimitFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeRateLimit, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewRateLimit(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, 20, aValue)
	assert.Equal(t, "foo", initLabel)
}

func TestRateLimitPluginWithoutConfig(t *testing.T) {
	configSpec := service.NewConfigSpec()

	var initLabel string
	require.NoError(t, service.RegisterRateLimit("test_rate_limit_plugin_without_config", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			initLabel = mgr.Label()
			return nil, errors.New("this is a test error")
		}))

	inConfStr := `label: foo
test_rate_limit_plugin_without_config: null # No default (required)
`

	inConf, err := testutil.RateLimitFromYAML(inConfStr)
	require.NoError(t, err)

	var outNode yaml.Node
	require.NoError(t, outNode.Encode(inConf))

	require.NoError(t, docs.SanitiseYAML(docs.TypeRateLimit, &outNode, testSanitConf()))

	outConfOutBytes, err := yaml.Marshal(outNode)
	require.NoError(t, err)
	assert.Equal(t, inConfStr, string(outConfOutBytes))

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	_, err = mgr.NewRateLimit(inConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "this is a test error")
	assert.Equal(t, "foo", initLabel)
}
