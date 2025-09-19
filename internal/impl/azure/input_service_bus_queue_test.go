package azure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestServiceBusQueueConfig(t *testing.T) {
	// Test configuration parsing
	conf := service.NewConfigSpec().
		Field(service.NewStringField("connection_string").Default("")).
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewStringField("queue").Default("test-queue")).
		Field(service.NewInputMaxInFlightField().Default(10)).
		Field(service.NewBoolField("auto_ack").Default(false)).
		Field(service.NewStringListField("nack_reject_patterns").Default([]any{}))

	parsed, err := conf.ParseYAML(`
connection_string: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
queue: "my-queue"
max_in_flight: 5
auto_ack: true
nack_reject_patterns: ["^reject.*"]
`, nil)
	require.NoError(t, err)

	config, err := sbqConfigFromParsed(parsed)
	require.NoError(t, err)

	assert.Equal(t, "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test", config.connectionString)
	assert.Equal(t, "my-queue", config.queueName)
	assert.Equal(t, 5, config.maxInFlight)
	assert.True(t, config.autoAck)
	require.Len(t, config.nackRejectPatterns, 1)
	assert.True(t, config.nackRejectPatterns[0].MatchString("reject this"))
}

func TestServiceBusQueueConfigFromNamespace(t *testing.T) {
	// Test configuration with namespace instead of connection string
	conf := service.NewConfigSpec().
		Field(service.NewStringField("connection_string").Default("")).
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewStringField("queue").Default("test-queue")).
		Field(service.NewInputMaxInFlightField().Default(10)).
		Field(service.NewBoolField("auto_ack").Default(false)).
		Field(service.NewStringListField("nack_reject_patterns").Default([]any{}))

	parsed, err := conf.ParseYAML(`
namespace: "test.servicebus.windows.net"
queue: "my-queue"
`, nil)
	require.NoError(t, err)

	config, err := sbqConfigFromParsed(parsed)
	require.NoError(t, err)

	assert.Empty(t, config.connectionString)
	assert.Equal(t, "test.servicebus.windows.net", config.namespace)
	assert.Equal(t, "my-queue", config.queueName)
}

func TestServiceBusQueueConfigValidation(t *testing.T) {
	// Test that configuration requires either connection_string or namespace
	conf := service.NewConfigSpec().
		Field(service.NewStringField("connection_string").Default("")).
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewStringField("queue").Default("test-queue")).
		Field(service.NewInputMaxInFlightField().Default(10)).
		Field(service.NewBoolField("auto_ack").Default(false)).
		Field(service.NewStringListField("nack_reject_patterns").Default([]any{}))

	parsed, err := conf.ParseYAML(`
queue: "my-queue"
`, nil)
	require.NoError(t, err)

	config, err := sbqConfigFromParsed(parsed)
	require.NoError(t, err)

	// Create reader with empty connection info
	reader, err := newAzureServiceBusQueueReader(config, service.MockResources())
	require.NoError(t, err)

	// Connect should fail when neither connection_string nor namespace are provided
	ctx := context.Background()
	err = reader.Connect(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "either connection_string or namespace must be provided")
}

func TestServiceBusQueueNackRejectPatterns(t *testing.T) {
	// Test nack reject pattern compilation
	conf := service.NewConfigSpec().
		Field(service.NewStringField("connection_string").Default("")).
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewStringField("queue").Default("test-queue")).
		Field(service.NewInputMaxInFlightField().Default(10)).
		Field(service.NewBoolField("auto_ack").Default(false)).
		Field(service.NewStringListField("nack_reject_patterns").Default([]any{}))

	parsed, err := conf.ParseYAML(`
connection_string: "test"
queue: "test-queue"
nack_reject_patterns: ["^reject.*", ".*error$"]
`, nil)
	require.NoError(t, err)

	config, err := sbqConfigFromParsed(parsed)
	require.NoError(t, err)

	require.Len(t, config.nackRejectPatterns, 2)

	// Test first pattern
	assert.True(t, config.nackRejectPatterns[0].MatchString("reject this message"))
	assert.False(t, config.nackRejectPatterns[0].MatchString("do not reject"))

	// Test second pattern
	assert.True(t, config.nackRejectPatterns[1].MatchString("some error"))
	assert.False(t, config.nackRejectPatterns[1].MatchString("no problem"))
}

func TestServiceBusQueueInvalidRegexPattern(t *testing.T) {
	// Test invalid regex pattern
	conf := service.NewConfigSpec().
		Field(service.NewStringField("connection_string").Default("")).
		Field(service.NewStringField("namespace").Default("")).
		Field(service.NewStringField("queue").Default("test-queue")).
		Field(service.NewInputMaxInFlightField().Default(10)).
		Field(service.NewBoolField("auto_ack").Default(false)).
		Field(service.NewStringListField("nack_reject_patterns").Default([]any{}))

	parsed, err := conf.ParseYAML(`
connection_string: "test"
queue: "test-queue"
nack_reject_patterns: ["[invalid"]
`, nil)
	require.NoError(t, err)

	_, err = sbqConfigFromParsed(parsed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to compile nack reject pattern")
}

func TestServiceBusQueueSpec(t *testing.T) {
	// Test that the spec can be created without errors
	spec := sbqSpec()
	require.NotNil(t, spec)

	// Test that all expected fields are present
	_, err := spec.ParseYAML(`
connection_string: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
queue: "test-queue"
max_in_flight: 10
auto_ack: false
nack_reject_patterns: []
`, nil)
	require.NoError(t, err)
}

func TestServiceBusQueueReader(t *testing.T) {
	// Test basic reader creation
	conf := &sbqConfig{
		connectionString: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
		queueName:        "test-queue",
		maxInFlight:      10,
		autoAck:          false,
	}

	reader, err := newAzureServiceBusQueueReader(conf, service.MockResources())
	require.NoError(t, err)
	require.NotNil(t, reader)

	assert.Equal(t, conf, reader.conf)
	assert.NotNil(t, reader.log)
	assert.NotNil(t, reader.messagesChan)
	assert.NotNil(t, reader.ackMessagesChan)
	assert.NotNil(t, reader.nackMessagesChan)
	assert.NotNil(t, reader.closeSignal)
}

func TestServiceBusQueueClose(t *testing.T) {
	// Test close functionality
	conf := &sbqConfig{
		connectionString: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
		queueName:        "test-queue",
		maxInFlight:      10,
		autoAck:          false,
	}

	reader, err := newAzureServiceBusQueueReader(conf, service.MockResources())
	require.NoError(t, err)

	// Test disconnect when nothing is connected
	err = reader.disconnect()
	require.NoError(t, err)
}

func TestServiceBusQueueReadNotConnected(t *testing.T) {
	// Test reading when not connected
	conf := &sbqConfig{
		connectionString: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
		queueName:        "test-queue",
		maxInFlight:      10,
		autoAck:          false,
	}

	reader, err := newAzureServiceBusQueueReader(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	// Should return ErrNotConnected when receiver is nil
	msg, ackFn, err := reader.Read(ctx)
	assert.Nil(t, msg)
	assert.Nil(t, ackFn)
	assert.Equal(t, service.ErrNotConnected, err)
}

func TestServiceBusQueueInit(t *testing.T) {
	// Test that the input is properly registered
	// This test ensures the init() function works correctly

	// We can't easily test init() directly, but we can test that
	// the registration doesn't panic and creates the right input type
	spec := sbqSpec()
	require.NotNil(t, spec)

	// Test configuration parsing through the registered spec
	parsed, err := spec.ParseYAML(`
connection_string: "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test"
queue: "test-queue"
`, nil)
	require.NoError(t, err)
	require.NotNil(t, parsed)
}
