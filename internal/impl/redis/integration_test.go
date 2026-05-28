package redis

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationRedis(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return client.Ping(context.Background()).Err()
	}))

	// STREAMS
	t.Run("streams", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_streams:
    url: tcp://localhost:$PORT
    stream: ${! meta("routing_stream_prefix") }-stream-$ID
    body_key: body
    max_length: 0
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
    batching:
      count: $OUTPUT_BATCH_COUNT
  processors:
    - bloblang: meta routing_stream_prefix = "bar"

input:
  redis_streams:
    url: tcp://localhost:$PORT
    body_key: body
    streams: [ bar-stream-$ID ]
    limit: 10
    client_id: client-input-$ID
    consumer_group: group-$ID
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestMetadata(),
			integration.StreamTestMetadataFilter(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(1000),
			integration.StreamTestStreamParallel(1000),
			integration.StreamTestStreamParallelLossy(1000),
			integration.StreamTestStreamParallelLossyThroughReconnect(100),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	t.Run("pubsub", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channel: channel-$ID
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channels: [ channel-$ID ]
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(100),
			integration.StreamTestStreamParallel(100),
			integration.StreamTestStreamParallelLossy(100),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	t.Run("list", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
`
		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatches(20, 100, 1),
			integration.StreamTestStreamSequential(1000),
			integration.StreamTestStreamParallel(1000),
			integration.StreamTestStreamParallelLossy(1000),
			integration.StreamTestSendBatchCount(10),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
		t.Run("with max in flight", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
				integration.StreamTestOptMaxInFlight(10),
			)
		})
	})

	// SCAN
	t.Run("scan", func(t *testing.T) {
		t.Parallel()
		template := `
input:
  redis_scan:
    url: 'tcp://localhost:$PORT'
    match: 'foo-*'
  processors:
    - mapping: 'root = this.value'

output:
  cache:
    target: rcache
    key: 'foo-${! counter() }'

cache_resources:
  - label: rcache
    redis:
      url: 'tcp://localhost:$PORT'
`
		suite := integration.StreamTests(
			integration.StreamTestStreamIsolated(1000),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	t.Run("scan value formats", func(t *testing.T) {
		ctx := context.Background()

		newInput := func(t *testing.T, confStr string) service.Input {
			t.Helper()

			conf, err := redisScanInputConfig().ParseYAML(confStr, nil)
			require.NoError(t, err)

			input, err := newRedisScanInputFromConfig(conf, service.MockResources())
			require.NoError(t, err)
			require.NoError(t, input.Connect(ctx))
			t.Cleanup(func() {
				assert.NoError(t, input.Close(ctx))
			})

			return input
		}

		readStructured := func(t *testing.T, input service.Input) map[string]map[string]any {
			t.Helper()

			actual := map[string]map[string]any{}
			for {
				msg, ackFn, err := input.Read(ctx)
				if errors.Is(err, service.ErrEndOfInput) {
					break
				}
				require.NoError(t, err)
				require.NoError(t, ackFn(ctx, nil))

				structured, err := msg.AsStructured()
				require.NoError(t, err)
				obj, ok := structured.(map[string]any)
				require.True(t, ok)

				id := fmt.Sprintf("%v", obj["key"])
				if field, exists := obj["field"]; exists {
					id += "|" + fmt.Sprintf("%v", field)
				}
				actual[id] = obj
			}
			return actual
		}

		readRaw := func(t *testing.T, input service.Input) map[string][]byte {
			t.Helper()

			actual := map[string][]byte{}
			for {
				msg, ackFn, err := input.Read(ctx)
				if errors.Is(err, service.ErrEndOfInput) {
					break
				}
				require.NoError(t, err)
				require.NoError(t, ackFn(ctx, nil))

				key, exists := msg.MetaGet(redisScanMetaKey)
				require.True(t, exists)
				id := key
				if field, exists := msg.MetaGet(redisScanMetaHashField); exists {
					id += "|" + field
				}
				payload, err := msg.AsBytes()
				require.NoError(t, err)
				actual[id] = payload
			}
			return actual
		}

		stringStructuredPrefix := "bento_test_redis_scan_string_structured"
		require.NoError(t, client.Del(ctx, stringStructuredPrefix+":foo", stringStructuredPrefix+":bar").Err())
		require.NoError(t, client.Set(ctx, stringStructuredPrefix+":foo", "payload-a", 0).Err())
		require.NoError(t, client.Set(ctx, stringStructuredPrefix+":bar", "payload-b", 0).Err())
		stringStructuredInput := newInput(t, fmt.Sprintf(`
url: tcp://localhost:%v
match: %s:*
scan_count: 1
`, resource.GetPort("6379/tcp"), stringStructuredPrefix))
		assert.Equal(t, map[string]map[string]any{
			stringStructuredPrefix + ":foo": {
				"key":   stringStructuredPrefix + ":foo",
				"value": "payload-a",
			},
			stringStructuredPrefix + ":bar": {
				"key":   stringStructuredPrefix + ":bar",
				"value": "payload-b",
			},
		}, readStructured(t, stringStructuredInput))

		stringRawPrefix := "bento_test_redis_scan_string_raw"
		require.NoError(t, client.Del(ctx, stringRawPrefix+":foo", stringRawPrefix+":bar").Err())
		require.NoError(t, client.Set(ctx, stringRawPrefix+":foo", []byte{0x00, 0x01, 0x02}, 0).Err())
		require.NoError(t, client.Set(ctx, stringRawPrefix+":bar", []byte("payload-b"), 0).Err())
		stringRawInput := newInput(t, fmt.Sprintf(`
url: tcp://localhost:%v
match: %s:*
data_type: string
value_format: raw
scan_count: 1
`, resource.GetPort("6379/tcp"), stringRawPrefix))
		assert.Equal(t, map[string][]byte{
			stringRawPrefix + ":foo": {0x00, 0x01, 0x02},
			stringRawPrefix + ":bar": []byte("payload-b"),
		}, readRaw(t, stringRawInput))

		hashStructuredPrefix := "bento_test_redis_scan_hash_structured"
		require.NoError(t, client.Del(ctx, hashStructuredPrefix+":foo", hashStructuredPrefix+":bar").Err())
		require.NoError(t, client.HSet(ctx, hashStructuredPrefix+":foo", "field_a", "payload-a", "field_b", "payload-b").Err())
		require.NoError(t, client.HSet(ctx, hashStructuredPrefix+":bar", "field_c", "payload-c").Err())
		hashStructuredInput := newInput(t, fmt.Sprintf(`
url: tcp://localhost:%v
match: %s:*
data_type: hash
value_format: structured
scan_count: 1
hash_scan_count: 1
`, resource.GetPort("6379/tcp"), hashStructuredPrefix))
		assert.Equal(t, map[string]map[string]any{
			hashStructuredPrefix + ":foo|field_a": {
				"key":   hashStructuredPrefix + ":foo",
				"field": "field_a",
				"value": "payload-a",
			},
			hashStructuredPrefix + ":foo|field_b": {
				"key":   hashStructuredPrefix + ":foo",
				"field": "field_b",
				"value": "payload-b",
			},
			hashStructuredPrefix + ":bar|field_c": {
				"key":   hashStructuredPrefix + ":bar",
				"field": "field_c",
				"value": "payload-c",
			},
		}, readStructured(t, hashStructuredInput))

		hashRawPrefix := "bento_test_redis_scan_hash_raw"
		require.NoError(t, client.Del(ctx, hashRawPrefix+":foo", hashRawPrefix+":bar", "bento_test_redis_scan_hash_raw_ignore").Err())
		require.NoError(t, client.HSet(ctx, hashRawPrefix+":foo", "field_a", []byte{0x00, 0x01, 0x02}, "field_b", []byte("payload-b")).Err())
		require.NoError(t, client.HSet(ctx, hashRawPrefix+":bar", "field_c", []byte{0xff, 0x00, 0x10}).Err())
		require.NoError(t, client.HSet(ctx, "bento_test_redis_scan_hash_raw_ignore", "field_z", []byte("ignored")).Err())
		hashRawInput := newInput(t, fmt.Sprintf(`
url: tcp://localhost:%v
match: %s:*
data_type: hash
value_format: raw
scan_count: 1
hash_scan_count: 1
`, resource.GetPort("6379/tcp"), hashRawPrefix))
		assert.Equal(t, map[string][]byte{
			hashRawPrefix + ":foo|field_a": {0x00, 0x01, 0x02},
			hashRawPrefix + ":foo|field_b": []byte("payload-b"),
			hashRawPrefix + ":bar|field_c": {0xff, 0x00, 0x10},
		}, readRaw(t, hashRawInput))
	})

	// HASH
	t.Run("hash", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  redis_hash:
    url: tcp://localhost:$PORT
    key: $ID-${! json("id") }
    fields:
      content: ${! content() }
`
		hashGetFn := func(ctx context.Context, testID, id string) (string, []string, error) {
			client := redis.NewClient(&redis.Options{
				Addr:    fmt.Sprintf("localhost:%v", resource.GetPort("6379/tcp")),
				Network: "tcp",
			})
			key := testID + "-" + id
			res, err := client.HGet(ctx, key, "content").Result()
			if err != nil {
				return "", nil, err
			}
			return res, nil, nil
		}
		suite := integration.StreamTests(
			integration.StreamTestOutputOnlySendSequential(10, hashGetFn),
			integration.StreamTestOutputOnlySendBatch(10, hashGetFn),
			integration.StreamTestOutputOnlyOverride(hashGetFn),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})
}

func BenchmarkIntegrationRedis(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		b.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	_ = resource.Expire(900)
	require.NoError(b, pool.Retry(func() error {
		return client.Ping(context.Background()).Err()
	}))

	// STREAMS
	b.Run("streams", func(b *testing.B) {
		template := `
output:
  redis_streams:
    url: tcp://localhost:$PORT
    stream: stream-$ID
    body_key: body
    max_length: 0
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  redis_streams:
    url: tcp://localhost:$PORT
    body_key: body
    streams: [ stream-$ID ]
    limit: 10
    client_id: client-input-$ID
    consumer_group: group-$ID
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	b.Run("pubsub", func(b *testing.B) {
		template := `
output:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channel: channel-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  redis_pubsub:
    url: tcp://localhost:$PORT
    channels: [ channel-$ID ]
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})

	b.Run("list", func(b *testing.B) {
		template := `
output:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  redis_list:
    url: tcp://localhost:$PORT
    key: key-$ID
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			integration.StreamBenchWrite(20),
			integration.StreamBenchWrite(10),
			integration.StreamBenchWrite(1),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6379/tcp")),
		)
	})
}
