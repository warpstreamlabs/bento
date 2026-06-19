package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationNatsKV(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js", "--trace"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var natsConn *nats.Conn
	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		return err
	}))
	t.Cleanup(func() {
		natsConn.Close()
	})

	template := `
output:
  label: kv_output
  nats_kv:
    urls: [ tcp://localhost:$PORT ]
    bucket: bucket-$ID
    # We need to make this key random as the NATS server will only deliver the
    # latest revision of a key when it's requested by a watcher, this is by
    # design, but if we want to test bento semantics like batching we should
    # use unique keys for every message passing through the output
    key: ${! ksuid() }

input:
  label: kv_input
  nats_kv:
    urls: [ tcp://localhost:$PORT ]
    bucket: bucket-$ID
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), // NATS KV doesn't support metadata
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			js, err := jetstream.New(natsConn)
			require.NoError(t, err)

			bucketName := "bucket-" + vars.ID

			_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
				Bucket: bucketName,
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)

	t.Run("cache", func(t *testing.T) {
		template := `
cache_resources:
  - label: testcache
    nats_kv:
      bucket: bucket-$ID
      urls: [ tcp://localhost:$PORT ]`
		suite := integration.CacheTests(
			integration.CacheTestOpenClose(),
			integration.CacheTestMissingKey(),
			integration.CacheTestDoubleAdd(),
			integration.CacheTestDelete(),
			integration.CacheTestGetAndSet(50),
		)
		suite.Run(
			t, template,
			integration.CacheTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.CacheTestConfigVars) {
				js, err := jetstream.New(natsConn)
				require.NoError(t, err)

				bucketName := "bucket-" + vars.ID

				_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
					Bucket: bucketName,
				})
				require.NoError(t, err)
			}),
			integration.CacheTestOptPort(resource.GetPort("4222/tcp")),
		)
	})

	t.Run("processor", func(t *testing.T) {
		createBucket := func(t *testing.T) (jetstream.KeyValue, string) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			js, err := jetstream.New(natsConn)
			require.NoError(t, err)

			bucketName := "bucket-" + u4.String()

			bucket, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
				Bucket:  bucketName,
				History: 5,
			})
			require.NoError(t, err)

			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))

			return bucket, url
		}

		process := func(yaml string) (service.MessageBatch, error) {
			spec := natsKVProcessorConfig()
			parsed, err := spec.ParseYAML(yaml, nil)
			require.NoError(t, err)

			p, err := newKVProcessor(parsed, service.MockResources())
			require.NoError(t, err)

			m := service.NewMessage([]byte("hello"))
			m.MetaSetMut("inherited", "world")
			return p.Process(context.Background(), m)
		}

		t.Run("get operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: get
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("lawblog"), bytes)
			if v, ok := m.MetaGetMut("inherited"); ok {
				assert.Equal(t, "world", v)
			} else {
				t.Error("inherited metadata not found")
			}

		})

		t.Run("get_revision operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			revision, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: get_revision
        key: blob
        revision: %d
        urls: [%s]`, bucket.Bucket(), revision, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("lawblog"), bytes)
			if v, ok := m.MetaGetMut("inherited"); ok {
				assert.Equal(t, "world", v)
			} else {
				t.Error("inherited metadata not found")
			}

		})

		t.Run("create operation (success)", func(t *testing.T) {
			bucket, url := createBucket(t)
			yaml := fmt.Sprintf(`
        bucket: %s
        operation: create
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
			if v, ok := m.MetaGetMut("inherited"); ok {
				assert.Equal(t, "world", v)
			} else {
				t.Error("inherited metadata not found")
			}

		})

		t.Run("create operation (error)", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: create
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			_, err = process(yaml)
			require.Error(t, err)
		})

		t.Run("put operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			yaml := fmt.Sprintf(`
        bucket: %s
        operation: put
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
		})

		t.Run("update operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			revision, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: update
        key: blob
        revision: %d
        urls: [%s]`, bucket.Bucket(), revision, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)
		})

		t.Run("delete operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: delete
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)

			_, err = bucket.Get(context.Background(), "blob")
			require.Error(t, err)
		})

		t.Run("purge operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: purge
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)

			_, err = bucket.Get(context.Background(), "blob")
			require.Error(t, err)
		})

		t.Run("history operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)
			_, err = bucket.PutString(context.Background(), "blob", "sawedlog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: history
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			require.Len(t, result, 1)

			msg, err := result[0].AsStructured()
			require.NoError(t, err)
			require.IsType(t, []any{}, msg)
			records := msg.([]any)
			require.Len(t, records, 2)
			record := records[1]
			require.IsType(t, map[string]any{}, record)
			assert.Contains(t, record, "created")
			assert.Subset(t, record, map[string]any{
				"key":       "blob",
				"value":     []byte("sawedlog"),
				"bucket":    bucket.Bucket(),
				"revision":  uint64(2),
				"delta":     uint64(0),
				"operation": "KeyValuePutOp",
			})
		})

		t.Run("keys operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)
			_, err = bucket.PutString(context.Background(), "bobs", "sawedlog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
        bucket: %s
        operation: keys
        key: blob
        urls: [%s]`, bucket.Bucket(), url)

			result, err := process(yaml)
			require.NoError(t, err)

			require.Len(t, result, 1)

			msg, err := result[0].AsBytes()
			require.NoError(t, err)
			expected, err := json.Marshal([]any{"blob"})
			require.NoError(t, err)
			assert.JSONEq(t, string(expected), string(msg))
		})
	})
}

// TestIntegrationNatsKVCacheReconnect verifies that the DisconnectErrHandler
// registered on the NATS connection fires when the server drops the connection
// and that the cache transparently reconnects on the next operation.
func TestIntegrationNatsKVCacheReconnect(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 30 * time.Second

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(120)

	natsPort := resource.GetPort("4222/tcp")
	natsURL := fmt.Sprintf("tcp://localhost:%s", natsPort)
	bucketName := "reconnect-test"

	// Wait for NATS to be ready.
	var setupConn *nats.Conn
	require.NoError(t, pool.Retry(func() error {
		setupConn, err = nats.Connect(natsURL)
		return err
	}))
	defer setupConn.Close()

	createBucket := func(t *testing.T, nc *nats.Conn) {
		t.Helper()
		js, err := jetstream.New(nc)
		require.NoError(t, err)
		_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: bucketName})
		require.NoError(t, err)
	}
	createBucket(t, setupConn)

	// Build the cache under test.
	spec := natsKVCacheConfig()
	yamlConf := fmt.Sprintf("urls: [%s]\nbucket: %s", natsURL, bucketName)
	conf, err := spec.ParseYAML(yamlConf, nil)
	require.NoError(t, err)

	cache, err := newKVCache(conf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close(context.Background()) })

	// Verify normal operation before disconnect.
	require.NoError(t, cache.Set(context.Background(), "k1", []byte("v1"), nil))
	val, err := cache.Get(context.Background(), "k1")
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	// Stop the container — this drops the TCP connection.
	require.NoError(t, pool.Client.StopContainer(resource.Container.ID, 5))

	// The DisconnectErrHandler should clear natsConn promptly (TCP RST/FIN
	// from the server means no need to wait for a ping timeout).
	require.Eventually(t, func() bool {
		cache.connMut.RLock()
		defer cache.connMut.RUnlock()
		return cache.natsConn == nil
	}, 10*time.Second, 50*time.Millisecond,
		"DisconnectErrHandler should have cleared natsConn after server stop")

	// Restart the container — Docker reuses the original port bindings when
	// no HostConfig is specified.
	require.NoError(t, pool.Client.StartContainer(resource.Container.ID, nil))

	// Wait for NATS to accept connections again, then recreate the KV bucket
	// (JetStream state is not persisted across restarts by default).
	// Use a longer deadline here — container restart can take longer than the
	// initial startup.
	pool.MaxWait = 60 * time.Second
	var recoveryConn *nats.Conn
	require.NoError(t, pool.Retry(func() error {
		recoveryConn, err = nats.Connect(natsURL)
		return err
	}))
	defer recoveryConn.Close()
	createBucket(t, recoveryConn)

	// The cache should reconnect transparently on the next operation.
	require.NoError(t, cache.Set(context.Background(), "k2", []byte("v2"), nil))
	val, err = cache.Get(context.Background(), "k2")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}
