package couchbase_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

var (
	username           = "bento"
	password           = "password"
	port               = ""
	integrationCleanup func() error
	integrationOnce    sync.Once
)

// TestMain cleanup couchbase cluster if required by tests.
func TestMain(m *testing.M) {
	code := m.Run()
	if integrationCleanup != nil {
		if err := integrationCleanup(); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}

func requireCouchbase(tb testing.TB) string {
	integrationOnce.Do(func() {
		pool, resource, err := setupCouchbase(tb)
		require.NoError(tb, err)

		port = resource.GetPort("11210/tcp")
		integrationCleanup = func() error {
			return pool.Purge(resource)
		}
	})

	return port
}

func setupCouchbase(tb testing.TB) (*dockertest.Pool, *dockertest.Resource, error) {
	tb.Log("setup couchbase cluster")

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get working directory: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "couchbase",
		Tag:        "latest",
		Cmd:        []string{"/opt/couchbase/configure-server.sh"},
		Env: []string{
			"CLUSTER_NAME=couchbase",
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_USERNAME=%s", username),
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_PASSWORD=%s", password),
		},
		Mounts: []string{
			fmt.Sprintf("%s/testdata/configure-server.sh:/opt/couchbase/configure-server.sh", pwd),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8091/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "8091",
				},
			},
			"11210/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "11210",
				},
			},
		},
	})
	if err != nil {
		return nil, nil, err
	}

	// Look for readyness
	var stderr bytes.Buffer
	time.Sleep(15 * time.Second)
	for {
		time.Sleep(time.Second)
		exitCode, err := resource.Exec([]string{"/usr/bin/cat", "/is-ready"}, dockertest.ExecOptions{
			StdErr: &stderr, // without stderr exit code is not reported
		})
		if exitCode == 0 && err == nil {
			break
		}
	}

	tb.Log("couchbase cluster ready")

	return pool, resource, nil
}

func TestIntegrationCouchbaseProcessor(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	bucket := fmt.Sprintf("testing-processor-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
	})

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseProcessorInsert(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseProcessorRemove(uid, bucket, servicePort, t)
	})
	t.Run("GetMissing", func(t *testing.T) {
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testCouchbaseProcessorUpsert(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseProcessorReplace(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
	t.Run("TTL", func(t *testing.T) {
		testCouchbaseProcessorUpsertTTL(payload, bucket, servicePort, t)
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
		time.Sleep(5 * time.Second)
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})
	// counters tests (entry cleared by ttl before)
	t.Run("Increment non existing", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "1", "1", bucket, servicePort, t)
	})
	t.Run("Decrement to minimal value", func(t *testing.T) { // minimum value of counter is zero
		testCouchbaseProcessorCounter(uid, "decrement", "2", "0", bucket, servicePort, t)
	})
	t.Run("Increment", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "8", "8", bucket, servicePort, t)
	})
	t.Run("Decrement", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "2", "6", bucket, servicePort, t)
	})
	// noop only retrive value
	t.Run("Increment zero", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "0", "6", bucket, servicePort, t)
	})
	t.Run("Decrement zero", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "0", "6", bucket, servicePort, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseProcessorRemove(uid, bucket, servicePort, t)
	})
	t.Run("Decrement non existing with negative initial", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "-10", "10", bucket, servicePort, t)
	})

	t.Run("Error increment empty", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "", bucket, servicePort, t)
	})
	t.Run("Error decrement empty", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "", bucket, servicePort, t)
	})
	t.Run("Error increment float", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "0.1", bucket, servicePort, t)
	})
	t.Run("Error decrement float", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "0.1", bucket, servicePort, t)
	})
	t.Run("Error increment invalid", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "invalid", bucket, servicePort, t)
	})
	t.Run("Error decrement invalid", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "invalid", bucket, servicePort, t)
	})
}

func TestIntegrationCouchbaseOutput(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	bucket := fmt.Sprintf("testing-processor-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
	})

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseOutputInsert(payload, bucket, servicePort, t)
	})
	// validate with processor.
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseOutputRemove(uid, bucket, servicePort, t)
	})
	t.Run("GetMissing", func(t *testing.T) {
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testCouchbaseOutputUpsert(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseOutputReplace(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
	t.Run("TTL", func(t *testing.T) {
		testCouchbaseOutputUpsertTTL(payload, bucket, servicePort, t)
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
		time.Sleep(5 * time.Second)
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})
}

func TestIntegrationCouchbaseStream(t *testing.T) {
	ctx := context.Background()

	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)
	bucket := fmt.Sprintf("testing-stream-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
	})

	for _, clearCAS := range []bool{true, false} {
		t.Run(fmt.Sprintf("%t", clearCAS), func(t *testing.T) {
			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))

			inFn, err := streamOutBuilder.AddBatchProducerFunc()
			require.NoError(t, err)

			var outBatches []service.MessageBatch
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
				outBatchMut.Lock()
				outBatches = append(outBatches, mb)
				outBatchMut.Unlock()
				return nil
			}))

			// insert
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  cas_enabled: true
  id: '${! json("key") }'
  content: 'root = this'
  operation: 'insert'
`, servicePort, bucket, username, password)))

			if clearCAS { // ignore cas check
				require.NoError(t, streamOutBuilder.AddProcessorYAML(`
mapping: |
  meta couchbase_cas = deleted()
`))
			}

			// replace
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  cas_enabled: true
  id: '${! json("key") }'
  content: 'root = this'
  operation: 'replace'
`, servicePort, bucket, username, password)))

			if clearCAS { // ignore cas check
				require.NoError(t, streamOutBuilder.AddProcessorYAML(`
mapping: |
  meta couchbase_cas = deleted()
`))
			}
			// remove
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  cas_enabled: true
  id: '${! json("key") }'
  operation: 'remove'
`, servicePort, bucket, username, password)))

			streamOut, err := streamOutBuilder.Build()
			require.NoError(t, err)
			go func() {
				err = streamOut.Run(context.Background())
				require.NoError(t, err)
			}()

			require.NoError(t, inFn(ctx, service.MessageBatch{
				service.NewMessage([]byte(`{"key":"hello","value":"word"}`)),
			}))
			require.NoError(t, streamOut.StopWithin(time.Second*15))

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1
			}, time.Second*5, time.Millisecond*100)

			// batch processing should be fine and contain one message.
			assert.NoError(t, err)
			assert.Len(t, outBatches, 1)
			assert.Len(t, outBatches[0], 1)

			// message should contain an error.
			assert.NoError(t, outBatches[0][0].GetError())
		})
	}
}

func TestIntegrationCouchbaseStreamError(t *testing.T) {
	ctx := context.Background()

	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)
	bucket := fmt.Sprintf("testing-stream-error-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
	})

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))

	inFn, err := streamOutBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	var outBatches []service.MessageBatch
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		outBatches = append(outBatches, mb)
		outBatchMut.Unlock()
		return nil
	}))

	// insert
	require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  cas_enabled: true
  id: '${! json("key") }'
  content: |
    root = this
    root.at = timestamp_unix_micro()
  operation: 'insert'
`, servicePort, bucket, username, password)))

	// upsert and remove in parallel
	require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
workflow:
  meta_path: ""
  branches:
    write:
      processors:
        - couchbase:
            url: 'couchbase://localhost:%[1]s'
            bucket: %[2]s
            username: %[3]s
            password: %[4]s
            cas_enabled: true
            id: '${! json("key") }'
            content: |
              root = this
              root.at = timestamp_unix_micro()
            operation: 'replace'
    remove:
      processors:
        - sleep:
            duration: "1s"
        - couchbase:
            url: 'couchbase://localhost:%[1]s'
            bucket: %[2]s
            username: %[3]s
            password: %[4]s
            cas_enabled: true
            id: '${! json("key") }'
            content: |
              root = this
              root.at = timestamp_unix_micro()
            operation: 'replace'
`, servicePort, bucket, username, password)))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	go func() {
		err = streamOut.Run(context.Background())
		require.NoError(t, err)
	}()

	require.NoError(t, inFn(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"key":"hello","value":"word"}`)),
	}))
	require.NoError(t, streamOut.StopWithin(time.Second*15))

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1
	}, time.Second*5, time.Millisecond*100)

	// batch contain one message.
	assert.NoError(t, err)
	assert.Len(t, outBatches, 1)
	assert.Len(t, outBatches[0], 1)

	// message should contain an error.
	assert.Error(t, outBatches[0][0].GetError())
}
