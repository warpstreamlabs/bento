package etcd_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func setupEtcd(t testing.TB) (*clientv3.Client, string) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 60

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "gcr.io/etcd-development/etcd",
		Tag:        "v3.6.5",
		Cmd: []string{
			"/usr/local/bin/etcd",
			"--listen-client-urls=http://0.0.0.0:2379",
			"--advertise-client-urls=http://0.0.0.0:2379",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	etcdDockerAddress := fmt.Sprintf("http://localhost:%s", resource.GetPort("2379/tcp"))

	_ = resource.Expire(900)
	var etcdClient *clientv3.Client
	require.NoError(t, pool.Retry(func() (err error) {
		defer func() {
			if err != nil {
				t.Logf("error: %v", err)
			}
		}()

		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{etcdDockerAddress},
			DialTimeout: 5 * time.Second,
		})

		if err != nil {
			return err
		}

		// Check the health of the etcd cluster
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = etcdClient.Get(ctx, "health")
		if err != nil {
			return err
		}

		return nil
	}))

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
		assert.NotNil(t, etcdClient)
		assert.NoError(t, etcdClient.Close())
	})

	return etcdClient, etcdDockerAddress

}

func TestIntegrationEtcd(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Each test will run and use it's own etcd Docker image
	t.Run("watches_single_key", func(t *testing.T) {
		client, address := setupEtcd(t)
		testWatchSingleKey(t, address, client)
	})

	t.Run("watches_all_keys", func(t *testing.T) {
		client, address := setupEtcd(t)
		testWatchKeyPrefix(t, address, client)
	})

}

func testWatchSingleKey(t *testing.T, etcdDockerAddress string, etcdClient *clientv3.Client) {
	template := fmt.Sprintf(`
etcd:
  key: "foobar"
  endpoints:
  - %s`, etcdDockerAddress)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = content()'`))

	var outBatches []service.MessageBatch
	var outBatchMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		outBatches = append(outBatches, mb.DeepCopy())
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		_ = streamOut.Run(context.Background())
	}()

	time.Sleep(time.Second)

	key := "foobar"
	for i := 0; i < 100; i++ {
		value := fmt.Sprintf("bar-%d", i)

		if _, err := etcdClient.Put(context.Background(), key, value); err != nil {
			t.Error(err)
		}
	}

	if _, err = etcdClient.Delete(context.Background(), key); err != nil {
		t.Error(err)
	}

	var tmpSize int
	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		tmpSize = len(outBatches)
		// 100 PUTs and 1 DELETE
		return tmpSize == 101
	}, time.Second*10, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	finalDeleteEvent, err := outBatches[len(outBatches)-1][0].AsBytes()
	require.NoError(t, err)

	for i := 0; i < len(outBatches)-1; i++ {
		putEvent, err := outBatches[i][0].AsBytes()
		require.NoError(t, err)
		expected := fmt.Sprintf(`[{"create_revision":2,"key":"foobar","lease":0,"mod_revision":%d,"type":"PUT","value":"bar-%d","version":%d}]`, i+2, i, i+1)
		require.Equal(t, expected, string(putEvent))
	}
	require.Equal(t, `[{"create_revision":0,"key":"foobar","lease":0,"mod_revision":102,"type":"DELETE","value":"","version":0}]`, string(finalDeleteEvent))

}

func testWatchKeyPrefix(t *testing.T, etcdDockerAddress string, etcdClient *clientv3.Client) {
	template := fmt.Sprintf(`
etcd:
  key: ""
  endpoints:
  - %s
  options:
    with_prefix: true`, etcdDockerAddress)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = content()'`))

	var outBatches []service.MessageBatch
	var outBatchMut sync.Mutex

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		outBatchMut.Lock()
		outBatches = append(outBatches, mb.DeepCopy())
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		_ = streamOut.Run(context.Background())
	}()

	time.Sleep(time.Second)
	prefixes := []string{"/foo", "/foo/bar"}
	for _, prefix := range prefixes {
		for i := 0; i < 100; i++ {

			value := fmt.Sprintf("bar-%d", i)
			if _, err := etcdClient.Put(context.Background(), prefix, value); err != nil {
				t.Error(err)
			}

		}
	}

	if _, err := etcdClient.Delete(context.Background(), "/foo", clientv3.WithPrefix()); err != nil {
		t.Error(err)
	}

	var tmpSize int
	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		tmpSize = len(outBatches)
		t.Logf("length = %d", tmpSize)
		// 200 PUTs and 1 DELETE
		return tmpSize == 201
	}, time.Second*10, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	// First 100 with prefix /foo
	for i := 0; i < 100; i++ {
		putEvent, err := outBatches[i][0].AsBytes()
		require.NoError(t, err)

		tmpl := `[{"create_revision":2,"key":"/foo","lease":0,"mod_revision":%d,"type":"PUT","value":"bar-%d","version":%d}]`

		expectedVersion := i + 1
		expectedModRevision := i + 2

		expected := fmt.Sprintf(tmpl, expectedModRevision, i, expectedVersion)
		require.Equal(t, expected, string(putEvent))
	}

	// Next 100 with prefix /foo/bar
	for i := 0; i < 100; i++ {
		offset := i + 100
		putEvent, err := outBatches[offset][0].AsBytes()
		require.NoError(t, err)

		tmpl := `[{"create_revision":102,"key":"/foo/bar","lease":0,"mod_revision":%d,"type":"PUT","value":"bar-%d","version":%d}]`

		expectedVersion := i + 1
		expectedModRevision := offset + 2

		expected := fmt.Sprintf(tmpl, expectedModRevision, i, expectedVersion)
		require.Equal(t, expected, string(putEvent))
	}

	finalDeleteEvent, err := outBatches[len(outBatches)-1][0].AsBytes()
	require.NoError(t, err)

	expectedDeleteEvent := "[" + `{"create_revision":0,"key":"/foo","lease":0,"mod_revision":202,"type":"DELETE","value":"","version":0}` + "," +
		`{"create_revision":0,"key":"/foo/bar","lease":0,"mod_revision":202,"type":"DELETE","value":"","version":0}` + "]"

	require.Equal(t, expectedDeleteEvent, string(finalDeleteEvent))
}
