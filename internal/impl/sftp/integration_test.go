package sftp

import (
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"

	// Bring in memory cache.
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

var (
	sftpUsername = "foo"
	sftpPassword = "pass"
)

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			// https://github.com/atmoz/sftp/issues/401
			"/bin/sh", "-c", "ulimit -n 65535 && exec /entrypoint " + sftpUsername + ":" + sftpPassword + ":1001:100:upload",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	creds := Credentials{
		Username: sftpUsername,
		Password: sftpPassword,
	}

	require.NoError(t, pool.Retry(func() error {
		_, err = creds.GetClient(&osPT{}, "localhost:"+resource.GetPort("22/tcp"))
		return err
	}))

	t.Run("sftp", func(t *testing.T) {
		template := `
output:
  sftp:
    address: localhost:$PORT
    path: /upload/test-$ID/${!uuid_v4()}.txt
    credentials:
      username: foo
      password: pass
    codec: $VAR1
    max_in_flight: 1

input:
  sftp:
    address: localhost:$PORT
    paths:
      - /upload/test-$ID/*.txt
    credentials:
      username: foo
      password: pass
    codec: $VAR1
    delete_on_finish: false
    watcher:
      enabled: $VAR2
      minimum_age: 100ms
      poll_interval: 100ms
      cache: files_memory

cache_resources:
  - label: files_memory
    memory:
      default_ttl: 900s
`
		suite := integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(100),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptPort(resource.GetPort("22/tcp")),
			integration.StreamTestOptVarSet("VAR1", "all-bytes"),
			integration.StreamTestOptVarSet("VAR2", "false"),
		)

		t.Run("watcher", func(t *testing.T) {
			watcherSuite := integration.StreamTests(
				integration.StreamTestOpenClose(),
				integration.StreamTestStreamParallel(50),
				integration.StreamTestStreamSequential(20),
				integration.StreamTestStreamParallelLossyThroughReconnect(20),
			)
			watcherSuite.Run(
				t, template,
				integration.StreamTestOptPort(resource.GetPort("22/tcp")),
				integration.StreamTestOptVarSet("VAR1", "all-bytes"),
				integration.StreamTestOptVarSet("VAR2", "true"),
			)
		})
	})
}

type osPT struct{}

func (o *osPT) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (o *osPT) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (o *osPT) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (o *osPT) Remove(name string) error {
	return os.Remove(name)
}

func (o *osPT) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}
