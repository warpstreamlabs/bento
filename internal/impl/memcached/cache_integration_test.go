package memcached

import (
	"fmt"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationMemcachedCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "memcached",
		dockertest.WithTag("latest"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		client := memcache.New(fmt.Sprintf("localhost:%v", resource.GetPort("11211/tcp")))
		cErr := client.Set(&memcache.Item{
			Key:        "testkey",
			Value:      []byte("testvalue"),
			Expiration: 30,
		})
		if cErr != nil {
			return cErr
		}
		if _, cErr = client.Get("testkey"); cErr != nil {
			return cErr
		}
		return nil
	}))

	template := `
cache_resources:
  - label: testcache
    memcached:
      addresses: [ localhost:$PORT ]
      prefix: $ID
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
		integration.CacheTestMissingKeyExists(),
		integration.CacheTestExistsAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(resource.GetPort("11211/tcp")),
	)
}
