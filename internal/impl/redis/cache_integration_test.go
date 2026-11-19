package redis

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationRedisCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	resource := pool.RunT(t, "redis",
		dockertest.WithTag("latest"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`url: %v`, url), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "bento_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: tcp://localhost:$PORT/1
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
		integration.CacheTestOptPort(resource.GetPort("6379/tcp")),
	)
}

func TestIntegrationRedisClusterCache(t *testing.T) {
	t.Skip("Skipping as networking often fails for this test")

	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	nets, _ := pool.Client().NetworkList(t.Context(), mobyclient.NetworkListOptions{})
	hostIP := ""
	for _, n := range nets.Items {
		// Gateway is a netip.Addr in the moby API; the zero value stringifies
		// as "invalid IP" rather than "", hence the validity check.
		if n.Name == "bridge" && len(n.IPAM.Config) > 0 && n.IPAM.Config[0].Gateway.IsValid() {
			hostIP = n.IPAM.Config[0].Gateway.String()
		}
	}
	if runtime.GOOS == "darwin" {
		hostIP = "0.0.0.0"
	}

	portBindings := make(dockernetwork.PortMap, 12)
	for i := range 6 {
		// HostPort is a bare number; the "/tcp" suffix belongs only to the
		// container-side port. v3 accepted the malformed value silently.
		portBindings[dockernetwork.MustParsePort(fmt.Sprintf("%d/tcp", 7000+i))] =
			[]dockernetwork.PortBinding{{HostPort: strconv.Itoa(7000 + i)}}
		portBindings[dockernetwork.MustParsePort(fmt.Sprintf("%d/tcp", 17000+i))] =
			[]dockernetwork.PortBinding{{HostPort: strconv.Itoa(17000 + i)}}
	}

	pool.RunT(t, "grokzen/redis-cluster",
		dockertest.WithName("redis-cluster"),
		dockertest.WithTag("6.0.7"),
		dockertest.WithPortBindings(portBindings),
		dockertest.WithEnv([]string{
			"IP=" + hostIP,
		}),
		dockertest.WithoutReuse(),
	)

	clusterURL := ""
	for i := range 6 {
		clusterURL += fmt.Sprintf("redis://%s:%s/0,", hostIP, fmt.Sprintf("%d", 7000+i))
	}
	clusterURL = strings.TrimSuffix(clusterURL, ",")

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: cluster
`, clusterURL), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "bento_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: $VAR1
      kind: cluster
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
		integration.CacheTestOptVarSet("VAR1", clusterURL),
	)
}

func TestIntegrationRedisFailoverCache(t *testing.T) {
	t.Skip("Skipping as networking often fails for this test")

	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	nets, _ := pool.Client().NetworkList(t.Context(), mobyclient.NetworkListOptions{})
	hostIP := ""
	for _, n := range nets.Items {
		// Gateway is a netip.Addr in the moby API; the zero value stringifies
		// as "invalid IP" rather than "", hence the validity check.
		if n.Name == "bridge" && len(n.IPAM.Config) > 0 && n.IPAM.Config[0].Gateway.IsValid() {
			hostIP = n.IPAM.Config[0].Gateway.String()
		}
	}
	if runtime.GOOS == "darwin" {
		hostIP = "0.0.0.0"
	}

	// CreateNetworkT tracks the network; the pool removes it during cleanup.
	_ = pool.CreateNetworkT(t, "redis-sentinel", nil)

	master := pool.RunT(t, "bitnami/redis",
		dockertest.WithName("redis-master"),
		dockertest.WithTag("6.0.9"),
		// v4 attaches at creation via NetworkMode; it takes a single network
		// where v3 took a slice, which is all this test needs.
		dockertest.WithHostConfig(func(hc *dockercontainer.HostConfig) {
			hc.NetworkMode = dockercontainer.NetworkMode("redis-sentinel")
		}),
		dockertest.WithPortBindings(dockernetwork.PortMap{
			dockernetwork.MustParsePort("6379/tcp"): {{HostPort: "6379"}},
		}),
		dockertest.WithEnv([]string{
			"ALLOW_EMPTY_PASSWORD=yes",
		}),
		dockertest.WithoutReuse(),
	)

	sentinel := pool.RunT(t, "bitnami/redis-sentinel",
		dockertest.WithName("redis-failover"),
		dockertest.WithTag("6.0.9"),
		dockertest.WithHostConfig(func(hc *dockercontainer.HostConfig) {
			hc.NetworkMode = dockercontainer.NetworkMode("redis-sentinel")
		}),
		dockertest.WithPortBindings(dockernetwork.PortMap{
			dockernetwork.MustParsePort("26379/tcp"): {{HostPort: "26379"}},
		}),
		dockertest.WithEnv([]string{
			"REDIS_SENTINEL_ANNOUNCE_IP=" + hostIP,
			"REDIS_SENTINEL_QUORUM=1",
			"REDIS_MASTER_HOST=" + hostIP,
			"REDIS_MASTER_PORT_NUMBER=" + master.GetPort("6379/tcp"),
		}),
		dockertest.WithoutReuse(),
	)

	t.Cleanup(func() {
	})

	clusterURL := ""
	clusterURL += fmt.Sprintf("redis://%s:%s/0,", hostIP, sentinel.GetPort("26379/tcp"))
	clusterURL = strings.TrimSuffix(clusterURL, ",")

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		pConf, cErr := redisCacheConfig().ParseYAML(fmt.Sprintf(`
url: %v
kind: failover
master: mymaster
`, clusterURL), nil)
		if cErr != nil {
			return cErr
		}

		r, cErr := newRedisCacheFromConfig(pConf)
		if cErr != nil {
			return cErr
		}

		cErr = r.Set(context.Background(), "bento_test_redis_connect", []byte("foo bar"), nil)
		return cErr
	}))

	template := `
cache_resources:
  - label: testcache
    redis:
      url: $VAR1
      kind: failover
      master: mymaster
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
		integration.CacheTestOptVarSet("VAR1", clusterURL),
	)
}
