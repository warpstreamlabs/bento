package aws

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

// TODO: Add config + options pattern or use an already existing library like https://github.com/elgohr/go-localstack
func GetLocalStack(t testing.TB, envVars []string, readyFns ...func(port string) error) (port string) {
	portInt, err := integration.GetFreePort()
	require.NoError(t, err)

	port = strconv.Itoa(portInt)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	lsImageName := "localstack/localstack"
	var env []string
	env = append(env, envVars...)

	// If an auth token is provided, use the pro-image
	if authToken, isPro := os.LookupEnv("LOCALSTACK_AUTH_TOKEN"); isPro && authToken != "" {
		env = append(env, "LOCALSTACK_AUTH_TOKEN="+authToken)
		lsImageName = lsImageName + "-pro"
	}
	env = append(env, "LS_LOG=debug")

	pool.MaxWait = time.Minute * 300

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   lsImageName,
		ExposedPorts: []string{"4566/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(port + "/tcp"): {
				docker.PortBinding{HostIP: "", HostPort: port},
			},
		},
		Env: env,
	})
	port = resource.GetPort("4566/tcp")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	require.NoError(t, pool.Retry(func() error {
		var err error
		defer func() {
			if err != nil {
				t.Logf("localstack probe error: %v", err)
			}
		}()
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/_localstack/health", port))
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("cannot connect to LocalStack")
		}

		return nil
	}))

	for _, readyFn := range readyFns {
		require.NoError(t, pool.Retry(func() error {
			return readyFn(port)
		}))
	}

	return
}
