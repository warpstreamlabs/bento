package aws

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

// TODO: Add config + options pattern or use an already existing library like https://github.com/elgohr/go-localstack
func GetLocalStack(t testing.TB, envVars []string, readyFns ...func(port string) error) (port string) {
	t.Helper()
	portInt, err := integration.GetFreePort()
	require.NoError(t, err)

	port = strconv.Itoa(portInt)

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	lsImageName := "localstack/localstack"
	var env []string
	env = append(env, envVars...)

	// If an auth token is provided, use the pro-image
	if authToken, isPro := os.LookupEnv("LOCALSTACK_AUTH_TOKEN"); isPro && authToken != "" {
		env = append(env, "LOCALSTACK_AUTH_TOKEN="+authToken)
		lsImageName = lsImageName + "-pro"
	}
	env = append(env, "LS_LOG=debug")

	resource := pool.RunT(t, lsImageName,
		dockertest.WithTag("4.9.2"), // pinning version: latest needs a license.
		// 4566 has no binding of its own, and GetPort reads it below. v4 only
		// waits for bindings when ExposedPorts is set, so this must stay.
		dockertest.WithContainerConfig(func(c *dockercontainer.Config) {
			c.ExposedPorts = dockernetwork.PortSet{
				dockernetwork.MustParsePort("4566/tcp"): {},
			}
		}),
		dockertest.WithPortBindings(dockernetwork.PortMap{
			dockernetwork.MustParsePort(port + "/tcp"): {
				dockernetwork.PortBinding{HostPort: port},
			},
		}),
		dockertest.WithEnv(env),
		dockertest.WithoutReuse(),
	)
	port = resource.GetPort("4566/tcp")

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
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
		require.NoError(t, pool.Retry(t.Context(), 0, func() error {
			return readyFn(port)
		}))
	}

	return
}
