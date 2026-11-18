package hdfs

import (
	"testing"
	"time"

	"github.com/colinmarc/hdfs"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationHDFS(t *testing.T) {
	integration.CheckSkip(t)
	// t.Skip() // Skip until we fix the static port bindings
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	pool.RunT(t, "cybermaggedon/hadoop",
		dockertest.WithTag("2.8.2"),
		dockertest.WithHostname("localhost"),
		dockertest.WithPortBindings(dockernetwork.PortMap{
			dockernetwork.MustParsePort("9000/tcp"):  {{HostPort: "9000"}},
			dockernetwork.MustParsePort("50070/tcp"): {{HostPort: "50070"}},
			dockernetwork.MustParsePort("50075/tcp"): {{HostPort: "50075"}},
			dockernetwork.MustParsePort("50010/tcp"): {{HostPort: "50010"}},
		}),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		testFile := "/cluster_ready" + time.Now().Format("20060102150405")
		client, err := hdfs.NewClient(hdfs.ClientOptions{
			Addresses: []string{"localhost:9000"},
			User:      "root",
		})
		if err != nil {
			return err
		}
		fw, err := client.Create(testFile)
		if err != nil {
			return err
		}
		_, err = fw.Write([]byte("testing hdfs reader"))
		if err != nil {
			return err
		}
		err = fw.Close()
		if err != nil {
			return err
		}
		_ = client.Remove(testFile)
		return nil
	}))

	template := `
output:
  hdfs:
    hosts: [ localhost:9000 ]
    user: root
    directory: /$ID
    path: ${!count("$ID")}-${!timestamp_unix_nano()}.txt
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  hdfs:
    hosts: [ localhost:9000 ]
    user: root
    directory: /$ID
`
	integration.StreamTests(
		integration.StreamTestOpenCloseIsolated(),
		integration.StreamTestStreamIsolated(10),
		integration.StreamTestSendBatchCountIsolated(10),
	).Run(t, template)
}
