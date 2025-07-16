package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationV2ElasticsearchV8(t *testing.T) {
	integration.CheckSkip(t)

	pool, resource, port := startElasticsearch(t, []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
		"xpack.security.transport.ssl.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
	})

	url := fmt.Sprintf("http://localhost:%s", port)
	client := &http.Client{}

	waitForElasticsearch(t, pool, client, url)

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch_v2:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${! json("id") }
`
	var esClient *elasticsearch.Client
	var err error

	esClient, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		res, err := esClient.Get(testID, messageID)
		if err != nil {
			return "", nil, err
		}
		defer res.Body.Close()

		if res.StatusCode == http.StatusNotFound {
			return "", nil, fmt.Errorf("document %v not found", messageID)
		}

		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", nil, err
		}

		var parsed map[string]any
		if err := json.Unmarshal(resBytes, &parsed); err != nil {
			return "", nil, fmt.Errorf("failed to parse response: %w", err)
		}

		source, err := json.Marshal(parsed["_source"])
		if err != nil {
			return "", nil, fmt.Errorf("failed to marshal _source: %w", err)
		}

		return string(source), nil, nil
	}

	suite := integration.StreamTests(
		integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("9200/tcp")),
	)
}

func TestIntegrationV2Connect(t *testing.T) {
	integration.CheckSkip(t)
	tests := []struct {
		name       string
		env        []string
		pollURL    func(port string) string
		configYAML func(argsString []string) string
	}{
		{
			name: "NoAuth",
			env: []string{
				"discovery.type=single-node",
				"xpack.security.enabled=false",
				"xpack.security.transport.ssl.enabled=false",
				"xpack.security.http.ssl.enabled=false",
				"ES_JAVA_OPTS=-Xms512m -Xmx512m",
			},
			pollURL: func(port string) string {
				return fmt.Sprintf("http://localhost:%s", port)
			},
			configYAML: func(argsString []string) string {
				argsAny := make([]any, 0, 2)
				for _, v := range argsString {
					argsAny = append(argsAny, v)
				}
				return fmt.Sprintf(`
urls:
  - http://localhost:%s
index: test-index
id: ${! uuid_v4() }
`, argsAny...)
			},
		},
		{
			name: "BasicAuth",
			env: []string{
				"discovery.type=single-node",
				"xpack.security.enabled=true",
				"xpack.security.transport.ssl.enabled=false",
				"xpack.security.http.ssl.enabled=false",
				"ES_JAVA_OPTS=-Xms512m -Xmx512m",
				"ELASTIC_PASSWORD=password",
			},
			pollURL: func(port string) string {
				return fmt.Sprintf("http://elastic:password@localhost:%s", port)
			},
			configYAML: func(argsString []string) string {
				argsAny := make([]any, 0, 2)
				for _, v := range argsString {
					argsAny = append(argsAny, v)
				}
				return fmt.Sprintf(`
urls: 
  - http://localhost:%s
index: test-index
id: ${! uuid_v4 }
basic_auth:
  enabled: true
  username: elastic
  password: password
`, argsAny...)
			},
		},
		{
			name: "TLS",
			env: []string{
				"discovery.type=single-node",
				"ES_JAVA_OPTS=-Xms512m -Xmx512m",
				"ELASTIC_PASSWORD=password",
			},
			pollURL: func(port string) string {
				return fmt.Sprintf("https://elastic:password@localhost:%s", port)
			},
			configYAML: func(argsString []string) string {
				argsAny := make([]any, 0, 2)
				for _, v := range argsString {
					argsAny = append(argsAny, v)
				}
				return fmt.Sprintf(`
urls:
  - https://localhost:%s
index: test-index
id: ${! uuid_v4 }
basic_auth:
  enabled: true
  username: elastic
  password: password
tls:
  enabled: true
  root_cas_file: "%s"
`, argsAny...)
			},
		},
	}

	for _, tc := range tests {

		pool, resource, port := startElasticsearch(t, tc.env)
		pollURL := tc.pollURL(port)

		var client *http.Client
		var configArgs []string
		if tc.name == "TLS" {
			// give elasticsearch a moment to create a tls cert
			time.Sleep(time.Second * 20)

			containerName := resource.Container.Name[1:]
			cmd := exec.Command("docker", "exec", containerName, "cat", "config/certs/http_ca.crt")
			output, err := cmd.Output()
			require.NoError(t, err)

			tmpDir := t.TempDir()
			fileName := "http_ca.crt"
			CAFilePath := filepath.Join(tmpDir, fileName)

			err = os.WriteFile(CAFilePath, output, 0644)
			require.NoError(t, err)

			client = createHTTPClientWithCA(t, output)
			configArgs = []string{port, CAFilePath}
		} else {
			client = &http.Client{}
			configArgs = []string{port}
		}

		waitForElasticsearch(t, pool, client, pollURL)

		connectOutput(t, tc.configYAML(configArgs))
	}
}

//------------------------------------------------------------------------------

func startElasticsearch(t *testing.T, env []string) (pool *dockertest.Pool, resource *dockertest.Resource, port string) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err = pool.Run("elasticsearch", "8.16.5", env)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port = resource.GetPort("9200/tcp")
	return pool, resource, port
}

func waitForElasticsearch(t *testing.T, pool *dockertest.Pool, client *http.Client, url string) {
	err := pool.Retry(func() error {
		resp, err := client.Get(fmt.Sprintf("%s/_cluster/health", url))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("not ready yet, status: %s", resp.Status)
		}
		return nil
	})
	require.NoError(t, err)
}

func connectOutput(t *testing.T, template string) {
	pConf, err := OutputSpecV2().ParseYAML(template, nil)
	require.NoError(t, err)

	o, err := EsoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)

	err = o.Connect(context.Background())
	require.NoError(t, err)
}
