package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

func TestIntegrationV2ConnectNoAuth(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "8.16.5", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
		"xpack.security.transport.ssl.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	url := fmt.Sprintf("http://localhost:%s", resource.GetPort("9200/tcp"))

	err = pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("%s/_cluster/health", url))
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

	template := fmt.Sprintf(`
urls:
  - %s
index: test-index
id: ${! uuid_v4() }
`, url) // TODO: Update with defaults if changed

	pConf, err := OutputSpecV2().ParseYAML(template, nil)
	require.NoError(t, err)

	o, err := esoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)

	err = o.Connect(context.Background())
	require.NoError(t, err)
}

func TestIntegrationV2ConnectBasicAuth(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "8.16.5", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=true",
		"xpack.security.transport.ssl.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
		"ELASTIC_PASSWORD=password",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	polllURL := fmt.Sprintf("http://elastic:password@localhost:%s", resource.GetPort("9200/tcp"))
	configURL := fmt.Sprintf("http://localhost:%s", resource.GetPort("9200/tcp"))

	err = pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("%s/_cluster/health", polllURL))
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

	template := fmt.Sprintf(`
urls:
  - %s
index: test-index
id: ${! uuid_v4() }
basic_auth:
  enabled: true
  username: elastic
  password: password
`, configURL) // TODO: Update with defaults if changed

	pConf, err := OutputSpecV2().ParseYAML(template, nil)
	require.NoError(t, err)

	o, err := esoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)

	err = o.Connect(context.Background())
	require.NoError(t, err)
}

func TestIntegrationV2ConnectTLS(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "8.16.5", []string{
		"discovery.type=single-node",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
		"ELASTIC_PASSWORD=password",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	// give elasticsearch a moment to create a tls cert
	time.Sleep(time.Second * 20)

	containerName := resource.Container.Name[1:]
	cmd := exec.Command("docker", "exec", containerName, "cat", "config/certs/http_ca.crt")
	output, err := cmd.Output()
	require.NoError(t, err)

	tmpDir := t.TempDir()
	fileName := "http_ca.crt"
	fullPath := filepath.Join(tmpDir, fileName)

	err = os.WriteFile(fullPath, output, 0644)
	require.NoError(t, err)

	client := createHTTPClientWithCA(t, output)
	pollURL := fmt.Sprintf("https://elastic:password@localhost:%s", resource.GetPort("9200/tcp"))
	configURL := fmt.Sprintf("https://localhost:%s", resource.GetPort("9200/tcp"))

	err = pool.Retry(func() error {
		resp, err := client.Get(fmt.Sprintf("%s/_cluster/health", pollURL))
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

	template := fmt.Sprintf(`
urls:
  - %s
index: test-index
id: ${! uuid_v4() }
basic_auth:
  enabled: true
  username: elastic
  password: password
tls:
  enabled: true
  root_cas_file: "%s"
`, configURL, fullPath) // TODO: Update with defaults if changed

	pConf, err := OutputSpecV2().ParseYAML(template, nil)
	require.NoError(t, err)

	o, err := esoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)

	err = o.Connect(context.Background())
	require.NoError(t, err)
}

func createHTTPClientWithCA(t *testing.T, caBytes []byte) *http.Client {
	caCertPool := x509.NewCertPool()
	require.True(t, caCertPool.AppendCertsFromPEM(caBytes))

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	return &http.Client{Transport: transport}
}

func TestIntegrationV2ElasticsearchV8(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "8.16.5", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
		"xpack.security.transport.ssl.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	url := fmt.Sprintf("http://localhost:%s", resource.GetPort("9200/tcp"))

	err = pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("%s/_cluster/health", url))
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

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch_v2:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${! json("id") }
`
	var client *elasticsearch.Client

	client, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))},
	})
	require.NoError(t, err)

	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		res, err := client.Get(testID, messageID)
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
