package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

var elasticIndex = `{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"properties": {
			"user":{
				"type":"keyword"
			},
			"message":{
				"type": "text",
				"store": true,
				"fielddata": true
			}
		}
	}
}`

func TestIntegrationElasticsearchV8(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "8.1.2", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m", // By default ES immediately gobbles half the available RAM, what a psychopath.
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    sniff: false
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		res, err := client.Get().
			Index(testID).
			Id(messageID).
			Do(ctx)
		if err != nil {
			return "", nil, err
		}

		if !res.Found {
			return "", nil, fmt.Errorf("document %v not found", messageID)
		}

		resBytes, err := res.Source.MarshalJSON()
		if err != nil {
			return "", nil, err
		}
		return string(resBytes), nil, nil
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

func TestIntegrationElasticsearchV7(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "7.17.2", []string{
		"discovery.type=single-node",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m", // By default ES immediately gobbles half the available RAM, what a psychopath.
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, err = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
			if err != nil && strings.Contains(err.Error(), "already exists") {
				err = nil
			}
		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    sniff: false
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		res, err := client.Get().
			Index(testID).
			Id(messageID).
			Do(ctx)
		if err != nil {
			return "", nil, err
		}

		if !res.Found {
			return "", nil, fmt.Errorf("document %v not found", messageID)
		}

		resBytes, err := res.Source.MarshalJSON()
		if err != nil {
			return "", nil, err
		}
		return string(resBytes), nil, nil
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

func TestIntegrationConnectTLS(t *testing.T) {
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
	polllURL := fmt.Sprintf("https://elastic:password@localhost:%s", resource.GetPort("9200/tcp"))
	configURL := fmt.Sprintf("https://localhost:%s", resource.GetPort("9200/tcp"))

	err = pool.Retry(func() error {
		resp, err := client.Get(fmt.Sprintf("%s/_cluster/health", polllURL))
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
sniff: false
basic_auth:
  enabled: true
  username: elastic
  password: password
tls:
  enabled: true
  root_cas_file: "%s"
`, configURL, fullPath)

	pConf, err := OutputSpec().ParseYAML(template, nil)
	require.NoError(t, err)

	o, err := OutputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	err = o.Connect(context.Background())
	require.NoError(t, err)
}
func BenchmarkIntegrationElasticsearch(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("elasticsearch", "7.13.4", []string{
		"discovery.type=single-node",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m", // By default ES immediately gobbles half the available RAM, what a psychopath.
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
		}
		return cerr
	}); err != nil {
		b.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    sniff: false
`
	suite := integration.StreamBenchs(
		integration.StreamBenchWrite(20),
		integration.StreamBenchWrite(10),
		integration.StreamBenchWrite(1),
	)
	suite.Run(
		b, template,
		integration.StreamTestOptPort(resource.GetPort("9200/tcp")),
	)
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
