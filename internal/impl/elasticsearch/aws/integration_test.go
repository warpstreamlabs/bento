package aws_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	es "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/warpstreamlabs/bento/internal/impl/elasticsearch"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/internal/impl/elasticsearch/aws"
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

func TestIntegrationElasticsearchAWS(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// Setup a LocalStack instance and create an ElasticSearch domain + cluster.
	// Also, the AWS signer client should skip TLS verification since LS doesn't
	// support this for ElasticSearch.
	// TODO: This approach needs a re-think since it's very verbose

	signer := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	awsConf, err := service.NewConfigSpec().Field(elasticsearch.AWSField()).ParseYAML(`
aws:
  enabled: true
  region: us-east-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx`, nil)
	require.NoError(t, err)

	awsOpts, err := elasticsearch.AWSOptFn(awsConf, signer)
	require.NoError(t, err)

	var awsEsClient *es.Client
	setupESCluster := func(port string) error {
		cfg := aws.Config{
			Region: "us-east-1",
			Credentials: credentials.NewStaticCredentialsProvider(
				"xxxxx", "xxxxx", "xxxxx",
			),
		}

		awsEsClient = es.NewFromConfig(cfg, func(o *es.Options) {
			o.BaseEndpoint = aws.String(fmt.Sprintf("http://localhost:%s", port))
		})

		input := &es.CreateElasticsearchDomainInput{
			DomainName: aws.String("bento-test"),
		}

		if _, esErr := awsEsClient.CreateElasticsearchDomain(context.TODO(), input); esErr != nil {
			return esErr
		}

		return nil
	}

	var client *elastic.Client
	setupClient := func(port string) error {
		resp, esErr := awsEsClient.DescribeElasticsearchDomain(context.TODO(), &es.DescribeElasticsearchDomainInput{
			DomainName: aws.String("bento-test"),
		})
		if esErr != nil {
			return esErr
		}

		if *resp.DomainStatus.Processing {
			return errors.New("es cluster not created yet")
		}

		baseURL := fmt.Sprintf("http://localhost:%s/es/us-east-1/bento-test", port)
		healthResp, esErr := http.Get(fmt.Sprintf("%s/_cluster/health", baseURL))

		if esErr != nil {
			return esErr
		}
		if healthResp.StatusCode != 200 {
			return errors.New("es cluster health check failed")

		}

		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(baseURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
			elastic.SetHttpClient(signer),
		}
		opts = append(opts, awsOpts...)
		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
		}

		return cerr
	}

	envs := []string{"OPENSEARCH_ENDPOINT_STRATEGY=path"}
	servicePort := GetLocalStack(t, envs, setupESCluster, setupClient)
	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT/es/us-east-1/bento-test
    index: $ID
    id: ${!json("id")}
    sniff: false
    healthcheck: false
    aws:
      enabled: true
      endpoint: http://localhost:$PORT
      region: us-east-1
      credentials:
        id: xxxxx
        secret: xxxxx
        token: xxxxx
    tls:
        enabled: true
        skip_cert_verify: true
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
		integration.StreamTestOptPort(servicePort),
	)
}

// TODO: Add config + options pattern or use an already existing library like https://github.com/elgohr/go-localstack
func GetLocalStack(t testing.TB, envVars []string, readyFns ...func(port string) error) (port string) {
	t.Helper()
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
		Tag:          "4.9.2", // pinning version: latest needs a license.
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
