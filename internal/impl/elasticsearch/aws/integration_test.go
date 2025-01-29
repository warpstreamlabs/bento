package aws_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	es "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
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
	servicePort := baws.GetLocalStack(t, envs, setupESCluster, setupClient)
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
