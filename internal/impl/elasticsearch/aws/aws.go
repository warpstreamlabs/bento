package aws

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/olivere/elastic/v7"
	es_aws "github.com/olivere/elastic/v7/aws/v4"

	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/internal/impl/elasticsearch"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	elasticsearch.AWSOptFn = func(conf *service.ParsedConfig, client *http.Client) ([]elastic.ClientOptionFunc, error) {
		if enabled, _ := conf.FieldBool(elasticsearch.ESOFieldAWSEnabled); !enabled {
			return nil, nil
		}

		tsess, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return nil, err
		}

		region := tsess.Region
		if region == "" {
			return nil, errors.New("unable to detect target AWS region, if you encounter this error please report it via: https://github.com/warpstreamlabs/bento/issues/new")
		}

		creds, err := tsess.Credentials.Retrieve(context.TODO())
		if err != nil {
			return nil, err
		}

		staticCreds := credentials.NewStaticCredentials(
			creds.AccessKeyID,
			creds.SecretAccessKey,
			creds.SessionToken,
		)

		awsClient := es_aws.NewV4SigningClientWithHTTPClient(staticCreds, region, client)
		return []elastic.ClientOptionFunc{elastic.SetHttpClient(awsClient)}, nil
	}
}
