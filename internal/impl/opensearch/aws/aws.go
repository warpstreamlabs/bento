package aws

import (
	"context"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/signer/awsv2"

	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/internal/impl/opensearch"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	opensearch.AWSOptFn = func(conf *service.ParsedConfig, osconf *opensearchapi.Config) error {
		if enabled, _ := conf.FieldBool(opensearch.ESOFieldAWSEnabled); !enabled {
			return nil
		}

		tsess, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return err
		}

		signer, err := awsv2.NewSigner(tsess)
		if err != nil {
			return err
		}

		osconf.Client.Signer = signer
		return nil
	}
}
