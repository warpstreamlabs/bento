package aws

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/warpstreamlabs/bento/internal/impl/kafka"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/twmb/franz-go/pkg/sasl"
	kaws "github.com/twmb/franz-go/pkg/sasl/aws"

	sess "github.com/warpstreamlabs/bento/internal/impl/aws"
)

func init() {
	kafka.AWSSASLFromConfigFn = func(c *service.ParsedConfig) (sasl.Mechanism, error) {
		awsConf, err := sess.GetSession(context.TODO(), c.Namespace("aws"))
		if err != nil {
			return nil, err
		}

		creds := awsConf.Credentials
		return kaws.ManagedStreamingIAM(func(ctx context.Context) (kaws.Auth, error) {
			val, err := creds.Retrieve(ctx)
			if err != nil {
				return kaws.Auth{}, err
			}
			return kaws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
			}, nil
		}), nil
	}

	kafka.SaramaTokenProviderFromConfigFn = func(c *service.ParsedConfig) (sarama.AccessTokenProvider, error) {
		awsConf, err := sess.GetSession(context.Background(), c.Namespace("aws"))
		if err != nil {
			return nil, err
		}

		return &mskAccessTokenProvider{
			region:              awsConf.Region,
			credentialsProvider: awsConf.Credentials,
		}, nil
	}
}

type mskAccessTokenProvider struct {
	region              string
	credentialsProvider aws.CredentialsProvider
}

func (m *mskAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(
		context.Background(),
		m.region,
		m.credentialsProvider,
	)
	return &sarama.AccessToken{Token: token}, err
}
