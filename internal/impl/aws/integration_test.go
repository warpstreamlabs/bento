package aws

import (
	"testing"

	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	servicePort := GetLocalStack(t, nil)

	t.Run("kinesis", func(t *testing.T) {
		kinesisIntegrationSuite(t, servicePort)
	})

	t.Run("s3", func(t *testing.T) {
		s3IntegrationSuite(t, servicePort)
	})

	t.Run("sqs", func(t *testing.T) {
		sqsIntegrationSuite(t, servicePort)
	})
}
