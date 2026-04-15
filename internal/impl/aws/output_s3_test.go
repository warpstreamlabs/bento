package aws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestS3OutputChecksumAlgorithm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		checksumAlgorithm string
		wantChecksumCalc  aws.RequestChecksumCalculation
	}{
		{
			name:              "no checksum set",
			checksumAlgorithm: "",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenRequired,
		},
		{
			name:              "CRC32",
			checksumAlgorithm: "CRC32",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenSupported,
		},
		{
			name:              "CRC32C",
			checksumAlgorithm: "CRC32C",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenSupported,
		},
		{
			name:              "SHA1",
			checksumAlgorithm: "SHA1",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenSupported,
		},
		{
			name:              "SHA256",
			checksumAlgorithm: "SHA256",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenSupported,
		},
		{
			name:              "CRC64NVME",
			checksumAlgorithm: "CRC64NVME",
			wantChecksumCalc:  aws.RequestChecksumCalculationWhenSupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			yaml := fmt.Sprintf(`
bucket: test-bucket
path: test-key
checksum_algorithm: "%s"
`, tt.checksumAlgorithm)

			spec := s3oOutputSpec()
			pConf, err := spec.ParseYAML(yaml, service.NewEnvironment())
			require.NoError(t, err)

			conf, err := s3oConfigFromParsed(pConf)
			require.NoError(t, err)
			assert.Equal(t, tt.checksumAlgorithm, conf.ChecksumAlgorithm)
			assert.Equal(t, tt.wantChecksumCalc, conf.aconf.RequestChecksumCalculation)
		})
	}
}
