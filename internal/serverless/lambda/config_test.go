package lambda_test

import (
	"testing"

	"github.com/warpstreamlabs/bento/v1/internal/serverless/lambda"
	_ "github.com/warpstreamlabs/bento/v1/public/components/all"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfig(t *testing.T) {
	conf, _, err := lambda.DefaultConfigAndSpec()
	require.NoError(t, err)

	assert.Equal(t, "none", conf.Metrics.Type)
	assert.Equal(t, "json", conf.Logger.Format)
}
