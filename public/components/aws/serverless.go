package aws

import (
	"context"

	"github.com/warpstreamlabs/bento/v4/internal/serverless/lambda"
)

// RunLambda executes Bento as an AWS Lambda function. Configuration can be
// stored within the environment variable BENTO_CONFIG.
func RunLambda(ctx context.Context) {
	lambda.Run()
}
