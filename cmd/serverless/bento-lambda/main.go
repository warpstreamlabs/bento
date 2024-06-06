package main

import (
	"github.com/warpstreamlabs/bento/v1/internal/serverless/lambda"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/v1/public/components/all"
)

func main() {
	lambda.Run()
}
