package main

import (
	"github.com/warpstreamlabs/bento/internal/serverless/lambda"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

func main() {
	lambda.Run()
}
