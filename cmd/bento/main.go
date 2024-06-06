package main

import (
	"context"

	"github.com/warpstreamlabs/bento/v1/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/v1/public/components/all"
)

func main() {
	service.RunCLI(context.Background())
}
