package main

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

func main() {
	service.RunCLI(context.Background())
}
