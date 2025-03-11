package main

import (
	"context"

	// import all components with:
	// _ "github.com/warpstreamlabs/bento/public/components/all"

	// or you can import select components with:
	// _ "github.com/warpstreamlabs/bento/public/components/aws"
	// for example.

	// io + pure contain components such as stdin/stdout & mapping
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"

	"github.com/warpstreamlabs/bento/public/service"

	// import your plugins:
	_ "plugin_example/plugins"
)

func main() {
	// RunCLI accepts a number of optional functions:
	// https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service#CLIOptFunc
	service.RunCLI(context.Background())
}
