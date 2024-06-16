package main

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

var (
	// Version version set at compile time.
	Version string
	// DateBuilt date built set at compile time.
	DateBuilt string
	// BinaryName binary name.
	BinaryName string = "bento"
)

func main() {
	service.RunCLI(
		context.Background(),
		service.CLIOptSetVersion(Version, DateBuilt),
		service.CLIOptSetBinaryName(BinaryName),
		service.CLIOptSetProductName("Bento"),
		service.CLIOptSetDocumentationURL("https://warpstreamlabs.github.io/bento/docs"),
		service.CLIOptSetShowRunCommand(true),
	)
}
