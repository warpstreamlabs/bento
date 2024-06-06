package studio

import (
	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/v1/internal/cli/common"
)

// CliCommand is a cli.Command definition for interacting with Benthos studio.
func CliCommand(cliOpts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "studio",
		Usage: "Interact with Benthos studio (https://studio.benthos.dev)",
		Description: `
EXPERIMENTAL: This subcommand is experimental and therefore are subject to
change outside of major version releases.`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoint",
				Aliases: []string{"e"},
				Value:   "https://studio.benthos.dev",
				Usage:   "Specify the URL of the Bento studio server to connect to.",
			},
		},
		Subcommands: []*cli.Command{
			syncSchemaCommand(cliOpts),
			pullCommand(cliOpts),
		},
	}
}
