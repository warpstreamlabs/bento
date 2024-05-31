package template

import (
	"github.com/urfave/cli/v2"
)

// CliCommand is a cli.Command definition for interacting with templates.
func CliCommand() *cli.Command {
	return &cli.Command{
		Name:  "template",
		Usage: "Interact and generate Bento templates",
		Description: `
EXPERIMENTAL: This subcommand, and templates in general, are experimental and
therefore are subject to change outside of major version releases.

Allows linting and generating Bento templates.

  bento template lint ./path/to/templates/...

For more information check out the docs at:
https://warpstreamlabs.github.io/bento/docs/configuration/templating`[1:],
		Subcommands: []*cli.Command{
			lintCliCommand(),
		},
	}
}
