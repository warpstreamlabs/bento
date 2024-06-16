package test

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/cli/common"
	"github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
)

// CliCommand is a cli.Command definition for unit testing.
func CliCommand(cliOpts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "test",
		Usage: cliOpts.ExecTemplate("Execute {{.ProductName}} unit tests"),
		Description: cliOpts.ExecTemplate(`
Execute any number of {{.ProductName}} unit test definitions. If one or more tests
fail the process will report the errors and exit with a status code 1.

  {{.BinaryName}} test ./path/to/configs/...
  {{.BinaryName}} test ./foo_configs/*.yaml ./bar_configs/*.yaml
  {{.BinaryName}} test ./foo.yaml

For more information check out the docs at:
{{.DocumentationURL}}/configuration/unit_testing`)[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log",
				Value: "",
				Usage: "allow components to write logs at a provided level to stdout.",
			},
		},
		Action: func(c *cli.Context) error {
			if len(c.StringSlice("set")) > 0 {
				fmt.Fprintln(os.Stderr, "Cannot override fields with --set (-s) during unit tests")
				os.Exit(1)
			}
			resourcesPaths := c.StringSlice("resources")
			var err error
			if resourcesPaths, err = filepath.Globs(ifs.OS(), resourcesPaths); err != nil {
				fmt.Printf("Failed to resolve resource glob pattern: %v\n", err)
				os.Exit(1)
			}
			if logLevel := c.String("log"); logLevel != "" {
				logConf := log.NewConfig()
				logConf.LogLevel = logLevel
				logger, err := log.New(os.Stdout, ifs.OS(), logConf)
				if err != nil {
					fmt.Printf("Failed to init logger: %v\n", err)
					os.Exit(1)
				}
				if RunAll(c.Args().Slice(), cliOpts.MainConfigSpecCtor(), "_bento_test", true, logger, resourcesPaths) {
					os.Exit(0)
				}
			} else if RunAll(c.Args().Slice(), cliOpts.MainConfigSpecCtor(), "_bento_test", true, log.Noop(), resourcesPaths) {
				os.Exit(0)
			}
			os.Exit(1)
			return nil
		},
	}
}
