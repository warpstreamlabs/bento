package template

import (
	"errors"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/v1/internal/docs"
	ifilepath "github.com/warpstreamlabs/bento/v1/internal/filepath"
	"github.com/warpstreamlabs/bento/v1/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/v1/internal/template"
)

var (
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
)

type pathLint struct {
	source string
	lint   docs.Lint
}

func lintFile(path string) (pathLints []pathLint) {
	conf, lints, err := template.ReadConfigFile(path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   docs.NewLintError(1, docs.LintFailedRead, err),
		})
		return
	}

	for _, l := range lints {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   l,
		})
	}

	testErrors, err := conf.Test()
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   docs.NewLintError(1, docs.LintFailedRead, err),
		})
		return
	}

	for _, tErr := range testErrors {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   docs.NewLintError(1, docs.LintFailedRead, errors.New(tErr)),
		})
	}
	return
}

func lintCliCommand() *cli.Command {
	return &cli.Command{
		Name:  "lint",
		Usage: "Parse Bento templates and report any linting errors",
		Description: `
Exits with a status code 1 if any linting errors are detected:

  bento template lint
  bento template lint ./templates/*.yaml
  bento template lint ./foo.yaml ./bar.yaml
  bento template lint ./templates/...

If a path ends with '...' then Bento will walk the target and lint any
files with the .yaml or .yml extension.`[1:],
		Action: func(c *cli.Context) error {
			targets, err := ifilepath.GlobsAndSuperPaths(ifs.OS(), c.Args().Slice(), "yaml", "yml")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Lint paths error: %v\n", err)
				os.Exit(1)
			}
			var pathLints []pathLint
			for _, target := range targets {
				if target == "" {
					continue
				}
				lints := lintFile(target)
				if len(lints) > 0 {
					pathLints = append(pathLints, lints...)
				}
			}
			if len(pathLints) == 0 {
				os.Exit(0)
			}
			for _, lint := range pathLints {
				lintText := fmt.Sprintf("%v%v\n", lint.source, lint.lint.Error())
				if lint.lint.Type == docs.LintFailedRead {
					fmt.Fprint(os.Stderr, red(lintText))
				} else {
					fmt.Fprint(os.Stderr, yellow(lintText))
				}
			}
			os.Exit(1)
			return nil
		},
	}
}
