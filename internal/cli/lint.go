package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"

	"github.com/fatih/color"
	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/cli/common"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	ifilepath "github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

var (
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
)

type pathLint struct {
	source string
	lint   docs.Lint
}

func lintFile(path string, skipEnvVarCheck bool, spec docs.FieldSpecs, lConf docs.LintConfig) (pathLints []pathLint) {
	_, lints, err := config.ReadYAMLFileLinted(ifs.OS(), spec, path, skipEnvVarCheck, lConf)
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
	return
}

func lintMDSnippets(path string, spec docs.FieldSpecs, lConf docs.LintConfig) (pathLints []pathLint) {
	rawBytes, err := ifs.ReadFile(ifs.OS(), path)
	if err != nil {
		pathLints = append(pathLints, pathLint{
			source: path,
			lint:   docs.NewLintError(1, docs.LintFailedRead, err),
		})
		return
	}

	startTag, endTag := []byte("```yaml"), []byte("```")

	nextSnippet := bytes.Index(rawBytes, startTag)
	for nextSnippet != -1 {
		nextSnippet += len(startTag)

		snippetLine := bytes.Count(rawBytes[:nextSnippet], []byte("\n")) + 1

		endOfSnippet := bytes.Index(rawBytes[nextSnippet:], endTag)
		if endOfSnippet == -1 {
			pathLints = append(pathLints, pathLint{
				source: path,
				lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, errors.New("markdown snippet not terminated")),
			})
			return
		}
		endOfSnippet = nextSnippet + endOfSnippet + len(endTag)

		configBytes := rawBytes[nextSnippet : endOfSnippet-len(endTag)]
		if nextSnippet = bytes.Index(rawBytes[endOfSnippet:], []byte("```yaml")); nextSnippet != -1 {
			nextSnippet += endOfSnippet
		}

		cNode, err := docs.UnmarshalYAML(configBytes)
		if err != nil {
			pathLints = append(pathLints, pathLint{
				source: path,
				lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, err),
			})
			continue
		}

		pConf, err := spec.ParsedConfigFromAny(cNode)
		if err != nil {
			var l docs.Lint
			if errors.As(err, &l) {
				l.Line += snippetLine - 1
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   l,
				})
			} else {
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, err),
				})
			}
		}

		if _, err := config.FromParsed(lConf.DocsProvider, pConf, nil); err != nil {
			var l docs.Lint
			if errors.As(err, &l) {
				l.Line += snippetLine - 1
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   l,
				})
			} else {
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   docs.NewLintError(snippetLine, docs.LintFailedRead, err),
				})
			}
		} else {
			for _, l := range spec.LintYAML(docs.NewLintContext(lConf), cNode) {
				l.Line += snippetLine - 1
				pathLints = append(pathLints, pathLint{
					source: path,
					lint:   l,
				})
			}
		}
	}
	return
}

func lintCliCommand(cliOpts *common.CLIOpts) *cli.Command {
	return &cli.Command{
		Name:  "lint",
		Usage: cliOpts.ExecTemplate("Parse {{.ProductName}} configs and report any linting errors"),
		Description: cliOpts.ExecTemplate(`
Exits with a status code 1 if any linting errors are detected:

  {{.BinaryName}} -c target.yaml lint
  {{.BinaryName}} lint ./configs/*.yaml
  {{.BinaryName}} lint ./foo.yaml ./bar.yaml
  {{.BinaryName}} lint ./configs/...

If a path ends with '...' then {{.ProductName}} will walk the target and lint any
files with the .yaml or .yml extension.`)[1:],
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "deprecated",
				Value: false,
				Usage: "Print linting errors for the presence of deprecated fields.",
			},
			&cli.BoolFlag{
				Name:  "labels",
				Value: false,
				Usage: "Print linting errors when components do not have labels.",
			},
			&cli.BoolFlag{
				Name:  "skip-env-var-check",
				Value: false,
				Usage: "Do not produce lint errors when environment interpolations exist without defaults within configs but aren't defined.",
			},
		},
		Action: func(c *cli.Context) error {
			if code := LintAction(c, cliOpts, os.Stderr); code != 0 {
				os.Exit(code)
			}
			return nil
		},
	}
}

// LintAction performs the bento lint subcommand and returns the appropriate
// exit code. This function is exported for testing purposes only.
func LintAction(c *cli.Context, opts *common.CLIOpts, stderr io.Writer) int {
	targets, err := ifilepath.GlobsAndSuperPaths(ifs.OS(), c.Args().Slice(), "yaml", "yml")
	if err != nil {
		fmt.Fprintf(stderr, "Lint paths error: %v\n", err)
		return 1
	}
	if conf := c.String("config"); conf != "" {
		targets = append(targets, conf)
	}
	targets = append(targets, c.StringSlice("resources")...)

	lConf := docs.NewLintConfig(bundle.GlobalEnvironment)
	lConf.RejectDeprecated = c.Bool("deprecated")
	lConf.WarnDeprecated = !lConf.RejectDeprecated
	lConf.RequireLabels = c.Bool("labels")
	skipEnvVarCheck := c.Bool("skip-env-var-check")

	spec := opts.MainConfigSpecCtor()

	var pathLintMut sync.Mutex
	var pathLints []pathLint
	threads := runtime.NumCPU()
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func(threadID int) {
			defer wg.Done()
			for j, target := range targets {
				if j%threads != threadID {
					continue
				}
				if target == "" {
					continue
				}
				var lints []pathLint
				if path.Ext(target) == ".md" {
					lints = lintMDSnippets(target, spec, lConf)
				} else {
					lints = lintFile(target, skipEnvVarCheck, spec, lConf)
				}
				if len(lints) > 0 {
					pathLintMut.Lock()
					pathLints = append(pathLints, lints...)
					pathLintMut.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()

	// filter out deprecated warnings and other lints
	var lintErrors []pathLint

	for _, lint := range pathLints {
		if lint.lint.Level == docs.LintWarning && lint.lint.Type == docs.LintDeprecated {
			fmt.Print(yellow(fmt.Sprintf("%v%v\n", lint.source, lint.lint.Error())))
		} else {
			lintErrors = append(lintErrors, lint)
		}
	}

	if len(lintErrors) == 0 {
		return 0
	}

	for _, lint := range pathLints {
		lintText := fmt.Sprintf("%v%v\n", lint.source, lint.lint.Error())
		if lint.lint.Type == docs.LintFailedRead || lint.lint.Type == docs.LintComponentMissing {
			fmt.Fprint(stderr, red(lintText))
		} else {
			fmt.Fprint(stderr, yellow(lintText))
		}
	}
	return 1
}
