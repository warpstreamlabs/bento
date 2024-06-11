package service

import (
	"context"
	"log/slog"
	"os"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/cli"
	"github.com/warpstreamlabs/bento/internal/cli/common"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/log"
)

// RunCLI executes Bento as a CLI, allowing users to specify a configuration
// file path(s) and execute subcommands for linting configs, testing configs,
// etc. This is how a standard distribution of Bento operates.
//
// This call blocks until either:
//
// 1. The service shuts down gracefully due to the inputs closing
// 2. A termination signal is received
// 3. The provided context has a deadline that is reached, triggering graceful termination
// 4. The provided context is cancelled (WARNING, this prevents graceful termination)
//
// This function must only be called once during the entire lifecycle of your
// program, as it interacts with singleton state. In order to manage multiple
// Bento stream lifecycles in a program use the StreamBuilder API instead.
func RunCLI(ctx context.Context, optFuncs ...CLIOptFunc) {
	cliOpts := &CLIOptBuilder{
		opts: common.NewCLIOpts(cli.Version, cli.DateBuilt),
	}
	for _, o := range optFuncs {
		o(cliOpts)
	}
	cliOpts.opts.OnLoggerInit = func(l log.Modular) (log.Modular, error) {
		if cliOpts.outLoggerFn != nil {
			cliOpts.outLoggerFn(&Logger{m: l})
		}
		if cliOpts.teeLogger != nil {
			return log.TeeLogger(l, log.NewBentoLogAdapter(cliOpts.teeLogger)), nil
		}
		return l, nil
	}
	_ = cli.App(cliOpts.opts).RunContext(ctx, os.Args)
}

type CLIOptBuilder struct {
	opts        *common.CLIOpts
	teeLogger   *slog.Logger
	outLoggerFn func(*Logger)
}

// CLIOptFunc defines an option to pass through the standard Bento CLI in order
// to customise it's behaviour.
type CLIOptFunc func(*CLIOptBuilder)

// CLIOptSetVersion overrides the default version and date built stamps.
func CLIOptSetVersion(version, dateBuilt string) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.Version = version
		c.opts.DateBuilt = dateBuilt
	}
}

// CLIOptSetBinaryName overrides the default binary name in CLI help docs.
func CLIOptSetBinaryName(n string) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.BinaryName = n
	}
}

// CLIOptSetProductName overrides the default product name in CLI help docs.
func CLIOptSetProductName(n string) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.ProductName = n
	}
}

// CLIOptSetDocumentationURL overrides the default documentation URL in CLI help
// docs.
func CLIOptSetDocumentationURL(n string) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.DocumentationURL = n
	}
}

// CLIOptSetShowRunCommand determines whether a `run` subcommand should appear
// in CLI help and autocomplete.
func CLIOptSetShowRunCommand(show bool) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.ShowRunCommand = show
	}
}

// CLIOptSetDefaultConfigPaths overrides the default paths used for detecting
// and loading config files when one was not provided explicitly with the
// --config flag.
func CLIOptSetDefaultConfigPaths(paths ...string) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.ConfigSearchPaths = paths
	}
}

// CLIOptOnLoggerInit sets a closure to be called when the service-wide logger
// is initialised. A modified version can be returned, allowing you to mutate
// the fields and settings that it has.
func CLIOptOnLoggerInit(fn func(*Logger)) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.outLoggerFn = fn
	}
}

// CLIOptAddTeeLogger adds another logger to receive all log events from the
// service initialised via the CLI.
func CLIOptAddTeeLogger(l *slog.Logger) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.teeLogger = l
	}
}

// CLIOptSetMainSchemaFrom overrides the default Bento configuration schema
// for another. A constructor is provided such that downstream components can
// still modify copies of the schema when needed.
//
// NOTE: This transfers the configuration schema but NOT the Environment plugins
// themselves, which is the global set by default.
func CLIOptSetMainSchemaFrom(fn func() *ConfigSchema) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.MainConfigSpecCtor = func() docs.FieldSpecs {
			return fn().fields
		}
	}
}

// CLIOptOnConfigParsed sets a closure function to be called when a main
// configuration file load has occurred.
//
// If an error is returned this will be treated by the CLI the same as any other
// failure to parse the bootstrap config.
func CLIOptOnConfigParse(fn func(fn *ParsedConfig) error) CLIOptFunc {
	return func(c *CLIOptBuilder) {
		c.opts.OnManagerInitialised = func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error {
			return fn(&ParsedConfig{
				i:   pConf,
				mgr: mgr,
			})
		}
	}
}
