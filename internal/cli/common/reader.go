package common

import (
	"fmt"
	"os"
	"strings"

	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"

	"github.com/urfave/cli/v2"
)

// ReadConfig attempts to read a general service wide config via a returned
// config.Reader based on input CLI flags. This includes applying any config
// overrides expressed by the --set flag.
func ReadConfig(c *cli.Context, cliOpts *CLIOpts, streamsMode bool) (mainPath string, inferred bool, conf *config.Reader) {
	path := c.String("config")
	if path == "" {
		// Iterate default config paths
		for _, dpath := range cliOpts.ConfigSearchPaths {
			if _, err := ifs.OS().Stat(dpath); err == nil {
				inferred = true
				path = dpath
				break
			}
		}
	}

	if strings.HasPrefix(path, "https://") && c.Bool("watcher") {
		fmt.Fprintln(os.Stderr, "error: --watcher is not supported with a remote config URL")
		os.Exit(1)
	}

	opts := []config.OptFunc{
		config.OptSetFullSpec(cliOpts.MainConfigSpecCtor),
		config.OptAddOverrides(c.StringSlice("set")...),
		config.OptTestSuffix("_bento_test"),
		config.OptSetLintConfigWarnDeprecated(),
		config.OptSetConfigHeaders(c.StringSlice("config-header")),
	}
	if streamsMode {
		opts = append(opts, config.OptSetStreamPaths(c.Args().Slice()...))
	}
	return path, inferred, config.NewReader(path, c.StringSlice("resources"), opts...)
}
