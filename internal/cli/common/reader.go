package common

import (
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
	opts := []config.OptFunc{
		config.OptSetFullSpec(cliOpts.MainConfigSpecCtor),
		config.OptAddOverrides(c.StringSlice("set")...),
		config.OptTestSuffix("_bento_test"),
		config.OptSetLintConfigWarnDeprecated(),
	}
	if streamsMode {
		opts = append(opts, config.OptSetStreamPaths(c.Args().Slice()...))
	}
	return path, inferred, config.NewReader(path, c.StringSlice("resources"), opts...)
}
