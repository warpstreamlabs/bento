package common_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/cli/common"
)

// TestRunServiceErrWriter proves that an error raised before the service
// logger exists (here: a config file that fails to load) is written to the
// CLIOpts.ErrWriter rather than being hard-wired to os.Stderr, so that a
// program embedding the CLI (via public/service.RunCLI and
// CLIOptSetErrWriter) can capture it.
//
// RunService is exercised directly, rather than through the full CLI App,
// because the App's own Action wraps RunService's non-zero return code in
// an os.Exit call, which would make in-process testing of this path
// impossible.
func TestRunServiceErrWriter(t *testing.T) {
	cliOpts := common.NewCLIOpts("1.2.3", "aaa")

	var errBuf bytes.Buffer
	cliOpts.ErrWriter = &errBuf

	// A path that does not exist, so config loading fails before the
	// logger is created.
	badConfigPath := filepath.Join(t.TempDir(), "does-not-exist.yaml")

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config", Aliases: []string{"c"}},
			&cli.StringSliceFlag{Name: "set", Aliases: []string{"s"}},
			&cli.StringSliceFlag{Name: "resources", Aliases: []string{"r"}},
			&cli.BoolFlag{Name: "chilled"},
			&cli.BoolFlag{Name: "watcher", Aliases: []string{"w"}},
		},
	}

	var code int
	app.Action = func(c *cli.Context) error {
		code = common.RunService(c, cliOpts, false)
		return nil
	}

	require.NoError(t, app.Run([]string{"bento", "-c", badConfigPath}))

	assert.Equal(t, 1, code)
	assert.Contains(t, errBuf.String(), "Configuration file read error")
}
