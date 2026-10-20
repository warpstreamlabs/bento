package cli_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	icli "github.com/warpstreamlabs/bento/internal/cli"
	"github.com/warpstreamlabs/bento/internal/cli/common"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestRunCLI(t *testing.T) {
	tmpDir := t.TempDir()
	tests := map[string]struct {
		configFormatString string
		envVars            map[string]string
	}{
		"config file": {
			configFormatString: `input:
  generate:
    mapping: 'root.id = "foobar"'
    interval: "100ms"
output:
  file:
    codec: lines
    path: %v`,
			envVars: map[string]string{},
		},
		"config file with env var interpolation": {
			configFormatString: `input:
  generate:
    mapping: 'root.id = "${FOO}"'
    interval: "100ms"
output:
  file:
    codec: lines
    path: %v`,
			envVars: map[string]string{"FOO": "foobar"},
		},
		// check config file has precedence over BENTO_CONFIG env var
		"config file and BENTO_CONFIG env var": {
			configFormatString: `input:
  generate:
    mapping: 'root.id = "foobar"'
    interval: "100ms"
output:
  file:
    codec: lines
    path: %v`,
			envVars: map[string]string{"BENTO_CONFIG": `{"pipeline":{"processors":["mapping":"root.message = this.message.uppercase()"]}}`},
		},
		"BENTO_CONFIG env var": {
			configFormatString: "",
			envVars:            map[string]string{"BENTO_CONFIG": `{"input":{"generate": {"mapping": "root.id = \"foobar\"","interval": "100ms"}},"output": {"file": {"codec": "lines","path": "<PATH>"}}}`},
		},
		"BENTO_CONFIG env var with env var interpolation": {
			configFormatString: "",
			envVars:            map[string]string{"BENTO_CONFIG": `{"input":{"generate": {"mapping": "root.id = \"${FOO}\"","interval": "100ms"}},"output": {"file": {"codec": "lines","path": "<PATH>"}}}`, "FOO": "foobar"},
		},
	}

	for i, test := range tests {
		outPath := filepath.Join(tmpDir, fmt.Sprintf("out_%v.txt", i))

		var confPath string
		if test.configFormatString != "" {
			confPath = filepath.Join(tmpDir, fmt.Sprintf("conf_%v.yaml", i))
			err := os.WriteFile(confPath, fmt.Appendf(nil, test.configFormatString, outPath), 0o644)
			require.NoError(t, err)
		}

		for k, v := range test.envVars {
			if k == "BENTO_CONFIG" {
				v = strings.ReplaceAll(v, "<PATH>", outPath)
			}
			t.Setenv(k, v)
		}

		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
		defer cancel()

		if confPath != "" {
			require.NoError(t, icli.App(common.NewCLIOpts("1.2.3", "aaa")).RunContext(ctx, []string{"bento", "-c", confPath}))
		} else {
			require.NoError(t, icli.App(common.NewCLIOpts("1.2.3", "aaa")).RunContext(ctx, []string{"bento"}))
		}
		data, err := os.ReadFile(outPath)
		require.NoError(t, err)
		assert.Contains(t, string(data), "foobar")
	}
}

// gracefulCancelChildEnv, when set in the environment, signals that this test
// binary has been re-executed as a child process to exercise
// TestRunCLIGracefulCancel's scenario in isolation. This lets the scenario
// freely call code paths that may call os.Exit, without risking the exit of
// the parent `go test` process.
const gracefulCancelChildEnv = "BENTO_TEST_GRACEFUL_CANCEL_CONF"

// TestRunCLIGracefulCancel proves that cancelling the context passed to
// RunCLI/RunContext (as opposed to letting it reach a deadline, or sending a
// termination signal) still allows the running stream to shut down
// gracefully, rather than failing shutdown immediately and forcing the
// process to exit uncleanly.
//
// Without the fix to RunManagerUntilStopped's deferred cleanup, the cleanup
// context is derived from the already-cancelled run context, so
// stopStrm.Stop(ctx) fails instantly (its internal waits select on an
// already-closed ctx.Done()) and the child process below is terminated via
// os.Exit(1) instead of shutting down gracefully.
func TestRunCLIGracefulCancel(t *testing.T) {
	if confPath := os.Getenv(gracefulCancelChildEnv); confPath != "" {
		// We are the re-executed child: run the CLI with a context that
		// gets cancelled shortly after the stream starts running, then let
		// the process exit code reflect whether shutdown was graceful
		// (0) or not (RunManagerUntilStopped's cleanup calls os.Exit(1)
		// on failure).
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		_ = icli.App(common.NewCLIOpts("1.2.3", "aaa")).RunContext(ctx, []string{"bento", "-c", confPath})
		return
	}

	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "out.txt")
	confPath := filepath.Join(tmpDir, "conf.yaml")

	conf := fmt.Sprintf(`input:
  generate:
    mapping: 'root.id = "foobar"'
    interval: "10ms"
output:
  file:
    codec: lines
    path: %v
`, outPath)
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0o644))

	cmd := exec.Command(os.Args[0], "-test.run=^TestRunCLIGracefulCancel$", "-test.v")
	cmd.Env = append(os.Environ(), gracefulCancelChildEnv+"="+confPath)

	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, "a cancelled run context should let the stream stop gracefully instead of exiting uncleanly; child output:\n%s", out)
}
