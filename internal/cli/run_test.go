package cli_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

var capitalise string = `
input:
  generate:
    mapping: 'root.id = "foobar"'
    interval: "100ms"

pipeline:
  processors:
    - mapping: root = content().uppercase()

output:
  file:
    codec: lines
    path: %v
`

var fetchConfig string = `url: %v/capitalise
verb: GET
timeout: 5s`

func TestRunCLIGetConfigViaHttp(t *testing.T) {
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, fmt.Sprintf("out_%v.txt", 1))
	addr := startConfigServer(t, capitalise, outPath)

	confPath := filepath.Join(tmpDir, fmt.Sprintf("conf_%v", 1))
	err := os.WriteFile(confPath, fmt.Appendf(nil, fetchConfig, addr), 0o644)
	require.NoError(t, err)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()

	require.NoError(t, icli.App(common.NewCLIOpts("1.2.3", "aaa")).RunContext(ctx, []string{"bento", "-rc", confPath}))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(data), "FOOBAR")
}

func startConfigServer(t *testing.T, config, outPath string) string {
	t.Helper()

	config = fmt.Sprintf(config, outPath)

	mux := http.NewServeMux()
	mux.HandleFunc("/capitalise", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, config)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv.URL
}
