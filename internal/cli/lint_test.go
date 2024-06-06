package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	icli "github.com/warpstreamlabs/bento/internal/cli"
	"github.com/warpstreamlabs/bento/internal/cli/common"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func executeLintSubcmd(t *testing.T, args []string) (exitCode int, printedErr string) {
	opts := common.NewCLIOpts("1.2.3", "now")
	cliApp := icli.App(opts)
	for _, c := range cliApp.Commands {
		if c.Name == "lint" {
			c.Action = func(ctx *cli.Context) error {
				var buf bytes.Buffer
				exitCode = icli.LintAction(ctx, opts, &buf)
				printedErr = buf.String()
				return nil
			}
		}
	}
	require.NoError(t, cliApp.Run(args))
	return
}

func TestLints(t *testing.T) {
	tmpDir := t.TempDir()
	tFile := func(name string) string {
		return filepath.Join(tmpDir, name)
	}

	tests := []struct {
		name          string
		files         map[string]string
		args          []string
		expectedCode  int
		expectedLints []string
	}{
		{
			name: "one file no errors",
			args: []string{"bento", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = uuid_v4()'
output:
  drop: {}
`,
			},
		},
		{
			name: "one file unexpected fields",
			args: []string{"bento", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "one file with c flag",
			args: []string{"bento", "-c", tFile("foo.yaml"), "lint"},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "one file with r flag",
			args: []string{"bento", "-r", tFile("foo.yaml"), "lint"},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "env var missing",
			args: []string{"bento", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = "${BENTO_ENV_VAR_HOPEFULLY_MISSING}"'
output:
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"required environment variables were not set: [BENTO_ENV_VAR_HOPEFULLY_MISSING]",
			},
		},
		{
			name: "env var missing but we dont care",
			args: []string{"bento", "lint", "--skip-env-var-check", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = "${BENTO_ENV_VAR_HOPEFULLY_MISSING}"'
output:
  drop: {}
`,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for name, c := range test.files {
				require.NoError(t, os.WriteFile(tFile(name), []byte(c), 0o644))
			}

			code, outStr := executeLintSubcmd(t, test.args)
			assert.Equal(t, test.expectedCode, code)

			if len(test.expectedLints) == 0 {
				assert.Empty(t, outStr)
			} else {
				for _, l := range test.expectedLints {
					assert.Contains(t, outStr, l)
				}
			}
		})
	}
}
