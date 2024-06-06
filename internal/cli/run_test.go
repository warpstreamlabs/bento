package cli_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	icli "github.com/warpstreamlabs/bento/v1/internal/cli"
	"github.com/warpstreamlabs/bento/v1/internal/cli/common"

	_ "github.com/warpstreamlabs/bento/v1/public/components/io"
	_ "github.com/warpstreamlabs/bento/v1/public/components/pure"
)

func TestRunCLIShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "foo.yaml")
	outPath := filepath.Join(tmpDir, "out.txt")

	require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
input:
  generate:
    mapping: 'root.id = "foobar"'
    interval: "100ms"
output:
  file:
    codec: lines
    path: %v
`, outPath), 0o644))

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()

	require.NoError(t, icli.App(common.NewCLIOpts("1.2.3", "aaa")).RunContext(ctx, []string{"bento", "-c", confPath}))

	data, _ := os.ReadFile(outPath)
	assert.Contains(t, string(data), "foobar")
}
