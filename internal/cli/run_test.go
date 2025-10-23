package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	icli "github.com/warpstreamlabs/bento/internal/cli"
	"github.com/warpstreamlabs/bento/internal/cli/common"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
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

// implements a Bento Processor
type proc struct{}

func (f proc) Close(ctx context.Context) error {
	return nil
}

func (f proc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	return service.MessageBatch{msg}, nil
}

func TestRunCLIWarningsFromConfigFile(t *testing.T) {
	tests := map[string]struct {
		configSpec    *service.ConfigSpec
		processorName string
		cliOpts       []string
		expectWarning bool
		level         string
	}{
		"allow-experimental": {
			configSpec:    service.NewConfigSpec(), // status experimental is default
			processorName: "allow_experimental",
			cliOpts:       []string{"--allow-experimental"},
			expectWarning: false,
			level:         "experimental",
		},
		"experimental-warning": {
			configSpec:    service.NewConfigSpec(),
			processorName: "experimental_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "experimental",
		},
		"allow-beta": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "allow_beta",
			cliOpts:       []string{"--allow-beta"},
			expectWarning: false,
			level:         "beta",
		},
		"beta-warning": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "beta_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "beta",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := service.RegisterProcessor(
				test.processorName,
				test.configSpec,
				func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
					return proc{}, nil
				},
			)
			require.NoError(t, err)

			tmpDir := t.TempDir()
			confPath := filepath.Join(tmpDir, fmt.Sprintf("%v.yaml", name))
			outPath := filepath.Join(tmpDir, fmt.Sprintf("%v-out.txt", name))

			require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
input:
  generate: 
    mapping: 'root.id = "foobar"'
    count: 1
  
pipeline:
  processors: 
    - %v: {}
  
output:
  file:
    path: %v
`, test.processorName, outPath), 0o644))

			originalStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = originalStdout
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			require.NoError(t, icli.App(common.NewCLIOpts("", "")).RunContext(ctx, append([]string{"bento", "-c", confPath}, test.cliOpts...)))

			time.Sleep(100 * time.Millisecond)

			var buf bytes.Buffer
			w.Close()
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			data, _ := os.ReadFile(outPath)
			assert.Contains(t, string(data), "foobar")

			warningLog := fmt.Sprintf("processor %v is %v; silence warning with --allow-%v", test.processorName, test.level, test.level)
			if test.expectWarning {
				assert.Contains(t, buf.String(), warningLog)
			} else {
				assert.NotContains(t, buf.String(), warningLog)
			}
		})
	}
}

func TestRunCLIWarningsFromConfigFileStreamMode(t *testing.T) {
	tests := map[string]struct {
		configSpec    *service.ConfigSpec
		processorName string
		cliOpt        string
		expectWarning bool
		level         string
	}{
		"allow-experimental": {
			configSpec:    service.NewConfigSpec(), // status experimental is default
			processorName: "allow_experimental",
			cliOpt:        "--allow-experimental",
			expectWarning: false,
			level:         "experimental",
		},
		"experimental-warning": {
			configSpec:    service.NewConfigSpec(),
			processorName: "experimental_warning",
			cliOpt:        "",
			expectWarning: true,
			level:         "experimental",
		},
		"allow-beta": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "allow_beta",
			cliOpt:        "--allow-beta",
			expectWarning: false,
			level:         "beta",
		},
		"beta-warning": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "beta_warning",
			cliOpt:        "",
			expectWarning: true,
			level:         "beta",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := service.RegisterProcessor(
				test.processorName,
				test.configSpec,
				func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
					return proc{}, nil
				},
			)
			require.NoError(t, err)

			tmpDir := t.TempDir()
			confPath := filepath.Join(tmpDir, fmt.Sprintf("%v.yaml", name))
			outPath := filepath.Join(tmpDir, fmt.Sprintf("%v-out.txt", name))

			require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
input:
  generate: 
    mapping: 'root.id = "foobar"'
    count: 1
  
pipeline:
  processors: 
    - %v: {}
  
output:
  file:
    path: %v
`, test.processorName, outPath), 0o644))

			originalStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = originalStdout
			}()

			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
			defer cancel()

			errChan := make(chan error, 1)
			go func() {
				errChan <- icli.App(common.NewCLIOpts("", "")).RunContext(ctx, []string{"bento", "streams", test.cliOpt, confPath})
			}()

			time.Sleep(2 * time.Second)

			cancel()

			select {
			case err := <-errChan:
				if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
					t.Fatalf("unexpected error: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("app did not stop after context cancellation")
			}

			var buf bytes.Buffer
			w.Close()
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			data, _ := os.ReadFile(outPath)
			assert.Contains(t, string(data), "foobar")

			warningLog := fmt.Sprintf("processor %v is %v; silence warning with --allow-%v", test.processorName, test.level, test.level)
			if test.expectWarning {
				assert.Contains(t, buf.String(), warningLog)
			} else {
				assert.NotContains(t, buf.String(), warningLog)
			}
		})
	}
}

func TestRunCLIWarningsFromResourceFile(t *testing.T) {
	tests := map[string]struct {
		configSpec    *service.ConfigSpec
		processorName string
		cliOpts       []string
		expectWarning bool
		level         string
	}{
		"allow-experimental": {
			configSpec:    service.NewConfigSpec(), // status experimental is default
			processorName: "allow_experimental",
			cliOpts:       []string{"--allow-experimental"},
			expectWarning: false,
			level:         "experimental",
		},
		"experimental-warning": {
			configSpec:    service.NewConfigSpec(),
			processorName: "experimental_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "experimental",
		},
		"allow-beta": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "allow_beta",
			cliOpts:       []string{"--allow-beta"},
			expectWarning: false,
			level:         "beta",
		},
		"beta-warning": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "beta_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "beta",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := service.RegisterProcessor(
				test.processorName,
				test.configSpec,
				func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
					return proc{}, nil
				},
			)
			require.NoError(t, err)

			tmpDir := t.TempDir()
			confPath := filepath.Join(tmpDir, fmt.Sprintf("%v.yaml", name))
			resPath := filepath.Join(tmpDir, fmt.Sprintf("%v-resource.yaml", name))
			outPath := filepath.Join(tmpDir, fmt.Sprintf("%v-out.txt", name))

			require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
input:
  generate: 
    mapping: 'root.id = "foobar"'
    count: 1
  
pipeline:
  processors: 
    - resource: bar
  
output:
  file:
    path: %v
`, outPath), 0o644))

			require.NoError(t, os.WriteFile(resPath, fmt.Appendf(nil, `
processor_resources:
  - label: bar
    %v: {}
`, test.processorName), 0o644))

			originalStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = originalStdout
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			require.NoError(t, icli.App(common.NewCLIOpts("", "")).RunContext(ctx, append([]string{"bento", "-r", resPath, "-c", confPath}, test.cliOpts...)))

			time.Sleep(100 * time.Millisecond)

			var buf bytes.Buffer
			w.Close()
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			data, _ := os.ReadFile(outPath)
			assert.Contains(t, string(data), "foobar")

			warningLog := fmt.Sprintf("processor %v is %v; silence warning with --allow-%v", test.processorName, test.level, test.level)
			if test.expectWarning {
				assert.Contains(t, buf.String(), warningLog)
			} else {
				assert.NotContains(t, buf.String(), warningLog)
			}
		})
	}
}

func TestRunCLIWarningsLint(t *testing.T) {
	tests := map[string]struct {
		configSpec    *service.ConfigSpec
		processorName string
		cliOpts       []string
		expectWarning bool
		level         string
	}{
		"allow-experimental": {
			configSpec:    service.NewConfigSpec(), // status experimental is default
			processorName: "allow_experimental",
			cliOpts:       []string{"--allow-experimental"},
			expectWarning: false,
			level:         "experimental",
		},
		"experimental-warning": {
			configSpec:    service.NewConfigSpec(),
			processorName: "experimental_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "experimental",
		},
		"allow-beta": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "allow_beta",
			cliOpts:       []string{"--allow-beta"},
			expectWarning: false,
			level:         "beta",
		},
		"beta-warning": {
			configSpec:    service.NewConfigSpec().Beta(),
			processorName: "beta_warning",
			cliOpts:       []string{},
			expectWarning: true,
			level:         "beta",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := service.RegisterProcessor(
				test.processorName,
				test.configSpec,
				func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
					return proc{}, nil
				},
			)
			require.NoError(t, err)

			tmpDir := t.TempDir()
			confPath := filepath.Join(tmpDir, fmt.Sprintf("%v.yaml", name))

			require.NoError(t, os.WriteFile(confPath, fmt.Appendf(nil, `
input:
  stdin: {}
  
pipeline:
  processors: 
    - %v: {}
  
output:
  stdout: {}
`, test.processorName), 0o644))

			originalStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = originalStdout
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			require.NoError(t, icli.App(common.NewCLIOpts("", "")).RunContext(ctx, append([]string{"bento", "-c", confPath, "lint"}, test.cliOpts...)))

			time.Sleep(100 * time.Millisecond)

			var buf bytes.Buffer
			w.Close()
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			r.Close()

			warningLog := fmt.Sprintf("processor %v is %v; silence warning with --allow-%v", test.processorName, test.level, test.level)
			if test.expectWarning {
				assert.Contains(t, buf.String(), warningLog)
			} else {
				assert.NotContains(t, buf.String(), warningLog)
			}
		})
	}
}
