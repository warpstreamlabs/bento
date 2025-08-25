package huggingface

import (
	"testing"

	"github.com/knights-analytics/hugot"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestHugotConfigParsing(t *testing.T) {
	// We need to ensure this (frankly overly complicated) path specification field
	// gets parsed and validated correctly when used in conjunction with downloads being enabled.
	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "local file",
			config: `
name: test-pipeline
path: /path/to/model.onnx
`,
			expectError: false,
		},
		{
			name: "download with file path should fail",
			config: `
name: test-pipeline
path: /path/to/model.onnx
enable_download: true
download_options:
  repository: foo/bar
`,
			expectError: true,
		},
		{
			name: "download without repository should fail",
			config: `
name: test-pipeline
path: /path/to/models/
enable_download: true
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := hugotConfigSpec().ParseYAML(tt.config, nil)
			require.NoError(t, err)

			mockResources := service.MockResources()
			mockResources.SetGeneric(sessionConstructorKey{}, func() (*hugot.Session, error) {
				return &hugot.Session{}, nil
			})
			_, err = newPipelineProcessor(conf, mockResources)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	config := `
name: test-pipeline
path: /path/to/models/
enable_download: true
download_options:
  repository: microsoft/DialoGPT-medium
`

	conf, err := hugotConfigSpec().ParseYAML(config, nil)
	require.NoError(t, err)

	download, err := conf.FieldBool("enable_download")
	require.NoError(t, err)
	require.True(t, download)

	onnxPath, err := conf.FieldString("download_options", "onnx_filepath")
	require.NoError(t, err)
	require.Equal(t, "model.onnx", onnxPath)
}
