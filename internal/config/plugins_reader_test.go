package config

import (
	"errors"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin"
)

const pluginsMainConfig = `
plugins:
  - source: ./my_plugin
pipeline:
  processors:
    - hook_proc: {}
`

func TestReaderOnMainReadRunsBeforeLint(t *testing.T) {
	env := bundle.GlobalEnvironment.Clone()

	var calls int
	var sources []string
	onRead := func(root *yaml.Node) ([]string, []string, error) {
		calls++
		entries, err := plugin.EntriesFromConfig(root)
		if err != nil {
			return nil, nil, err
		}
		for _, e := range entries {
			sources = append(sources, e.Source)
		}
		// Register a processor, as loading a plugin would.
		if _, exists := env.GetDocs("hook_proc", docs.TypeProcessor); !exists {
			require.NoError(t, env.ProcessorAdd(func(processor.Config, bundle.NewManagement) (processor.V1, error) {
				return nil, errors.New("not implemented")
			}, docs.ComponentSpec{Name: "hook_proc", Type: docs.TypeProcessor, Config: docs.FieldComponent()}))
		}
		return []string{"hook lint"}, []string{"hook warning"}, nil
	}

	rdr := NewReader("main.yaml", nil,
		OptUseFS(testFS{m: fstest.MapFS{"main.yaml": {Data: []byte(pluginsMainConfig)}}}),
		OptSetLintConfig(docs.NewLintConfig(env)),
		OptOnMainRead(onRead),
	)

	_, _, lints, warns, err := rdr.Read()
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Equal(t, []string{"./my_plugin"}, sources)

	// The processor registered by the hook is known when linting.
	assert.Equal(t, []string{"hook lint"}, lints)
	assert.Equal(t, []string{"hook warning"}, warns)
}

func TestReaderOnMainReadError(t *testing.T) {
	rdr := NewReader("main.yaml", nil,
		OptUseFS(testFS{m: fstest.MapFS{"main.yaml": {Data: []byte(pluginsMainConfig)}}}),
		OptOnMainRead(func(*yaml.Node) ([]string, []string, error) {
			return nil, nil, errors.New("plugin went bang")
		}),
	)

	_, _, _, _, err := rdr.Read()
	require.ErrorContains(t, err, "plugin went bang")
	assert.Contains(t, err.Error(), "main.yaml")
}

func TestReaderPluginsOnlyInMainConfig(t *testing.T) {
	rdr := NewReader("main.yaml", []string{"res.yaml"},
		OptUseFS(testFS{m: fstest.MapFS{
			"main.yaml": {Data: []byte("plugins: []\n")},
			"res.yaml":  {Data: []byte("plugins:\n  - source: ./my_plugin\n")},
		}}),
	)

	_, _, lints, _, err := rdr.Read()
	require.NoError(t, err)
	require.Len(t, lints, 1)
	assert.Contains(t, lints[0], "res.yaml")
	assert.Contains(t, lints[0], "plugins")
}
