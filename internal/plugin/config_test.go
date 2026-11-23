package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

func parseYAML(t *testing.T, s string) *yaml.Node {
	t.Helper()
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte(s), &node))
	return &node
}

func TestEntriesFromConfig(t *testing.T) {
	root := parseYAML(t, `
input:
  stdin: {}
plugins:
  - source: ./a
  - source: ./b
    memory:
      max_pages: 512
    mounts:
      data:
        host_path: ./data
        writable: true
`)

	entries, err := EntriesFromConfig(root)
	require.NoError(t, err)
	assert.Equal(t, []Entry{
		{Source: "./a"},
		{Source: "./b", Grant: runtime.Grant{
			Memory: runtime.MemoryConfig{MaxPages: 512},
			Mounts: map[string]runtime.MountGrant{"data": {HostPath: "./data", Writable: true}},
		}},
	}, entries)
}

func TestEntriesFromConfigNoSection(t *testing.T) {
	entries, err := EntriesFromConfig(parseYAML(t, "input:\n  stdin: {}\n"))
	require.NoError(t, err)
	assert.Empty(t, entries)

	entries, err = EntriesFromConfig(nil)
	require.NoError(t, err)
	assert.Empty(t, entries)

	assert.Empty(t, ConfigSectionYAML(parseYAML(t, "input:\n  stdin: {}\n")))
}

func TestEntriesFromConfigInvalid(t *testing.T) {
	_, err := EntriesFromConfig(parseYAML(t, "plugins:\n  source: ./a\n"))
	require.ErrorContains(t, err, "plugins")
}
