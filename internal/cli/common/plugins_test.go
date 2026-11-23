package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPluginsOnMainReadWarnsOnChange(t *testing.T) {
	parse := func(s string) *yaml.Node {
		var n yaml.Node
		require.NoError(t, yaml.Unmarshal([]byte(s), &n))
		return &n
	}

	onRead := PluginsOnMainRead()

	_, warnings, err := onRead(parse("plugins: []\ninput:\n  stdin: {}\n"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	// Re-reading with an unchanged plugins section is fine.
	_, warnings, err = onRead(parse("plugins: []\ninput:\n  generate: {}\n"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	// Changes are not applied, as plugins can't be unloaded.
	_, warnings, err = onRead(parse("plugins:\n  - source: ./does-not-exist\n"))
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "restart Bento")
}
