package genkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestOllamaEmbedProcessorConfigSpec(t *testing.T) {
	spec := ollamaEmbedProcessorConfigSpec()
	require.NotNil(t, spec)

	validConfig := `
address: "http://localhost:11434"
model: "all-minilm"
`
	conf, err := spec.ParseYAML(validConfig, service.NewEnvironment())
	require.NoError(t, err)

	address, err := conf.FieldURL("address")
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:11434", address.String())

	model, err := conf.FieldString("model")
	require.NoError(t, err)
	assert.Equal(t, "all-minilm", model)
}
