package gcp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

const yamlConfig = `
    bucket: bkt
    prefix: ${!2 * 2}
    scanner:
      to_the_end: { }`

func gcpReaderConfig(t *testing.T, yamlStr string) csiConfig {
	t.Helper()
	spec := csiSpec()
	conf, err := spec.ParseYAML(yamlStr, service.GlobalEnvironment())
	require.NoError(t, err)

	ret, err := csiConfigFromParsed(conf)
	require.NoError(t, err)

	return ret
}

func Test_csiConfigFromParsed(t *testing.T) {
	cfg := gcpReaderConfig(t, yamlConfig)

	pre, err := cfg.Prefix.TryString(service.NewMessage([]byte{}))

	require.NoError(t, err)

	require.Equal(t, "4", pre)
}
