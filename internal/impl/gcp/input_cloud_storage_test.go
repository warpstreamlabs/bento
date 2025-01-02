package gcp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

const yamlConfig = `
    bucket: bkt
    prefix: ${!a * 2}
    scanner:
      to_the_end: { }`

const msgBody = `{"a":1,"b":"asd"}`

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

	msg := service.NewMessage([]byte(msgBody))

	pre, err := cfg.Prefix.TryString(msg)

	require.NoError(t, err)

	require.Equal(t, "2", pre)
}
