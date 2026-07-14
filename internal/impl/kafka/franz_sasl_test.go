package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestSaslMechanismsFromConfig(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(saslField())

	tests := []struct {
		name string
		conf string
	}{
		{
			name: "PLAIN",
			conf: `
sasl:
  - mechanism: PLAIN
    username: foo
    password: bar
`,
		},
		{
			name: "OAUTHBEARER static",
			conf: `
sasl:
  - mechanism: OAUTHBEARER
    token: foo
`,
		},
		{
			name: "OAUTHBEARER oauth2",
			conf: `
sasl:
  - mechanism: OAUTHBEARER
    oauth2:
      enabled: true
      client_key: foo
      client_secret: bar
      token_url: http://localhost/token
`,
		},
		{
			name: "SCRAM-SHA-256",
			conf: `
sasl:
  - mechanism: SCRAM-SHA-256
    username: foo
    password: bar
`,
		},
		{
			name: "SCRAM-SHA-512",
			conf: `
sasl:
  - mechanism: SCRAM-SHA-512
    username: foo
    password: bar
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := saslConf.ParseYAML(test.conf, nil)
			require.NoError(t, err)

			mechanisms, err := saslMechanismsFromConfig(pConf)
			require.NoError(t, err)
			assert.Len(t, mechanisms, 1)
			assert.NotNil(t, mechanisms[0])

			// We don't easily test the underlying mechanism details without brittle casting,
			// but we can at least check if it doesn't panic when calling Name() or Authenticate.
			// However, franz-go mechanisms are mostly opaque.
		})
	}
}

func TestSaslMechanismsFromConfigMultiple(t *testing.T) {
	saslConf := service.NewConfigSpec().Field(saslField())
	pConf, err := saslConf.ParseYAML(`
sasl:
  - mechanism: PLAIN
    username: foo
    password: bar
  - mechanism: SCRAM-SHA-256
    username: foo
    password: bar
`, nil)
	require.NoError(t, err)

	mechanisms, err := saslMechanismsFromConfig(pConf)
	require.NoError(t, err)
	assert.Len(t, mechanisms, 2)
}
