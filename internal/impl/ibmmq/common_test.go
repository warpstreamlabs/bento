//go:build x_bento_extra

package ibmmq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestIBMMQConnectionConfigFromParsed(t *testing.T) {
	const connection = `
queue_manager: QM1
channel: DEV.APP.SVRCONN
connection_name: localhost(1414)
queue: DEV.QUEUE.1
`

	tests := []struct {
		name          string
		config        string
		expected      ibmMQConnectionConfig
		expectedError string
	}{
		{
			name:   "connection only",
			config: connection,
			expected: ibmMQConnectionConfig{
				queueManager:   "QM1",
				channel:        "DEV.APP.SVRCONN",
				connectionName: "localhost(1414)",
			},
		},
		{
			name: "auth and tls",
			config: connection + `
auth:
  user: app
  password: passw0rd
tls:
  enabled: true
  cipher_spec: ANY_TLS12_OR_HIGHER
  key_repository: /opt/mqm/ssl/key
  certificate_label: bento
`,
			expected: ibmMQConnectionConfig{
				queueManager:        "QM1",
				channel:             "DEV.APP.SVRCONN",
				connectionName:      "localhost(1414)",
				authUser:            "app",
				authPassword:        "passw0rd",
				tlsEnabled:          true,
				tlsCipherSpec:       "ANY_TLS12_OR_HIGHER",
				tlsKeyRepository:    "/opt/mqm/ssl/key",
				tlsCertificateLabel: "bento",
			},
		},
		{
			name: "tls without cipher spec",
			config: connection + `
tls:
  enabled: true
  key_repository: /opt/mqm/ssl/key
`,
			expectedError: "tls.cipher_spec must be set when tls.enabled is true",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := ibmMQInputSpec().ParseYAML(test.config, nil)
			require.NoError(t, err)

			connectionConfig, err := ibmMQConnectionConfigFromParsed(config)
			if test.expectedError != "" {
				require.ErrorContains(t, err, test.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, connectionConfig)
		})
	}
}

func TestIBMMQConnectionLintRule(t *testing.T) {
	const connection = `
ibm_mq:
  queue_manager: QM1
  channel: DEV.APP.SVRCONN
  connection_name: localhost(1414)
  queue: DEV.QUEUE.1
`

	tests := []struct {
		name          string
		config        string
		expectedError string
	}{
		{
			name:   "connection only",
			config: connection,
		},
		{
			name: "password without user",
			config: connection + `
  auth:
    password: passw0rd
`,
			expectedError: "auth.password cannot be set without auth.user",
		},
		{
			name: "tls without cipher spec",
			config: connection + `
  tls:
    enabled: true
    key_repository: /opt/mqm/ssl/key
`,
			expectedError: "tls.cipher_spec must be set when tls.enabled is true",
		},
		{
			name: "tls without key repository",
			config: connection + `
  tls:
    enabled: true
    cipher_spec: ANY_TLS12_OR_HIGHER
`,
			expectedError: "tls.key_repository must be set when tls.enabled is true",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := service.NewStreamBuilder().AddInputYAML(test.config)
			if test.expectedError == "" {
				require.NoError(t, err)
				return
			}

			require.ErrorContains(t, err, test.expectedError)
		})
	}
}
