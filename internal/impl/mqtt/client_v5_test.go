package mqtt

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func parseClientConfigV5(t *testing.T, yaml string) (clientConfigV5, error) {
	t.Helper()
	spec := service.NewConfigSpec().Fields(clientFieldsV5()...)
	conf, err := spec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	return clientConfigV5FromParsed(conf)
}

func TestClientConfigV5Defaults(t *testing.T) {
	conf, err := parseClientConfigV5(t, `urls: [ tcp://localhost:1883 ]`)
	require.NoError(t, err)

	assert.True(t, conf.cleanStart)
	assert.EqualValues(t, 0, conf.sessionExpiryInterval)
	assert.Nil(t, conf.receiveMaximum)
	assert.Nil(t, conf.maximumPacketSize)
	assert.Equal(t, time.Second, conf.backoffMin)
	assert.Equal(t, time.Minute, conf.backoffMax)
	assert.EqualValues(t, 30, conf.keepAlive)
}

// TestClientConfigV5ConnectProperties covers the reason receive_maximum and
// maximum_packet_size need a packet builder at all: they have no field on
// autopaho.ClientConfig, and autopaho allocates the properties struct they go
// into only when a session expiry interval was set. On a default config the
// builder is handed a nil.
func TestClientConfigV5ConnectProperties(t *testing.T) {
	for _, test := range []struct {
		name              string
		yaml              string
		propertiesArrived bool
	}{
		{
			name: "properties arrive nil",
			yaml: `
urls: [ tcp://localhost:1883 ]
receive_maximum: 10
maximum_packet_size: 1024
`,
			propertiesArrived: false,
		},
		{
			name: "properties arrive allocated",
			yaml: `
urls: [ tcp://localhost:1883 ]
session_expiry_interval: 1h
receive_maximum: 10
maximum_packet_size: 1024
`,
			propertiesArrived: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf, err := parseClientConfigV5(t, test.yaml)
			require.NoError(t, err)

			var cfg autopaho.ClientConfig
			conf.apply(&cfg)
			require.NotNil(t, cfg.ConnectPacketBuilder)

			// Built the way autopaho builds it, so the nil case is the real one.
			pkt := &paho.Connect{}
			if test.propertiesArrived {
				pkt.Properties = &paho.ConnectProperties{SessionExpiryInterval: &conf.sessionExpiryInterval}
			}

			pkt, err = cfg.ConnectPacketBuilder(pkt, nil)
			require.NoError(t, err)
			require.NotNil(t, pkt.Properties)
			require.NotNil(t, pkt.Properties.ReceiveMaximum)
			require.NotNil(t, pkt.Properties.MaximumPacketSize)
			assert.EqualValues(t, 10, *pkt.Properties.ReceiveMaximum)
			assert.EqualValues(t, 1024, *pkt.Properties.MaximumPacketSize)

			if test.propertiesArrived {
				require.NotNil(t, pkt.Properties.SessionExpiryInterval)
				assert.EqualValues(t, 3600, *pkt.Properties.SessionExpiryInterval)
			}
		})
	}
}

// TestClientConfigV5Rejections covers the values autopaho would panic on, or
// silently truncate, if they reached it.
func TestClientConfigV5Rejections(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml string
		errs string
	}{
		{
			name: "backoff max below min",
			yaml: "urls: [ tcp://localhost:1883 ]\nreconnect_backoff:\n  min: 10s\n  max: 1s\n",
			errs: "reconnect_backoff.max must be greater than reconnect_backoff.min",
		},
		{
			name: "backoff min of zero",
			yaml: "urls: [ tcp://localhost:1883 ]\nreconnect_backoff:\n  min: 0s\n  max: 1s\n",
			errs: "reconnect_backoff.min must be greater than zero",
		},
		{
			name: "keepalive beyond the field width",
			yaml: "urls: [ tcp://localhost:1883 ]\nkeepalive: 70000\n",
			errs: "keepalive must be between 0 and 65535 seconds",
		},
		{
			name: "receive maximum beyond the field width",
			yaml: "urls: [ tcp://localhost:1883 ]\nreceive_maximum: 70000\n",
			errs: "receive_maximum must be between 1 and 65535",
		},
		{
			name: "will without a topic",
			yaml: "urls: [ tcp://localhost:1883 ]\nwill:\n  enabled: true\n",
			errs: "include topic to register a last will",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseClientConfigV5(t, test.yaml)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errs)
		})
	}
}

// TestClientConfigV5AppliesBackoff proves the exponential backoff is built
// rather than left to autopaho's default of a flat ten seconds, and that the
// arguments it is built with do not trip its own panics.
func TestClientConfigV5AppliesBackoff(t *testing.T) {
	conf, err := parseClientConfigV5(t, "urls: [ tcp://localhost:1883 ]\nreconnect_backoff:\n  min: 1s\n  max: 8s\n")
	require.NoError(t, err)

	var cfg autopaho.ClientConfig
	require.NotPanics(t, func() { conf.apply(&cfg) })
	require.NotNil(t, cfg.ReconnectBackoff)

	for attempt := range 12 {
		wait := cfg.ReconnectBackoff(attempt)
		assert.GreaterOrEqual(t, wait, time.Duration(0))
		assert.LessOrEqual(t, wait, 8*time.Second, "attempt %v exceeded the configured maximum", attempt)
	}
}

// TestClientConfigV5WillProperties guards the delay interval surviving as a
// pointer to this client's own value rather than to a loop or field that later
// changes underneath it.
func TestClientConfigV5WillProperties(t *testing.T) {
	conf, err := parseClientConfigV5(t, "urls: [ tcp://localhost:1883 ]\nwill:\n  enabled: true\n  topic: goodbye\n  payload: gone\n  qos: 1\n  delay_interval: 30s\n")
	require.NoError(t, err)

	var cfg autopaho.ClientConfig
	conf.apply(&cfg)

	require.NotNil(t, cfg.WillMessage)
	assert.Equal(t, "goodbye", cfg.WillMessage.Topic)
	assert.Equal(t, []byte("gone"), cfg.WillMessage.Payload)
	assert.EqualValues(t, 1, cfg.WillMessage.QoS)

	require.NotNil(t, cfg.WillProperties)
	require.NotNil(t, cfg.WillProperties.WillDelayInterval)
	assert.EqualValues(t, 30, *cfg.WillProperties.WillDelayInterval)
}

// TestConnackRefusalKeepsTheReasonCode is the point of the type: the library's
// error text reports that a connection failed, and only the wrapped error still
// knows what the server actually answered.
func TestConnackRefusalKeepsTheReasonCode(t *testing.T) {
	refusal := autopaho.NewConnackError(
		assert.AnError,
		&paho.Connack{ReasonCode: 0x87},
	)

	code, ok := connackRefusal(refusal)
	require.True(t, ok)
	assert.EqualValues(t, 0x87, code)
	assert.Equal(t, "not authorized", mv5ConnackReason(code))

	_, ok = connackRefusal(assert.AnError)
	assert.False(t, ok)
}

// TestServerDisconnectIsLoggedWithItsReason drives the handler autopaho would
// call, because the point of it is the log line: without the code, a server
// saying "another client took this client_id" is indistinguishable from a
// dropped network link.
func TestServerDisconnectIsLoggedWithItsReason(t *testing.T) {
	for _, test := range []struct {
		name       string
		disconnect *paho.Disconnect
		expected   []string
	}{
		{
			name:       "reason code alone",
			disconnect: &paho.Disconnect{ReasonCode: 0x8E},
			expected:   []string{"session taken over", "0x8E"},
		},
		{
			name: "reason string from the server is kept",
			disconnect: &paho.Disconnect{
				ReasonCode: 0x98,
				Properties: &paho.DisconnectProperties{ReasonString: "channel retired"},
			},
			expected: []string{"administrative action", "channel retired", "0x98"},
		},
		{
			name: "server reference is kept",
			disconnect: &paho.Disconnect{
				ReasonCode: 0x9D,
				Properties: &paho.DisconnectProperties{ServerReference: "mqtt://elsewhere:1883"},
			},
			expected: []string{"server moved", "mqtt://elsewhere:1883", "0x9D"},
		},
		{
			name:       "a code nobody has seen before still reports its number",
			disconnect: &paho.Disconnect{ReasonCode: 0xB7},
			expected:   []string{"unrecognised reason code", "0xB7"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logged bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))
			res := service.MockResources(service.MockResourcesOptUseSlogger(logger))

			var cfg autopaho.ClientConfig
			newConnectionV5(res.Logger(), mv5RefusedRetry).installHooks(&cfg, nil)
			require.NotNil(t, cfg.OnServerDisconnect, "the handler was never installed")

			cfg.OnServerDisconnect(test.disconnect)

			require.NotEmpty(t, logged.String(), "the handler ran but logged nothing")
			for _, want := range test.expected {
				assert.Contains(t, logged.String(), want)
			}
		})
	}
}
