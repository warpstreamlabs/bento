package mqtt

import (
	"bytes"
	"log/slog"
	"sort"
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

// TestDownSignalIsClosedOnEveryConnection guards the invariant the down signal
// exists for: every channel a reader can be holding must be closed when the
// connection it stands for drops, and a connect must never hand back one that
// is already closed.
//
// The ordering is what makes that awkward. autopaho releases AwaitConnection
// before it calls OnConnectionUp, so connect can return — and a read can
// capture the signal — while that handler has not run. Renewing the signal
// there would leave the reader holding a closed channel just after a
// successful connect, which reads as a live connection reporting itself down.
// So connect renews it instead, which it can because a drop now stops the
// manager rather than reconnecting behind it: every connection begins with a
// connect call.
func TestDownSignalIsClosedOnEveryConnection(t *testing.T) {
	conn := newConnectionV5(service.MockResources().Logger(), mv5RefusedRetry)

	var cfg autopaho.ClientConfig
	conn.installHooks(&cfg, nil)

	// What connect does before building a manager. The first signal is
	// already open, so this leaves it alone.
	renew := func() {
		conn.mu.Lock()
		conn.renewDownSignal()
		conn.mu.Unlock()
	}

	renew()
	// What a reader that beat OnConnectionUp would be holding.
	captured := conn.downSignal()

	cfg.OnConnectionUp(nil, nil)
	select {
	case <-captured:
		t.Fatal("the connection reported itself down at the moment it came up")
	default:
	}

	require.NotNil(t, cfg.OnConnectionDown)
	require.False(t, cfg.OnConnectionDown(),
		"a drop must stop the manager, so that Bento drives the reconnection")

	select {
	case <-captured:
	default:
		t.Fatal("a reader holding the channel from before OnConnectionUp never learned the link dropped")
	}

	// And the next connect starts from an open signal rather than the closed
	// one, before anything can read it.
	renew()
	second := conn.downSignal()
	select {
	case <-second:
		t.Fatal("connect handed back a signal that was already closed")
	default:
	}
	cfg.OnConnectionUp(nil, nil)
	require.False(t, cfg.OnConnectionDown())
	select {
	case <-second:
	default:
		t.Fatal("the second connection's channel was never closed")
	}
}

// TestRebuiltManagerNeverCleanStarts pins the session half of Bento-driven
// reconnection: clean_start speaks about the first connection of the
// component's life, so a manager built after one would discard the very
// session it exists to resume if it honoured the setting again.
func TestRebuiltManagerNeverCleanStarts(t *testing.T) {
	conn := newConnectionV5(service.MockResources().Logger(), mv5RefusedRetry)

	var cfg autopaho.ClientConfig
	cfg.CleanStartOnInitialConnection = true
	conn.installHooks(&cfg, nil)

	// Before anything has connected, the configured clean start stands: the
	// first connection is exactly what the setting is about.
	first := cfg
	conn.mu.Lock()
	conn.resumeRatherThanCleanStart(&first)
	conn.mu.Unlock()
	assert.True(t, first.CleanStartOnInitialConnection)

	cfg.OnConnectionUp(nil, nil)

	rebuilt := cfg
	conn.mu.Lock()
	conn.resumeRatherThanCleanStart(&rebuilt)
	conn.mu.Unlock()
	assert.False(t, rebuilt.CleanStartOnInitialConnection)
}

// TestOptionalPropertiesTolerateAbsentMetadata covers the configuration the
// output's own documentation recommends for bridging: each MQTT 5 property
// named from the metadata the input wrote. Most messages carry only some of
// them, and an absent metadata field interpolates to the string "null" rather
// than to an empty one — so treating that as a value failed every message whose
// source had no expiry, permanently, because the output retries and the input
// then holds every acknowledgement behind it.
func TestOptionalPropertiesTolerateAbsentMetadata(t *testing.T) {
	writerFor := func(t *testing.T) *mqttWriterV5 {
		t.Helper()
		conf, err := outputConfigSpecV5().ParseYAML(`
urls: [ tcp://localhost:1883 ]
topic: out
content_type: ${! meta("mqtt_content_type") }
response_topic: ${! meta("mqtt_response_topic") }
correlation_data: ${! meta("mqtt_correlation_data") }
message_expiry_interval: ${! meta("mqtt_message_expiry_interval") }
payload_format_indicator: ${! meta("mqtt_payload_format_indicator") }
`, nil)
		require.NoError(t, err)
		w, err := newMQTTWriterV5FromParsed(conf, service.MockResources())
		require.NoError(t, err)
		return w
	}

	t.Run("a message carrying none of them sets none of them", func(t *testing.T) {
		props, err := writerFor(t).properties(service.NewMessage([]byte(`{}`)))
		require.NoError(t, err, "a message without these properties must still be publishable")
		assert.Empty(t, props.ContentType)
		assert.Empty(t, props.ResponseTopic)
		assert.Empty(t, props.CorrelationData)
		assert.Nil(t, props.MessageExpiry)
		assert.Nil(t, props.PayloadFormat)
	})

	t.Run("a message carrying them all sets them all", func(t *testing.T) {
		msg := service.NewMessage([]byte(`{}`))
		msg.MetaSetMut("mqtt_content_type", "application/json")
		msg.MetaSetMut("mqtt_response_topic", "replies")
		msg.MetaSetMut("mqtt_correlation_data", "corr-1")
		msg.MetaSetMut("mqtt_message_expiry_interval", 300)
		msg.MetaSetMut("mqtt_payload_format_indicator", 1)

		props, err := writerFor(t).properties(msg)
		require.NoError(t, err)
		assert.Equal(t, "application/json", props.ContentType)
		assert.Equal(t, "replies", props.ResponseTopic)
		assert.Equal(t, []byte("corr-1"), props.CorrelationData)
		require.NotNil(t, props.MessageExpiry)
		assert.EqualValues(t, 300, *props.MessageExpiry)
		require.NotNil(t, props.PayloadFormat)
		assert.EqualValues(t, 1, *props.PayloadFormat)
	})

	t.Run("a value that is present but malformed is still an error", func(t *testing.T) {
		msg := service.NewMessage([]byte(`{}`))
		msg.MetaSetMut("mqtt_message_expiry_interval", "half an hour")
		_, err := writerFor(t).properties(msg)
		require.Error(t, err, "a malformed value is a configuration mistake and must not pass silently")
		assert.Contains(t, err.Error(), "whole number of seconds")
	})
}

// TestMetadataExclusionDefault covers the default that makes an MQTT-to-MQTT
// bridge correct without configuration: the mqtt_ namespace is what the input
// writes to describe a message it received, so forwarding it would add a
// handful of properties describing the previous hop to every message, on every
// hop. It stays overridable, because a pipeline that genuinely wants those
// forwarded should be able to say so.
func TestMetadataExclusionDefault(t *testing.T) {
	writerFor := func(t *testing.T, metadataYAML string) *mqttWriterV5 {
		t.Helper()
		conf, err := outputConfigSpecV5().ParseYAML(
			"urls: [ tcp://localhost:1883 ]\ntopic: out\n"+metadataYAML, nil)
		require.NoError(t, err)
		w, err := newMQTTWriterV5FromParsed(conf, service.MockResources())
		require.NoError(t, err)
		return w
	}

	// A message as the input would hand it over: the sender's own property,
	// plus the bookkeeping the input wrote about the delivery.
	bridged := func() *service.Message {
		msg := service.NewMessage([]byte(`{}`))
		msg.MetaSetMut("sensor-id", "temp-1")
		msg.MetaSetMut("mqtt_topic", "src/a")
		msg.MetaSetMut("mqtt_qos", 1)
		msg.MetaSetMut("mqtt_retained", false)
		msg.MetaSetMut("mqtt_content_type", "application/json")
		return msg
	}

	keys := func(props *paho.PublishProperties) []string {
		var out []string
		for _, u := range props.User {
			out = append(out, u.Key)
		}
		sort.Strings(out)
		return out
	}

	t.Run("by default everything is forwarded", func(t *testing.T) {
		props, err := writerFor(t, "").properties(bridged())
		require.NoError(t, err)
		assert.Equal(t, []string{
			"mqtt_content_type", "mqtt_qos", "mqtt_retained", "mqtt_topic", "sensor-id",
		}, keys(props), "an unconfigured metadata setting must mean an empty exclusion list")
	})

	t.Run("excluding mqtt_ holds back the input's bookkeeping", func(t *testing.T) {
		// The configuration the documentation recommends for a bridge.
		props, err := writerFor(t, "metadata:\n  exclude_prefixes: [ mqtt_ ]").properties(bridged())
		require.NoError(t, err)
		assert.Equal(t, []string{"sensor-id"}, keys(props),
			"a bridge forwarded the bookkeeping describing the previous hop")
	})

	t.Run("a prefix of the pipeline's own works too", func(t *testing.T) {
		msg := bridged()
		msg.MetaSetMut("secret_token", "must not travel")
		props, err := writerFor(t, "metadata:\n  exclude_prefixes: [ secret_ ]").properties(msg)
		require.NoError(t, err)
		assert.NotContains(t, keys(props), "secret_token")
		assert.Contains(t, keys(props), "mqtt_topic")
	})
}
