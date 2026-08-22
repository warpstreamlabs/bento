package mqtt

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
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

// TestDownSignalIsClosedOnEveryConnection guards the invariant the down channel
// exists for: every channel a reader can be holding must be closed when the
// connection it stands for drops.
//
// The first connection is the awkward one. autopaho releases AwaitConnection
// before it calls OnConnectionUp, so connect can return — and a read can
// capture the channel — while OnConnectionUp has not run. If that handler
// replaced the channel unconditionally, the one the reader holds would be
// orphaned open and that reader would never learn the link had dropped.
func TestDownSignalIsClosedOnEveryConnection(t *testing.T) {
	conn := newConnectionV5(service.MockResources().Logger(), mv5RefusedRetry)

	var cfg autopaho.ClientConfig
	conn.installHooks(&cfg, nil)

	// What a reader that beat OnConnectionUp would be holding.
	captured := conn.downSignal()

	cfg.OnConnectionUp(nil, nil)
	select {
	case <-captured:
		t.Fatal("the connection reported itself down at the moment it came up")
	default:
	}

	require.NotNil(t, cfg.OnConnectionDown)
	cfg.OnConnectionDown()

	select {
	case <-captured:
	default:
		t.Fatal("a reader holding the channel from before OnConnectionUp never learned the link dropped")
	}

	// And a later connection gets a channel of its own, closed in its turn.
	cfg.OnConnectionUp(nil, nil)
	second := conn.downSignal()
	select {
	case <-second:
		t.Fatal("the second connection started already down")
	default:
	}
	cfg.OnConnectionDown()
	select {
	case <-second:
	default:
		t.Fatal("the second connection's channel was never closed")
	}
}

// TestProgressWatchdogWarnsWhenNothingMoves covers the stall this input can
// reach without ever seeing an error: under the default auto_replay_nacks a
// message that can never succeed is retried inside Bento for ever, so the
// acknowledgement function here is never called at all, the acknowledgement is
// never released, and once the server's receive window is full it stops
// delivering. Nothing about that is an error anything can report — only the
// counters standing still show it.
func TestProgressWatchdogWarnsWhenNothingMoves(t *testing.T) {
	newReader := func(t *testing.T) (*mqttReaderV5, *syncBuffer) {
		t.Helper()
		conf, err := inputConfigSpecV5().ParseYAML("urls: [ tcp://localhost:1883 ]\ntopics: [ x ]", nil)
		require.NoError(t, err)
		res, captured := capturedLogger()
		rdr, err := newMQTTReaderV5FromParsed(conf, res)
		require.NoError(t, err)
		rdr.stallSample, rdr.stallAfter = 20*time.Millisecond, 3
		t.Cleanup(func() { _ = rdr.Close(context.Background()) })
		return rdr, captured
	}

	t.Run("messages outstanding and nothing moving warns", func(t *testing.T) {
		rdr, captured := newReader(t)
		// Four handed to the pipeline, three finished with: one is stuck, and
		// no error was ever reported for it.
		rdr.handed.Store(4)
		rdr.settled.Store(3)
		go rdr.watchProgress()

		require.Eventually(t, func() bool {
			return strings.Contains(captured.String(), "has not finished a message in at least")
		}, 5*time.Second, 10*time.Millisecond, "a stalled input warned about nothing")

		logged := captured.String()
		assert.Contains(t, logged, "1 are outstanding")
		assert.Contains(t, logged, "dead-letter")
		assert.Equal(t, 1, strings.Count(logged, "has not finished a message"), "the warning repeated")
	})

	t.Run("an idle input owing nothing stays quiet", func(t *testing.T) {
		rdr, captured := newReader(t)
		rdr.handed.Store(7)
		rdr.settled.Store(7)
		go rdr.watchProgress()

		time.Sleep(300 * time.Millisecond)
		assert.NotContains(t, captured.String(), "has not finished a message",
			"a quiet topic was reported as a stall")
	})

	t.Run("a slow but backlogged pipeline is reported once, not per message", func(t *testing.T) {
		// A pipeline whose honest per-message latency exceeds the window
		// reaches the same condition as a stalled one. Re-arming on any
		// movement made this warn once per message for the healthy case and
		// once in total for the broken one, which is the wrong way round.
		rdr, captured := newReader(t)
		rdr.handed.Store(4)
		rdr.settled.Store(0)
		go rdr.watchProgress()

		for range 4 {
			time.Sleep(120 * time.Millisecond) // longer than the window
			rdr.settled.Add(1)                 // a message legitimately finishes
			rdr.handed.Add(1)                  // and another arrives behind it
		}
		time.Sleep(120 * time.Millisecond)

		assert.Equal(t, 1, strings.Count(captured.String(), "has not finished a message in at least"),
			"a slow pipeline was reported once per message rather than once")
	})

	t.Run("an input that catches up is reported again if it stalls later", func(t *testing.T) {
		rdr, captured := newReader(t)
		rdr.handed.Store(2)
		rdr.settled.Store(1)
		go rdr.watchProgress()

		require.Eventually(t, func() bool {
			return strings.Count(captured.String(), "has not finished a message in at least") == 1
		}, 5*time.Second, 10*time.Millisecond, "the first stall was never reported")

		// Caught up: nothing is owed.
		rdr.settled.Store(2)
		time.Sleep(120 * time.Millisecond)

		// And stalls again.
		rdr.handed.Store(3)
		require.Eventually(t, func() bool {
			return strings.Count(captured.String(), "has not finished a message in at least") == 2
		}, 5*time.Second, 10*time.Millisecond, "a second stall after catching up went unreported")
	})

	t.Run("a slow but advancing pipeline stays quiet", func(t *testing.T) {
		rdr, captured := newReader(t)
		rdr.handed.Store(4)
		rdr.settled.Store(1)
		go rdr.watchProgress()

		for range 12 {
			time.Sleep(25 * time.Millisecond)
			rdr.handed.Add(1)
			rdr.settled.Add(1)
		}
		assert.NotContains(t, captured.String(), "has not finished a message",
			"a pipeline that was still making progress was reported as stalled")
	})
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
