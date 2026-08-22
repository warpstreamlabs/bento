package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	// Registers the default tracer and metrics exporter the stream builder needs.
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

// TestProbeInputAgainstBroker drives the registered mqtt_v5 input against a
// real server, which is the only thing that proves any of it: everything below
// the config parsing is behaviour of a protocol, not of this code.
//
// Set MQTT_V5_PROBE_URL to run it, e.g. tcp://localhost:1883.
func TestProbeInputAgainstBroker(t *testing.T) {
	broker := os.Getenv("MQTT_V5_PROBE_URL")
	if broker == "" {
		t.Skip("set MQTT_V5_PROBE_URL to run this probe")
	}
	topic := os.Getenv("MQTT_V5_PROBE_TOPIC")
	if topic == "" {
		topic = "probe"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	received := make(chan *service.Message, 16)

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML("level: INFO\nformat: logfmt"))
	require.NoError(t, builder.AddInputYAML(fmt.Sprintf(`
mqtt_v5:
  urls: [ %v ]
  topics: [ %v ]
  client_id: bento-v5-probe
  qos: 1
  clean_start: true
`, broker, topic)))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		received <- msg
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Go(func() {
		_ = stream.Run(ctx)
	})
	t.Cleanup(func() {
		_ = stream.StopWithin(10 * time.Second)
		wg.Wait()
	})

	// The publisher is a separate connection, so that what is asserted is what
	// crossed the wire rather than anything this component did to it.
	publisherURL, err := url.Parse(broker)
	require.NoError(t, err)

	publisher, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{publisherURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ClientConfig:                  paho.ClientConfig{ClientID: "bento-v5-probe-publisher"},
	})
	require.NoError(t, err)
	require.NoError(t, publisher.AwaitConnection(ctx))
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	expiry := uint32(300)
	format := byte(1)

	// A marker unique to this run. An append-style channel replays everything
	// it holds to a new subscriber, so the first message to arrive is very
	// often a stored one from an earlier run — taking it on trust is how a
	// probe asserts against data it did not publish.
	nonce := fmt.Sprintf("probe-%v", time.Now().UnixNano())

	publish := func(attempt int) {
		_, err := publisher.Publish(ctx, &paho.Publish{
			Topic:   topic,
			QoS:     1,
			Payload: fmt.Appendf(nil, `{"nonce":%q,"attempt":%v}`, nonce, attempt),
			Properties: &paho.PublishProperties{
				ContentType:     "application/json",
				ResponseTopic:   "probe/replies",
				CorrelationData: []byte("correlation-1"),
				MessageExpiry:   &expiry,
				PayloadFormat:   &format,
				User: paho.UserProperties{
					// Keys chosen so that no server claims them as its own:
					// saguin, for one, sets saguin-offset itself on an append
					// channel and would overwrite whatever was published.
					{Key: "probe-key", Value: "id-42"},
					{Key: "probe-second", Value: "7"},
					{Key: "probe-nonce", Value: nonce},
					// A publisher must not be able to overwrite the fields the
					// input sets about the message itself.
					{Key: "mqtt_topic", Value: "forged"},
				},
			},
		})
		require.NoError(t, err, "publish failed")
	}

	// The subscription may not be in place the instant the stream starts, and a
	// message published before it is simply not delivered — so publish until
	// one arrives rather than publishing once and hoping.
	var msg *service.Message
	deadline := time.After(30 * time.Second)
	attempt := 0
	publish(attempt)

	replayed := 0
collect:
	for {
		select {
		case candidate := <-received:
			if got, _ := candidate.MetaGet("probe-nonce"); got == nonce {
				msg = candidate
				break collect
			}
			replayed++
		case <-time.After(time.Second):
			attempt++
			publish(attempt)
		case <-deadline:
			t.Fatalf("no message of this run reached the pipeline (%v other messages did)", replayed)
		}
	}
	if replayed > 0 {
		t.Logf("skipped %v message(s) the server replayed from before this run", replayed)
	}

	require.NotNil(t, msg, "the probe asserted nothing because nothing arrived")

	payload, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(payload), `"attempt"`)

	meta := map[string]any{}
	require.NoError(t, msg.MetaWalkMut(func(k string, v any) error {
		meta[k] = v
		return nil
	}))
	t.Logf("metadata: %v", meta)

	// User properties arrive under their own keys. This is the thing MQTT 3.1.1
	// cannot do at all.
	assert.Equal(t, "id-42", meta["probe-key"])
	assert.Equal(t, "7", meta["probe-second"])

	// And a user property cannot forge a field describing the message.
	assert.Equal(t, topic, meta["mqtt_topic"], "a user property overwrote mqtt_topic")

	assert.Equal(t, 1, meta["mqtt_qos"])
	assert.Equal(t, false, meta["mqtt_retained"])
	assert.Contains(t, meta, "mqtt_duplicate")
	assert.Contains(t, meta, "mqtt_message_id")

	// The remaining MQTT 5 properties are only asserted against a server that
	// forwards what the publisher sent. A store-and-replay server may deliver
	// its own record instead, carrying its own properties and none of the
	// publisher's — saguin does exactly that on an append channel. Whether they
	// arrive is the server's behaviour, not this input's, so the assertion is
	// switched off rather than weakened for everybody.
	if os.Getenv("MQTT_V5_PROBE_NO_PROPERTY_FORWARDING") != "" {
		t.Logf("skipping publisher-property assertions at the server's request")
		return
	}
	assert.Equal(t, "application/json", meta["mqtt_content_type"])
	assert.Equal(t, "probe/replies", meta["mqtt_response_topic"])
	assert.Equal(t, []byte("correlation-1"), meta["mqtt_correlation_data"])
	assert.Equal(t, 1, meta["mqtt_payload_format_indicator"])
	assert.Contains(t, meta, "mqtt_message_expiry_interval")
}
