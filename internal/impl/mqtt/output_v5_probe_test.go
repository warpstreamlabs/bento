package mqtt

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	// Registers the default tracer and metrics exporter the stream builder needs.
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

// TestProbeRoundTripAgainstBroker sends through the mqtt_v5 output and reads
// back through the mqtt_v5 input, which is the only way to prove that metadata
// survives as user properties: neither component can demonstrate it alone.
//
// Set MQTT_V5_PROBE_URL to run it.
func TestProbeRoundTripAgainstBroker(t *testing.T) {
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

	received := make(chan *service.Message, 64)

	inBuilder := service.NewStreamBuilder()
	require.NoError(t, inBuilder.SetLoggerYAML("level: WARN\nformat: logfmt"))
	require.NoError(t, inBuilder.AddInputYAML(fmt.Sprintf(
		"mqtt_v5:\n  urls: [ %v ]\n  topics: [ %v ]\n  client_id: roundtrip-in\n  qos: 1\n", broker, topic)))
	require.NoError(t, inBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		received <- m
		return nil
	}))
	inStream, err := inBuilder.Build()
	require.NoError(t, err)

	outBuilder := service.NewStreamBuilder()
	require.NoError(t, outBuilder.SetLoggerYAML("level: WARN\nformat: logfmt"))
	produce, err := outBuilder.AddProducerFunc()
	require.NoError(t, err)
	require.NoError(t, outBuilder.AddOutputYAML(fmt.Sprintf(`
mqtt_v5:
  urls: [ %v ]
  client_id: roundtrip-out
  topic: %v
  qos: 1
  content_type: application/json
  correlation_data: ${! meta("correlation") }
  message_expiry_interval: 300
  payload_format_indicator: 1
  metadata:
    exclude_prefixes: [ secret_ ]
`, broker, topic)))
	outStream, err := outBuilder.Build()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Go(func() { _ = inStream.Run(ctx) })
	wg.Go(func() { _ = outStream.Run(ctx) })
	t.Cleanup(func() {
		_ = inStream.StopWithin(10 * time.Second)
		_ = outStream.StopWithin(10 * time.Second)
		wg.Wait()
	})

	nonce := fmt.Sprintf("roundtrip-%v", time.Now().UnixNano())

	send := func(attempt int) {
		msg := service.NewMessage(fmt.Appendf(nil, `{"nonce":%q,"attempt":%v}`, nonce, attempt))
		msg.MetaSetMut("probe-nonce", nonce)
		msg.MetaSetMut("probe-key", "value-from-metadata")
		msg.MetaSetMut("correlation", "correlation-out")
		// saguin reads this as the deduplication key and keeps what the
		// publisher sent, rather than generating one. It is the only reserved
		// name a client is meant to set, and carrying it is the reason an
		// output exists at all — the stock mqtt output cannot send it.
		msg.MetaSetMut("saguin-id", nonce)
		// Excluded by the metadata filter above, so it must not reach the wire.
		msg.MetaSetMut("secret_token", "must-not-travel")
		require.NoError(t, produce(ctx, msg))
	}

	send(0)

	var got *service.Message
	deadline := time.After(30 * time.Second)
	attempt := 0
	skipped := 0
collect:
	for {
		select {
		case candidate := <-received:
			if v, _ := candidate.MetaGet("probe-nonce"); v == nonce {
				got = candidate
				break collect
			}
			skipped++
		case <-time.After(time.Second):
			attempt++
			send(attempt)
		case <-deadline:
			t.Fatalf("nothing this run published came back (%v other messages did)", skipped)
		}
	}
	if skipped > 0 {
		t.Logf("skipped %v message(s) the server replayed from before this run", skipped)
	}

	meta := map[string]any{}
	require.NoError(t, got.MetaWalkMut(func(k string, v any) error { meta[k] = v; return nil }))
	t.Logf("round-tripped metadata: %v", meta)

	// Metadata went out as user properties and came back as metadata.
	assert.Equal(t, "value-from-metadata", meta["probe-key"])
	assert.Equal(t, nonce, meta["probe-nonce"])

	// A deduplication key set by the pipeline survives to the consumer.
	assert.Equal(t, nonce, meta["saguin-id"], "the deduplication key did not survive")

	// The exclusion filter was honoured, so a secret in metadata stayed local.
	assert.NotContains(t, meta, "secret_token", "an excluded metadata field reached the wire")

	payload, err := got.AsBytes()
	require.NoError(t, err)
	assert.Contains(t, string(payload), nonce)

	if os.Getenv("MQTT_V5_PROBE_NO_PROPERTY_FORWARDING") != "" {
		t.Log("skipping publisher-property assertions at the server's request")
		return
	}
	assert.Equal(t, "application/json", meta["mqtt_content_type"])
	assert.Equal(t, []byte("correlation-out"), meta["mqtt_correlation_data"])
	assert.Equal(t, 1, meta["mqtt_payload_format_indicator"])
	assert.Contains(t, meta, "mqtt_message_expiry_interval")
}
