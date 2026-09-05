package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	// Registers the tracer and metrics exporter the stream builder needs.
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestIntegrationMQTTv5(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "eclipse-mosquitto",
		Tag:        "2",
		// The image ships this configuration for exactly this purpose; the
		// default one listens on no port at all and accepts nobody.
		Cmd: []string{"mosquitto", "-c", "/mosquitto-no-auth.conf"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	port := resource.GetPort("1883/tcp")

	require.NoError(t, pool.Retry(func() error {
		client, err := connectV5(context.Background(), port, "readiness-probe")
		if err != nil {
			return err
		}
		return client.Disconnect(context.Background())
	}))

	template := `
output:
  mqtt_v5:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  mqtt_v5:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    client_id: client-input-$ID
    clean_start: false
    session_expiry_interval: 60s
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// The 3.1.1 component carries this one commented out as a TODO,
		// because MQTT 3.1.1 has nowhere to put metadata. This is the whole
		// argument for the component in a single line.
		integration.StreamTestMetadata(),
		integration.StreamTestMetadataFilter(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(port),
	)

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(port),
			integration.StreamTestOptMaxInFlight(10),
		)
	})

	// The cases below are the ones the generic suite cannot express. Without
	// them this proves nothing the 3.1.1 component did not already prove.
	t.Run("a durable session receives what was missed", func(t *testing.T) {
		testDurableSessionV5(t, port)
	})
	t.Run("a shared subscription splits one stream", func(t *testing.T) {
		testSharedSubscriptionV5(t, port)
	})
	t.Run("a nacked message is redelivered", func(t *testing.T) {
		testNackIsRedeliveredV5(t, port)
	})
	t.Run("an acknowledged message is not redelivered", func(t *testing.T) {
		testAckIsNotRedeliveredV5(t, port)
	})
	t.Run("mqtt 5 properties and user properties survive a round trip", func(t *testing.T) {
		testPropertiesRoundTripV5(t, port)
	})
	t.Run("a rejected message warns that consumption will stop", func(t *testing.T) {
		testRejectionWarnsV5(t, port)
	})
	t.Run("a dropped connection is handed back to Bento", func(t *testing.T) {
		testBentoDrivenReconnectV5(t, port)
	})
}

// connectV5 opens a plain MQTT 5 connection, used for publishing into these
// tests from outside the components under test.
func connectV5(ctx context.Context, port, clientID string) (*autopaho.ConnectionManager, error) {
	u, err := url.Parse(fmt.Sprintf("tcp://localhost:%v", port))
	if err != nil {
		return nil, err
	}
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	client, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		ClientConfig:                  paho.ClientConfig{ClientID: clientID},
	})
	if err != nil {
		return nil, err
	}
	if err := client.AwaitConnection(dialCtx); err != nil {
		return nil, err
	}
	return client, nil
}

func readerOnPort(t *testing.T, port, topics, extra string) *mqttReaderV5 {
	t.Helper()
	rdr, _ := readerOnPortLogged(t, port, topics, extra)
	return rdr
}

func readerOnPortLogged(t *testing.T, port, topics, extra string) (*mqttReaderV5, *syncBuffer) {
	t.Helper()
	yaml := fmt.Sprintf(`
urls: [ tcp://localhost:%v ]
topics: %v
qos: 1
%v
`, port, topics, extra)
	conf, err := inputConfigSpecV5().ParseYAML(yaml, nil)
	require.NoError(t, err)
	res, captured := capturedLogger()
	rdr, err := newMQTTReaderV5FromParsed(conf, res)
	require.NoError(t, err)
	return rdr, captured
}

func writerOnPort(t *testing.T, port, topic, extra string) *mqttWriterV5 {
	t.Helper()
	yaml := fmt.Sprintf(`
urls: [ tcp://localhost:%v ]
topic: %v
qos: 1
%v
`, port, topic, extra)
	conf, err := outputConfigSpecV5().ParseYAML(yaml, nil)
	require.NoError(t, err)
	w, err := newMQTTWriterV5FromParsed(conf, service.MockResources())
	require.NoError(t, err)
	return w
}

// testPropertiesRoundTripV5 sends through the output and reads back through the
// input. Neither component can demonstrate this alone, and none of it is
// expressible in MQTT 3.1.1 at all: the properties have no place in the packet
// and there is nowhere to put a user property.
func testPropertiesRoundTripV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("props-%v", time.Now().UnixNano())

	rdr := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic),
		fmt.Sprintf("client_id: props-in-%v", time.Now().UnixNano()))
	t.Cleanup(func() { _ = rdr.Close(context.Background()) })
	require.NoError(t, rdr.Connect(ctx))
	time.Sleep(500 * time.Millisecond)

	writer := writerOnPort(t, port, topic, fmt.Sprintf(`client_id: props-out-%v
content_type: application/json
response_topic: %v/replies
correlation_data: ${! meta("correlation") }
message_expiry_interval: 300
payload_format_indicator: 1
metadata:
  exclude_prefixes: [ secret_ ]`, time.Now().UnixNano(), topic))
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	require.NoError(t, writer.Connect(ctx))

	out := service.NewMessage([]byte(`{"hello":"world"}`))
	out.MetaSetMut("carried", "a user property")
	out.MetaSetMut("correlation", "correlation-1")
	// Excluded by the filter above, so it must not reach the wire.
	out.MetaSetMut("secret_token", "must not travel")
	// A publisher must not be able to forge a field describing the message.
	out.MetaSetMut("mqtt_topic", "forged")
	require.NoError(t, writer.Write(ctx, out))

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()

	msg, ack, err := rdr.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ack(readCtx, nil))

	meta := map[string]any{}
	require.NoError(t, msg.MetaWalkMut(func(k string, v any) error { meta[k] = v; return nil }))

	assert.Equal(t, "a user property", meta["carried"])
	assert.NotContains(t, meta, "secret_token", "an excluded metadata field reached the wire")
	assert.Equal(t, topic, meta["mqtt_topic"], "a user property overwrote mqtt_topic")

	assert.Equal(t, "application/json", meta["mqtt_content_type"])
	assert.Equal(t, topic+"/replies", meta["mqtt_response_topic"])
	assert.Equal(t, []byte("correlation-1"), meta["mqtt_correlation_data"])
	assert.Equal(t, 1, meta["mqtt_payload_format_indicator"])
	assert.Contains(t, meta, "mqtt_message_expiry_interval")
	assert.Equal(t, 1, meta["mqtt_qos"])
	assert.Equal(t, false, meta["mqtt_retained"])
	assert.Contains(t, meta, "mqtt_duplicate")
	assert.Contains(t, meta, "mqtt_message_id")
}

// testDurableSessionV5 is the combination people get wrong: clean_start false,
// a fixed client id and a session expiry above zero. Without all three, a
// message published while the pipeline was down is simply gone.
func testDurableSessionV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("durable-%v", time.Now().UnixNano())
	clientID := fmt.Sprintf("durable-client-%v", time.Now().UnixNano())
	extra := fmt.Sprintf("client_id: %v\nclean_start: false\nsession_expiry_interval: 300s", clientID)

	// Establish the session, then leave.
	first := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	require.NoError(t, first.Connect(ctx))
	// Give the subscription time to reach the server before disconnecting; a
	// session with no subscription in it stores nothing.
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, first.Close(ctx))

	// Publish while nothing is connected.
	publisher, err := connectV5(ctx, port, "durable-publisher")
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	_, err = publisher.Publish(ctx, &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte("published while away"),
	})
	require.NoError(t, err)

	// Come back under the same client id and collect what the session held.
	second := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	require.NoError(t, second.Connect(ctx))

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()

	msg, ack, err := second.Read(readCtx)
	require.NoError(t, err, "the session did not hold the message published while the input was away")
	require.NoError(t, ack(readCtx, nil))

	payload, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "published while away", string(payload))
}

// testSharedSubscriptionV5 covers the way this component scales out: a shared
// subscription is an ordinary filter, and two inputs using one group split the
// stream rather than each receiving all of it.
func testSharedSubscriptionV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	base := fmt.Sprintf("shared-%v", time.Now().UnixNano())
	filter := fmt.Sprintf("$share/probe-group/%v", base)

	readers := make([]*mqttReaderV5, 2)
	for i := range readers {
		readers[i] = readerOnPort(t, port, fmt.Sprintf("[ %v ]", filter),
			fmt.Sprintf("client_id: shared-%v-%v", i, time.Now().UnixNano()))
		require.NoError(t, readers[i].Connect(ctx))
		t.Cleanup(func() { _ = readers[i].Close(context.Background()) })
	}
	time.Sleep(500 * time.Millisecond)

	const total = 20
	publisher, err := connectV5(ctx, port, "shared-publisher")
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	for i := range total {
		_, err := publisher.Publish(ctx, &paho.Publish{
			Topic: base, QoS: 1, Payload: fmt.Appendf(nil, "%v", i),
		})
		require.NoError(t, err)
	}

	var mut sync.Mutex
	seen := map[string]int{}
	perReader := make([]int, len(readers))

	var wg sync.WaitGroup
	collectCtx, collectCancel := context.WithTimeout(ctx, 25*time.Second)
	defer collectCancel()

	for i, rdr := range readers {
		wg.Go(func() {
			for {
				msg, ack, err := rdr.Read(collectCtx)
				if err != nil {
					return
				}
				_ = ack(collectCtx, nil)
				payload, err := msg.AsBytes()
				if err != nil {
					return
				}
				mut.Lock()
				seen[string(payload)]++
				perReader[i]++
				done := len(seen) == total
				mut.Unlock()
				if done {
					collectCancel()
					return
				}
			}
		})
	}
	wg.Wait()

	mut.Lock()
	defer mut.Unlock()
	assert.Len(t, seen, total, "not every message arrived across the group")
	for payload, count := range seen {
		assert.Equal(t, 1, count, "message %v was delivered to more than one member of the group", payload)
	}
	// Which member gets what is the server's business, so no count is asserted
	// beyond both having been able to receive at all.
	t.Logf("split across the group: %v", perReader)
}

// testRejectionWarnsV5 covers the warning that stops a stalled input being
// silent. Acknowledgements can only be sent in arrival order, so a message that
// is never acknowledged holds back every one behind it, and once the receive
// window is full the server stops delivering while the input still reports
// itself healthy. Saying so at the first rejection is the whole mitigation, so
// an untested log line would be no mitigation at all.
func testRejectionWarnsV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("reject-warn-%v", time.Now().UnixNano())
	rdr, captured := readerOnPortLogged(t, port, fmt.Sprintf("[ %v ]", topic), fmt.Sprintf(
		"client_id: reject-warn-%v\nreceive_maximum: 4", time.Now().UnixNano()))
	t.Cleanup(func() { _ = rdr.Close(context.Background()) })
	require.NoError(t, rdr.Connect(ctx))
	time.Sleep(500 * time.Millisecond)

	publisher, err := connectV5(ctx, port, "reject-warn-publisher")
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	_, err = publisher.Publish(ctx, &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte("cannot be handled"),
	})
	require.NoError(t, err)

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	_, ack, err := rdr.Read(readCtx)
	require.NoError(t, err)

	require.NotContains(t, captured.String(), "was rejected", "the warning fired before anything was rejected")
	require.NoError(t, ack(readCtx, assert.AnError))

	logged := captured.String()
	assert.Contains(t, logged, "was rejected", "a rejection was not reported at all")
	assert.Contains(t, logged, topic, "the warning does not say which message")
	assert.Contains(t, logged, "4 are outstanding", "the warning does not say when delivery will stop")
	assert.Contains(t, logged, "dead-letter", "the warning does not say what to do instead")
}

// testAckIsNotRedeliveredV5 is the control for the test below it. Without it a
// redelivery test passes just as well against a component that never
// acknowledges anything at all.
func testAckIsNotRedeliveredV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("ack-%v", time.Now().UnixNano())
	clientID := fmt.Sprintf("ack-client-%v", time.Now().UnixNano())
	extra := fmt.Sprintf("client_id: %v\nclean_start: false\nsession_expiry_interval: 300s", clientID)

	first := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	require.NoError(t, first.Connect(ctx))
	time.Sleep(500 * time.Millisecond)

	publisher, err := connectV5(ctx, port, "ack-publisher")
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	_, err = publisher.Publish(ctx, &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte("handled once"),
	})
	require.NoError(t, err)

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()

	_, ack, err := first.Read(readCtx)
	require.NoError(t, err)
	require.NoError(t, ack(readCtx, nil))

	// Long enough for the acknowledgement ticker to have sent it.
	time.Sleep(time.Second)
	require.NoError(t, first.Close(ctx))

	second := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	require.NoError(t, second.Connect(ctx))

	againCtx, againCancel := context.WithTimeout(ctx, 5*time.Second)
	defer againCancel()

	_, _, err = second.Read(againCtx)
	require.Error(t, err, "an acknowledged message was delivered a second time")
}

// testNackIsRedeliveredV5 covers the contract the component exists for: a
// message the pipeline could not handle is not acknowledged, so the server
// keeps it and delivers it again.
func testNackIsRedeliveredV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("nack-%v", time.Now().UnixNano())
	clientID := fmt.Sprintf("nack-client-%v", time.Now().UnixNano())
	extra := fmt.Sprintf("client_id: %v\nclean_start: false\nsession_expiry_interval: 300s", clientID)

	first := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	require.NoError(t, first.Connect(ctx))
	time.Sleep(500 * time.Millisecond)

	publisher, err := connectV5(ctx, port, "nack-publisher")
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	_, err = publisher.Publish(ctx, &paho.Publish{
		Topic: topic, QoS: 1, Payload: []byte("needs redelivery"),
	})
	require.NoError(t, err)

	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()

	msg, ack, err := first.Read(readCtx)
	require.NoError(t, err)
	payload, err := msg.AsBytes()
	require.NoError(t, err)
	require.Equal(t, "needs redelivery", string(payload))

	// Reject it. Nothing is acknowledged, so the server still owns it.
	require.NoError(t, ack(readCtx, assert.AnError))

	// Wait out the acknowledgement interval before disconnecting. Ack only
	// marks a packet and a ticker sends the batch, so closing straight away
	// leaves the acknowledgement unsent whether or not one was asked for —
	// which makes redelivery happen for the wrong reason and the test pass
	// against its own defect. It did exactly that when first written.
	time.Sleep(time.Second)
	require.NoError(t, first.Close(ctx))

	// A new connection under the same session must be given it again.
	second := readerOnPort(t, port, fmt.Sprintf("[ %v ]", topic), extra)
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	require.NoError(t, second.Connect(ctx))

	againCtx, againCancel := context.WithTimeout(ctx, 20*time.Second)
	defer againCancel()

	redelivered, ackAgain, err := second.Read(againCtx)
	require.NoError(t, err, "a rejected message was not redelivered")
	require.NoError(t, ackAgain(againCtx, nil))

	payload, err = redelivered.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "needs redelivery", string(payload))
}

// testBentoDrivenReconnectV5 covers the half of a reconnection no unit test can
// reach. A dropped link has to be reported as service.ErrNotConnected rather
// than waited through, so that Bento counts a lost connection as one and
// applies its own backoff; nothing may quietly reconnect behind it; and the
// connection Bento then asks for has to arrive subscribed, because a client
// that reconnected without re-subscribing reads as a healthy input receiving
// nothing at all.
//
// The session is deliberately not durable here: with nothing kept on the
// server, a message can only arrive after the reconnection if this input
// really did subscribe again. The subscription log is what counts them, one
// line per SUBACK.
func testBentoDrivenReconnectV5(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	stamp := time.Now().UnixNano()
	topic := fmt.Sprintf("reconnect-%v", stamp)
	rdr, captured := readerOnPortLogged(t, port, fmt.Sprintf("[ %v ]", topic),
		fmt.Sprintf("client_id: reconnect-in-%v", stamp))
	t.Cleanup(func() { _ = rdr.Close(context.Background()) })

	// The summary line, one per SUBACK. Counting "Subscribed to" instead would
	// also count the debug line each granted filter logs.
	subscribes := func() int {
		return strings.Count(captured.String(), "topic filter(s)")
	}

	require.NoError(t, rdr.Connect(ctx))
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 1, subscribes(), "the first connection did not subscribe once")

	publisher, err := connectV5(ctx, port, fmt.Sprintf("reconnect-pub-%v", stamp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Disconnect(context.Background()) })

	publish := func(payload string) {
		t.Helper()
		_, err := publisher.Publish(ctx, &paho.Publish{
			Topic: topic, QoS: 1, Payload: []byte(payload),
		})
		require.NoError(t, err)
	}
	readOne := func() string {
		t.Helper()
		readCtx, cancelRead := context.WithTimeout(ctx, 20*time.Second)
		defer cancelRead()
		msg, ack, err := rdr.Read(readCtx)
		require.NoError(t, err)
		require.NoError(t, ack(readCtx, nil))
		body, err := msg.AsBytes()
		require.NoError(t, err)
		return string(body)
	}

	publish("before the drop")
	assert.Equal(t, "before the drop", readOne())

	// What a network failure looks like from underneath the client library.
	dropped := rdr.conn.manager()
	require.NotNil(t, dropped)
	dropped.TerminateConnectionForTest()

	downCtx, cancelDown := context.WithTimeout(ctx, 20*time.Second)
	defer cancelDown()
	_, _, err = rdr.Read(downCtx)
	require.ErrorIs(t, err, service.ErrNotConnected,
		"a read that met the dropped link did not report it to Bento")

	// Nothing brings the link back on its own. A silent reconnection would
	// fire the connection-up handler and subscribe a second time.
	time.Sleep(3 * time.Second)
	require.Equal(t, 1, subscribes(), "something reconnected and subscribed behind Bento")

	// What Bento does about it. The manager that dropped is released rather
	// than left running, and a fresh one takes its place.
	require.NoError(t, rdr.Connect(ctx))
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 2, subscribes(), "the reconnection did not subscribe again")

	select {
	case <-dropped.Done():
	case <-time.After(20 * time.Second):
		t.Fatal("the manager whose connection dropped was left running")
	}
	assert.NotSame(t, dropped, rdr.conn.manager(), "the reconnection reused a stopped manager")

	publish("after the reconnect")
	assert.Equal(t, "after the reconnect", readOne())
}
