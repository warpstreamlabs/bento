package mqtt

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// syncBuffer is a bytes.Buffer that can be read from a different goroutine than
// the one writing it. The client library logs from its own goroutines, so a
// plain buffer gives the reading test no guarantee of ever seeing those writes
// — which reads as "nothing was logged" rather than as the race it is.
type syncBuffer struct {
	mut sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.buf.String()
}

// capturedLogger returns a logger writing into a buffer, and the buffer. The
// reason-code handling is a log line as much as a control-flow decision, so an
// assertion that never reads the log proves only half of it.
func capturedLogger() (*service.Resources, *syncBuffer) {
	captured := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return service.MockResources(service.MockResourcesOptUseSlogger(logger)), captured
}

// waitForLog polls until the log contains want. It exists because
// require.Eventually evaluates its failure message when it is called rather
// than when it fails, so a buffer passed there is always reported empty — which
// looks exactly like "the component logged nothing" and sent one debugging
// session down the wrong path.
func waitForLog(t *testing.T, captured *syncBuffer, want, why string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(captured.String(), want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%v: never logged %q.\nlog was:\n%v", why, want, captured.String())
}

func readerFor(t *testing.T, yaml string) (*mqttReaderV5, *syncBuffer) {
	t.Helper()
	conf, err := inputConfigSpecV5().ParseYAML(yaml, nil)
	require.NoError(t, err)
	res, captured := capturedLogger()
	rdr, err := newMQTTReaderV5FromParsed(conf, res)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdr.Close(context.Background()) })
	return rdr, captured
}

func writerFor(t *testing.T, yaml string) (*mqttWriterV5, *syncBuffer) {
	t.Helper()
	conf, err := outputConfigSpecV5().ParseYAML(yaml, nil)
	require.NoError(t, err)
	res, captured := capturedLogger()
	w, err := newMQTTWriterV5FromParsed(conf, res)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w, captured
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestRefusedSubscribeIsLoggedWithItsCode covers the finding the whole design
// rests on: a refused subscription does not close the connection, so the input
// stays connected and healthy while receiving nothing. Silence is the one
// outcome that must not happen.
func TestRefusedSubscribeIsLoggedWithItsCode(t *testing.T) {
	server := newStubServerV5(t, stubConfig{suback: []byte{0x87}})

	rdr, captured := readerFor(t, stubInputYAML(server, "[ events/# ]", ""))

	// The connection itself succeeds — that is exactly the problem.
	require.NoError(t, rdr.Connect(testCtx(t)))

	// Wait for the retry line rather than the reason code: it is logged after
	// the refusal, so waiting for the code and then snapshotting can catch the
	// log between the two writes. That is what made this test flaky.
	waitForLog(t, captured, "Retrying the subscription", "a refused subscription under the default policy")
	logged := captured.String()
	assert.Contains(t, logged, "0x87", "the reason code was not logged")
	assert.Contains(t, logged, "not authorized", "the reason code was not decoded")
	assert.Contains(t, logged, "events/#", "the log does not say which filter was refused")
}

// TestRefusedSubscribeCanStopTheInput covers on_subscribe_refused: fail, for a
// pipeline that would rather fail than sit connected receiving nothing.
func TestRefusedSubscribeCanStopTheInput(t *testing.T) {
	server := newStubServerV5(t, stubConfig{suback: []byte{0x8F}})

	rdr, _ := readerFor(t, stubInputYAML(server, "[ events/# ]", "on_subscribe_refused: fail"))
	ctx := testCtx(t)
	require.NoError(t, rdr.Connect(ctx))

	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	_, _, err := rdr.Read(readCtx)
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrEndOfInput, "a refusal under `fail` must end the input, got: %v", err)
	assert.Contains(t, err.Error(), "0x8F", "the error does not carry the reason code")
	assert.Contains(t, err.Error(), "topic filter invalid")
}

// TestRefusedSubscribeCanContinueOnTheRest covers on_subscribe_refused:
// continue — one filter of several refused, and the pipeline runs on the others
// rather than delivering nothing at all.
func TestRefusedSubscribeCanContinueOnTheRest(t *testing.T) {
	server := newStubServerV5(t, stubConfig{suback: []byte{0x01, 0x87}}) // First granted, second refused.

	rdr, captured := readerFor(t, stubInputYAML(server, "[ events/#, secrets/# ]", "on_subscribe_refused: continue"))
	require.NoError(t, rdr.Connect(testCtx(t)))

	waitForLog(t, captured, "0x87", "a refusal among granted filters")
	logged := captured.String()
	assert.Contains(t, logged, "secrets/#", "the log names the wrong filter")
	assert.NotContains(t, logged, "Subscription to events/# refused", "a granted filter was reported as refused")
	assert.Contains(t, logged, "Carrying on with the filters that were granted")
	assert.NotContains(t, logged, "Retrying the subscription", "continue must not also retry")
}

// TestRefusedConnectIsLoggedWithItsCode covers the CONNACK half. Left to
// autopaho this is invisible: it retries for ever behind a Connect that never
// returns, so Bento's own log and failed-connection metric never fire and a
// wrong password looks like a healthy, idle pipeline.
func TestRefusedConnectIsLoggedWithItsCode(t *testing.T) {
	server := newStubServerV5(t, stubConfig{connack: 0x86})

	rdr, captured := readerFor(t, stubInputYAML(server, "[ events/# ]", "on_connect_refused: fail"))

	err := rdr.Connect(testCtx(t))
	require.Error(t, err, "a refused connection must not look like a healthy one")
	assert.ErrorIs(t, err, service.ErrEndOfInput, "under `fail` a refused connection must end the input")
	assert.Contains(t, err.Error(), "0x86")
	assert.Contains(t, err.Error(), "bad user name or password")

	waitForLog(t, captured, "Server refused the connection", "a refused connection")
	assert.Contains(t, captured.String(), "0x86")
}

// TestRefusedConnectRetriesByDefault is the other half of the same field: the
// default keeps trying, so a permission granted later recovers with no restart.
func TestRefusedConnectRetriesByDefault(t *testing.T) {
	server := newStubServerV5(t, stubConfig{connack: 0x87})

	rdr, captured := readerFor(t, stubInputYAML(server, "[ events/# ]", ""))

	err := rdr.Connect(testCtx(t))
	require.Error(t, err)
	assert.NotErrorIs(t, err, service.ErrEndOfInput, "the default must not end the input")

	waitForLog(t, captured, "0x87", "a refused connection")
	assert.Contains(t, captured.String(), "not authorized")
}

func stubOutputYAML(server *stubServerV5, qos int) string {
	return fmt.Sprintf(`
urls: [ %v ]
client_id: stub-probe-out
topic: events
qos: %v
connect_timeout: 5s
write_timeout: 10s
`, server.url(), qos)
}

// TestRefusedPublishCarriesTheReasonCode matters beyond the log: a fallback
// output copies this error into metadata and a switch routes on it, so the text
// is the interface a pipeline matches against.
func TestRefusedPublishCarriesTheReasonCode(t *testing.T) {
	server := newStubServerV5(t, stubConfig{puback: 0x87})

	writer, _ := writerFor(t, stubOutputYAML(server, 1))
	ctx := testCtx(t)
	require.NoError(t, writer.Connect(ctx))

	err := writer.Write(ctx, service.NewMessage([]byte("should be refused")))
	require.Error(t, err, "a publication the server refused was reported as delivered")
	assert.Contains(t, err.Error(), "0x87")
	assert.Contains(t, err.Error(), "not authorized")
	assert.NotErrorIs(t, err, service.ErrNotConnected, "a refusal is not a lost connection")
}

// TestRefusedPublishAtQoS2IsNotReportedAsDelivered is the one that cannot be
// tested against any real broker, and the one most worth having. The client
// library returns a PUBREC carrying a failure code with a nil error — it even
// logs "must have errored" to its own debug log first — so trusting the error
// return means telling the pipeline a rejected message was delivered.
func TestRefusedPublishAtQoS2IsNotReportedAsDelivered(t *testing.T) {
	server := newStubServerV5(t, stubConfig{pubrec: 0x87})

	writer, _ := writerFor(t, stubOutputYAML(server, 2))
	ctx := testCtx(t)
	require.NoError(t, writer.Connect(ctx))

	err := writer.Write(ctx, service.NewMessage([]byte("should be refused")))
	require.Error(t, err, "a QoS 2 publication the server refused was reported as delivered")
	assert.Contains(t, err.Error(), "0x87")
	assert.Contains(t, err.Error(), "not authorized")
}

// TestAcceptedPublishSucceeds is the control for the two above: without it they
// pass just as well against a writer that fails on everything.
func TestAcceptedPublishSucceeds(t *testing.T) {
	for _, qos := range []int{1, 2} {
		t.Run(fmt.Sprintf("qos %v", qos), func(t *testing.T) {
			server := newStubServerV5(t, stubConfig{})

			writer, _ := writerFor(t, stubOutputYAML(server, qos))
			ctx := testCtx(t)
			require.NoError(t, writer.Connect(ctx))
			require.NoError(t, writer.Write(ctx, service.NewMessage([]byte("accepted"))))

			require.Eventually(t, func() bool { return len(server.publishes()) > 0 },
				10*time.Second, 20*time.Millisecond, "the server never received the publication")
			assert.Equal(t, []byte("accepted"), server.publishes()[0].Payload)
		})
	}
}

// TestNoMatchingSubscribersIsNotAFailure guards a code that reads like an error
// and is not one: 0x10 means the server accepted the message and nobody was
// listening.
func TestNoMatchingSubscribersIsNotAFailure(t *testing.T) {
	server := newStubServerV5(t, stubConfig{puback: 0x10})

	writer, _ := writerFor(t, stubOutputYAML(server, 1))
	ctx := testCtx(t)
	require.NoError(t, writer.Connect(ctx))
	assert.NoError(t, writer.Write(ctx, service.NewMessage([]byte("nobody listening"))),
		"0x10 no matching subscribers is a success and must not fail the message")
}
