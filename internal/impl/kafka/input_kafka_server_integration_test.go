package kafka

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	// Import all standard Bento components
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// --- Shared utilities ---

// getHostAddress returns the address to use from inside a Docker container to reach the host.
func getHostAddress() string {
	return "host.docker.internal"
}

func getFreePort(t *testing.T) int {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	return port
}

// waitForTCPReady polls until the given TCP address is accepting connections.
func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// --- Message capture ---

// receivedMessage holds a captured message from kafka_server.
type receivedMessage struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// messageCapture captures messages received by kafka_server.
type messageCapture struct {
	messages []receivedMessage
	mu       sync.Mutex
}

func (mc *messageCapture) add(msg receivedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.messages = append(mc.messages, msg)
}

func (mc *messageCapture) get() []receivedMessage {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	result := make([]receivedMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

func (mc *messageCapture) waitForMessages(count int, timeout time.Duration) []receivedMessage {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := mc.get()
		if len(msgs) >= count {
			return msgs
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mc.get()
}

// waitForCount waits until at least count messages have been captured, checking only the
// length under lock without copying. Returns true if the count was reached.
func (mc *messageCapture) waitForCount(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mc.mu.Lock()
		n := len(mc.messages)
		mc.mu.Unlock()
		if n >= count {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// --- kafkaProducer interface ---

// kafkaProducer is an interface that both kafkaDockerClient and kcatClient implement,
// allowing reuse of common integration test logic.
type kafkaProducer interface {
	Close()
	produceMessage(brokerAddr, topic, value string) error
	produceMessageWithKey(brokerAddr, topic, key, value string) error
	produceMultipleMessages(brokerAddr, topic string, messages []string) error
	produceMessageIdempotent(brokerAddr, topic, value string) error
	produceWithSASLPlain(brokerAddr, topic, value, username, password string) error
	produceWithSASLPlainExpectFailure(brokerAddr, topic, value, username, password string) error
	produceWithSASLScram(brokerAddr, topic, value, username, password, mechanism string) error
	produceWithSASLScramExpectFailure(brokerAddr, topic, value, username, password, mechanism string) error
}

// --- Docker container helpers ---

// newDockerPool creates a new Docker connection pool for integration tests.
func newDockerPool(t *testing.T) *dockertest.Pool {
	t.Helper()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to Docker: %v", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Could not ping Docker: %v", err)
	}
	pool.MaxWait = time.Minute
	return pool
}

// dockerExecClient provides shared functionality for running commands in a Docker container.
type dockerExecClient struct {
	pool     *dockertest.Pool
	resource *dockertest.Resource
	t        *testing.T
}

// dockerContainerOpts configures a Docker container for testing.
type dockerContainerOpts struct {
	repository string
	tag        string
	cmd        []string
	readyCmd   []string
}

// newDockerExecClient creates a Docker container and returns an exec client for it.
func newDockerExecClient(t *testing.T, pool *dockertest.Pool, opts dockerContainerOpts) dockerExecClient {
	t.Helper()

	runOpts := &dockertest.RunOptions{
		Repository: opts.repository,
		Tag:        opts.tag,
		Cmd:        opts.cmd,
	}

	if runtime.GOOS == "linux" {
		runOpts.ExtraHosts = []string{"host.docker.internal:host-gateway"}
	}

	resource, err := pool.RunWithOptions(runOpts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Skipf("Could not start %s Docker container: %v", opts.repository, err)
	}

	_ = resource.Expire(300)

	dec := dockerExecClient{pool: pool, resource: resource, t: t}

	err = pool.Retry(func() error {
		_, _, execErr := dec.exec(opts.readyCmd)
		return execErr
	})
	if err != nil {
		t.Skipf("%s container did not become ready: %v", opts.repository, err)
	}

	return dec
}

// Close removes the Docker container.
func (c *dockerExecClient) Close() {
	if c.resource != nil {
		_ = c.pool.Purge(c.resource)
	}
}

// exec runs a command in the Docker container and returns stdout, stderr, and any error.
func (c *dockerExecClient) exec(cmd []string) (string, string, error) {
	var stdout, stderr bytes.Buffer

	exitCode, err := c.resource.Exec(cmd, dockertest.ExecOptions{
		StdOut: &stdout,
		StdErr: &stderr,
	})
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	if exitCode != 0 {
		return stdout.String(), stderr.String(), fmt.Errorf("command exited with code %d: stdout=%s stderr=%s", exitCode, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String(), nil
}

// runProduce executes a produce command and handles logging/error wrapping.
func (c *dockerExecClient) runProduce(label string, cmd []string) error {
	c.t.Logf("Producing %s", label)
	stdout, stderr, err := c.exec(cmd)
	if err != nil {
		c.t.Logf("stdout: %s", stdout)
		c.t.Logf("stderr: %s", stderr)
		return fmt.Errorf("%s failed: %w", label, err)
	}
	c.t.Logf("produce completed: %s", label)
	return nil
}

// runProduceExpectFailure executes a produce command expected to fail authentication.
func (c *dockerExecClient) runProduceExpectFailure(label string, cmd []string, failurePatterns []string) error {
	c.t.Logf("Producing %s (expect failure)", label)
	stdout, stderr, err := c.exec(cmd)
	combined := stdout + stderr
	c.t.Logf("output: %s", combined)
	if err != nil {
		return err
	}
	for _, pattern := range failurePatterns {
		if strings.Contains(combined, pattern) {
			return fmt.Errorf("authentication failed: %s", combined)
		}
	}
	return nil
}

// --- Shared test harness ---

// runKafkaServerTestWithCapture runs a kafka_server and captures received messages.
func runKafkaServerTestWithCapture(t *testing.T, port int, configYAML string, testFn func(ctx context.Context, capture *messageCapture)) {
	t.Helper()

	capture := &messageCapture{}

	// Ensure configYAML has an output section so the stream builder
	// doesn't create a default stdout output that blocks acknowledgments.
	if !strings.Contains(configYAML, "output:") {
		configYAML += "\noutput:\n  drop: {}\n"
	}

	builder := service.NewStreamBuilder()
	err := builder.SetYAML(configYAML)
	require.NoError(t, err)

	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		value, _ := msg.AsBytes()
		key, _ := msg.MetaGet("kafka_server_key")
		topic, _ := msg.MetaGet("kafka_server_topic")

		headers := make(map[string]string)
		_ = msg.MetaWalk(func(k, v string) error {
			headers[k] = v
			return nil
		})

		capture.add(receivedMessage{
			Topic:   topic,
			Key:     key,
			Value:   string(value),
			Headers: headers,
		})
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stream.Run(ctx)
	}()

	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	testFn(ctx, capture)

	cancel()
	wg.Wait()
}

// --- Common test scenarios ---
// These use the kafkaProducer interface so they work with any client (kafka CLI or kcat).

func testKafkaServerBasicNoAuth(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
output:
  drop: {}
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessage(hostAddr, "test-topic", `{"message": "hello from docker"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "test-topic", msgs[0].Topic)
		assert.Equal(t, `{"message": "hello from docker"}`, msgs[0].Value)
		t.Logf("Basic no-auth message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerMultipleMessages(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		messages := make([]string, 10)
		for i := 0; i < 10; i++ {
			messages[i] = fmt.Sprintf(`{"index": %d}`, i)
		}

		err := client.produceMultipleMessages(hostAddr, "multi-topic", messages)
		require.NoError(t, err)

		msgs := capture.waitForMessages(10, 10*time.Second)
		require.Len(t, msgs, 10, "Expected exactly 10 messages")
		for i, msg := range msgs {
			assert.Equal(t, "multi-topic", msg.Topic)
			assert.Equal(t, fmt.Sprintf(`{"index": %d}`, i), msg.Value)
		}
		t.Logf("Multiple messages received: count=%d", len(msgs))
	})
}

func testKafkaServerSASLPlain(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "testuser"
        password: "testpass"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlain(hostAddr, "auth-topic", `{"auth": "PLAIN"}`, "testuser", "testpass")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "auth-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "PLAIN"}`, msgs[0].Value)
		t.Logf("SASL PLAIN message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLPlainWrongPassword(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "testuser"
        password: "testpass"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlainExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "testuser", "wrongpassword")
		assert.Error(t, err)
		t.Logf("SASL PLAIN wrong password correctly rejected: %v", err)

		msgs := capture.waitForMessages(1, 2*time.Second)
		assert.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerSASLScram256(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram-topic", `{"auth": "SCRAM-SHA-256"}`, "scramuser", "scrampass", "SCRAM-SHA-256")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "scram-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "SCRAM-SHA-256"}`, msgs[0].Value)
		t.Logf("SASL SCRAM-SHA-256 message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLScram512(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-512
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram512-topic", `{"auth": "SCRAM-SHA-512"}`, "scramuser", "scrampass", "SCRAM-SHA-512")
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "scram512-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "SCRAM-SHA-512"}`, msgs[0].Value)
		t.Logf("SASL SCRAM-SHA-512 message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLScramWrongPassword(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScramExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "scramuser", "wrongpassword", "SCRAM-SHA-256")
		assert.Error(t, err)
		t.Logf("SASL SCRAM wrong password correctly rejected: %v", err)

		msgs := capture.waitForMessages(1, 2*time.Second)
		assert.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerMessageWithKey(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessageWithKey(hostAddr, "key-topic", "my-key", `{"test": "with_key"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "key-topic", msgs[0].Topic)
		assert.Equal(t, "my-key", msgs[0].Key)
		assert.Equal(t, `{"test": "with_key"}`, msgs[0].Value)
		t.Logf("Message with key received: topic=%s, key=%s, value=%s", msgs[0].Topic, msgs[0].Key, msgs[0].Value)
	})
}

func testKafkaServerIdempotentProducer(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    idempotent_write: true
output:
  drop: {}
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessageIdempotent(hostAddr, "idempotent-topic", `{"message": "hello from idempotent producer"}`)
		require.NoError(t, err)

		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "idempotent-topic", msgs[0].Topic)
		assert.Equal(t, `{"message": "hello from idempotent producer"}`, msgs[0].Value)
		t.Logf("Idempotent producer message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerMultipleUsers(t *testing.T, client kafkaProducer) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "user1"
        password: "pass1"
      - mechanism: PLAIN
        username: "user2"
        password: "pass2"
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlain(hostAddr, "user1-topic", `{"user": "user1"}`, "user1", "pass1")
		require.NoError(t, err)
		t.Log("User1 authenticated and sent message successfully")

		err = client.produceWithSASLPlain(hostAddr, "user2-topic", `{"user": "user2"}`, "user2", "pass2")
		require.NoError(t, err)
		t.Log("User2 authenticated and sent message successfully")

		msgs := capture.waitForMessages(2, 10*time.Second)
		require.Len(t, msgs, 2, "Expected exactly 2 messages")

		user1Msg := msgs[0]
		user2Msg := msgs[1]
		if user1Msg.Topic != "user1-topic" {
			user1Msg, user2Msg = user2Msg, user1Msg
		}

		assert.Equal(t, "user1-topic", user1Msg.Topic)
		assert.Equal(t, `{"user": "user1"}`, user1Msg.Value)
		assert.Equal(t, "user2-topic", user2Msg.Topic)
		assert.Equal(t, `{"user": "user2"}`, user2Msg.Value)
		t.Logf("Both users' messages received successfully")
	})
}

// --- Shared test dispatch ---

// runKafkaServerSubtests dispatches the standard set of kafka_server integration subtests.
// If scramSkipReason is non-empty, SCRAM-related tests are skipped with that reason.
func runKafkaServerSubtests(t *testing.T, client kafkaProducer, scramSkipReason string) {
	t.Run("basic_no_auth", func(t *testing.T) {
		t.Parallel()
		testKafkaServerBasicNoAuth(t, client)
	})

	t.Run("multiple_messages", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMultipleMessages(t, client)
	})

	t.Run("sasl_plain", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlain(t, client)
	})

	t.Run("sasl_plain_wrong_password", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlainWrongPassword(t, client)
	})

	t.Run("sasl_scram_sha256", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScram256(t, client)
	})

	t.Run("sasl_scram_sha512", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScram512(t, client)
	})

	t.Run("sasl_scram_wrong_password", func(t *testing.T) {
		if scramSkipReason != "" {
			t.Skip(scramSkipReason)
		}
		t.Parallel()
		testKafkaServerSASLScramWrongPassword(t, client)
	})

	t.Run("message_with_key", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMessageWithKey(t, client)
	})

	t.Run("idempotent_producer", func(t *testing.T) {
		t.Parallel()
		testKafkaServerIdempotentProducer(t, client)
	})
}
