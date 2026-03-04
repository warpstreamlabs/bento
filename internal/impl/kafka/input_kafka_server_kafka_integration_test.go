package kafka

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

// kafkaDockerClient wraps a Docker container running Kafka CLI tools.
type kafkaDockerClient struct {
	dockerExecClient
}

// Compile-time check that kafkaDockerClient implements kafkaProducer.
var _ kafkaProducer = (*kafkaDockerClient)(nil)

// kafkaAuthFailurePatterns are output patterns indicating authentication failure from kafka-console-producer.
var kafkaAuthFailurePatterns = []string{
	"SaslAuthenticationException",
	"Authentication failed",
	"TIMEOUT_OR_ERROR",
}

// newKafkaDockerClient creates a new Docker container with Kafka CLI tools.
func newKafkaDockerClient(t *testing.T) *kafkaDockerClient {
	pool := newDockerPool(t)
	return &kafkaDockerClient{
		dockerExecClient: newDockerExecClient(t, pool, dockerContainerOpts{
			repository: "apache/kafka",
			tag:        "4.1.1",
			cmd:        []string{"sleep", "infinity"},
			readyCmd:   []string{"echo", "ready"},
		}),
	}
}

func (c *kafkaDockerClient) produceMessage(brokerAddr, topic, value string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedValue, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("message to %s topic=%s", brokerAddr, topic), cmd)
}

func (c *kafkaDockerClient) produceMessageWithKey(brokerAddr, topic, key, value string) error {
	encodedKey := base64.StdEncoding.EncodeToString([]byte(key))
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "printf '%%s\t%%s\n' \"\$(echo %s | base64 -d)\" \"\$(echo %s | base64 -d)\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --property parse.key=true --property 'key.separator=	' --producer.config /tmp/producer.properties" 2>&1`,
			encodedKey, encodedValue, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("message with key to %s topic=%s", brokerAddr, topic), cmd)
}

func (c *kafkaDockerClient) produceMessageIdempotent(brokerAddr, topic, value string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=true
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=all
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedValue, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("idempotent message to %s topic=%s", brokerAddr, topic), cmd)
}

func (c *kafkaDockerClient) produceMultipleMessages(brokerAddr, topic string, messages []string) error {
	input := strings.Join(messages, "\n")
	encodedInput := base64.StdEncoding.EncodeToString([]byte(input))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedInput, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("%d messages to %s topic=%s", len(messages), brokerAddr, topic), cmd)
}

func (c *kafkaDockerClient) produceWithSASLPlain(brokerAddr, topic, value, username, password string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/plain.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/plain.properties" 2>&1`,
			username, password, encodedValue, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("SASL PLAIN message to %s topic=%s user=%s", brokerAddr, topic, username), cmd)
}

func (c *kafkaDockerClient) produceWithSASLPlainExpectFailure(brokerAddr, topic, value, username, password string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/plain.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=8000
acks=1
retries=0
PROPEOF
timeout 10 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/plain.properties 2>&1" || echo "TIMEOUT_OR_ERROR"`,
			username, password, encodedValue, brokerAddr, topic),
	}
	return c.runProduceExpectFailure(
		fmt.Sprintf("SASL PLAIN message to %s topic=%s user=%s", brokerAddr, topic, username),
		cmd, kafkaAuthFailurePatterns,
	)
}

func (c *kafkaDockerClient) produceWithSASLScram(brokerAddr, topic, value, username, password, mechanism string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/scram.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=%s
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/scram.properties" 2>&1`,
			mechanism, username, password, encodedValue, brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("SASL %s message to %s topic=%s user=%s", mechanism, brokerAddr, topic, username), cmd)
}

func (c *kafkaDockerClient) produceWithSASLScramExpectFailure(brokerAddr, topic, value, username, password, mechanism string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/scram.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=%s
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=8000
acks=1
retries=0
PROPEOF
timeout 10 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/scram.properties 2>&1" || echo "TIMEOUT_OR_ERROR"`,
			mechanism, username, password, encodedValue, brokerAddr, topic),
	}
	return c.runProduceExpectFailure(
		fmt.Sprintf("SASL %s message to %s topic=%s user=%s", mechanism, brokerAddr, topic, username),
		cmd, kafkaAuthFailurePatterns,
	)
}

// --- Top-level test functions ---

func TestIntegrationKafkaServer(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	client := newKafkaDockerClient(t)
	t.Cleanup(func() { client.Close() })

	runKafkaServerSubtests(t, client, "")
}

func TestIntegrationKafkaServerMultipleUsers(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	client := newKafkaDockerClient(t)
	t.Cleanup(func() { client.Close() })

	testKafkaServerMultipleUsers(t, client)
}
