package kafka

import (
	"fmt"
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

// kcatClient wraps a Docker container running kafkacat.
type kcatClient struct {
	dockerExecClient
}

// Compile-time check that kcatClient implements kafkaProducer.
var _ kafkaProducer = (*kcatClient)(nil)

// kcatAuthFailurePatterns are output patterns indicating authentication failure from kafkacat.
var kcatAuthFailurePatterns = []string{
	"Authentication failed",
	"SASL authentication error",
	"Disconnected",
	"EXIT_CODE=1",
	"timed out",
}

const kcatScramSkipReason = "SCRAM tests disabled for kcat due to librdkafka nonce doubling bug, see https://github.com/edenhill/kcat/issues/462 (fixed in https://github.com/confluentinc/librdkafka/commit/816df5ea328acfbf99a883764b3e7c46f51a91b9)"

// newKcatClient creates a new Docker container with kafkacat.
func newKcatClient(t *testing.T) *kcatClient {
	pool := newDockerPool(t)
	return &kcatClient{
		dockerExecClient: newDockerExecClient(t, pool, dockerContainerOpts{
			repository: "confluentinc/cp-kafkacat",
			tag:        "7.1.16",
			cmd:        []string{"sh", "-c", "sleep infinity"},
			readyCmd:   []string{"sh", "-c", "echo ready"},
		}),
	}
}

// shellEscape does minimal escaping for use inside single-quoted sh strings.
func shellEscape(s string) string {
	return strings.ReplaceAll(s, "'", `'\''`)
}

func (c *kcatClient) produceMessage(brokerAddr, topic, value string) error {
	cmd := []string{"sh", "-c", fmt.Sprintf("printf '%%s' '%s' | kafkacat -P -b %s -t %s", shellEscape(value), brokerAddr, topic)}
	return c.runProduce(fmt.Sprintf("message to %s topic=%s", brokerAddr, topic), cmd)
}

func (c *kcatClient) produceMessageWithKey(brokerAddr, topic, key, value string) error {
	cmd := []string{"sh", "-c", fmt.Sprintf("printf '%%s' '%s:%s' | kafkacat -P -b %s -t %s -K :", shellEscape(key), shellEscape(value), brokerAddr, topic)}
	return c.runProduce(fmt.Sprintf("message with key to %s topic=%s key=%s", brokerAddr, topic, key), cmd)
}

func (c *kcatClient) produceMultipleMessages(brokerAddr, topic string, messages []string) error {
	escaped := make([]string, len(messages))
	for i, m := range messages {
		escaped[i] = shellEscape(m)
	}
	input := strings.Join(escaped, "\\n")
	cmd := []string{"sh", "-c", fmt.Sprintf("printf '%s\\n' | kafkacat -P -b %s -t %s", input, brokerAddr, topic)}
	return c.runProduce(fmt.Sprintf("%d messages to %s topic=%s", len(messages), brokerAddr, topic), cmd)
}

func (c *kcatClient) produceMessageIdempotent(brokerAddr, topic, value string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' | kafkacat -P -b %s -t %s -X enable.idempotence=true -X acks=all",
			shellEscape(value), brokerAddr, topic),
	}
	return c.runProduce(fmt.Sprintf("idempotent message to %s topic=%s", brokerAddr, topic), cmd)
}

func (c *kcatClient) produceWithSASLPlain(brokerAddr, topic, value, username, password string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' | kafkacat -P -b %s -t %s -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN -X sasl.username=%s -X sasl.password=%s",
			shellEscape(value), brokerAddr, topic, username, password),
	}
	return c.runProduce(fmt.Sprintf("SASL PLAIN message to %s topic=%s user=%s", brokerAddr, topic, username), cmd)
}

func (c *kcatClient) produceWithSASLPlainExpectFailure(brokerAddr, topic, value, username, password string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' | timeout 10 kafkacat -P -b %s -t %s -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN -X sasl.username=%s -X sasl.password=%s 2>&1; echo EXIT_CODE=$?",
			shellEscape(value), brokerAddr, topic, username, password),
	}
	return c.runProduceExpectFailure(
		fmt.Sprintf("SASL PLAIN message to %s topic=%s user=%s", brokerAddr, topic, username),
		cmd, kcatAuthFailurePatterns,
	)
}

func (c *kcatClient) produceWithSASLScram(brokerAddr, topic, value, username, password, mechanism string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' | kafkacat -P -b %s -t %s -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=%s -X sasl.username=%s -X sasl.password=%s",
			shellEscape(value), brokerAddr, topic, mechanism, username, password),
	}
	return c.runProduce(fmt.Sprintf("SASL %s message to %s topic=%s user=%s", mechanism, brokerAddr, topic, username), cmd)
}

func (c *kcatClient) produceWithSASLScramExpectFailure(brokerAddr, topic, value, username, password, mechanism string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' | timeout 10 kafkacat -P -b %s -t %s -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=%s -X sasl.username=%s -X sasl.password=%s 2>&1; echo EXIT_CODE=$?",
			shellEscape(value), brokerAddr, topic, mechanism, username, password),
	}
	return c.runProduceExpectFailure(
		fmt.Sprintf("SASL %s message to %s topic=%s user=%s", mechanism, brokerAddr, topic, username),
		cmd, kcatAuthFailurePatterns,
	)
}

// --- Top-level test functions ---

func TestIntegrationKafkaServerKcat(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	client := newKcatClient(t)
	t.Cleanup(func() { client.Close() })

	runKafkaServerSubtests(t, client, kcatScramSkipReason)
}

func TestIntegrationKafkaServerMultipleUsersKcat(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	client := newKcatClient(t)
	t.Cleanup(func() { client.Close() })

	testKafkaServerMultipleUsers(t, client)
}
