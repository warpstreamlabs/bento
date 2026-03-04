package kafka

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	// Import io components for file output
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestIntegrationKafkaServerPerfTest(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	const numRecords = 10000
	const recordSizeBytes = 100

	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "perf_output.txt")

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "30s"
output:
  file:
    path: "%s"
    codec: lines
`, port, hostAddr, outputFile)

	capture := &messageCapture{}

	builder := service.NewStreamBuilder()
	err := builder.SetYAML(config)
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

	// Use a longer timeout for the perf test to avoid context cancellation on slow CI.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stream.Run(ctx)
	}()

	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	pool := newDockerPool(t)
	client := newDockerExecClient(t, pool, dockerContainerOpts{
		repository: "apache/kafka",
		tag:        "4.1.1",
		cmd:        []string{"sleep", "infinity"},
		readyCmd:   []string{"echo", "ready"},
	})
	t.Cleanup(func() { client.Close() })

	// Run kafka-producer-perf-test.sh against the bento kafka_server.
	cmd := []string{
		"/opt/kafka/bin/kafka-producer-perf-test.sh",
		"--topic", "perf-test-topic",
		"--num-records", fmt.Sprintf("%d", numRecords),
		"--record-size", fmt.Sprintf("%d", recordSizeBytes),
		"--throughput", "-1",
		"--producer-props",
		fmt.Sprintf("bootstrap.servers=%s", hostAddr),
		"acks=1",
		"retries=0",
		"delivery.timeout.ms=30000",
		"request.timeout.ms=5000",
	}

	t.Logf("Running kafka-producer-perf-test.sh: %v", cmd)
	stdout, stderr, err := client.exec(cmd)
	t.Logf("kafka-producer-perf-test.sh stdout:\n%s", stdout)
	if stderr != "" {
		t.Logf("kafka-producer-perf-test.sh stderr:\n%s", stderr)
	}
	require.NoError(t, err, "kafka-producer-perf-test.sh failed")

	// Wait for all messages to arrive through bento using the efficient count-only check.
	require.True(t, capture.waitForCount(numRecords, 60*time.Second),
		"expected all %d records from kafka-producer-perf-test.sh to arrive through bento", numRecords)

	t.Logf("Successfully received all %d messages from kafka-producer-perf-test.sh through bento", numRecords)

	cancel()
	wg.Wait()

	// Count the lines written to the output file.
	f, err := os.Open(outputFile)
	require.NoError(t, err, "failed to open output file")
	defer f.Close()

	lineCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineCount++
	}
	require.NoError(t, scanner.Err(), "error reading output file")
	require.Equal(t, numRecords, lineCount, "expected %d lines in output file, got %d", numRecords, lineCount)
	t.Logf("Output file contains %d lines, matching expected %d records", lineCount, numRecords)
}
