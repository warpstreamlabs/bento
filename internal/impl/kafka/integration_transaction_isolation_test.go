package kafka_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationKafkaTransactionIsolation(t *testing.T) {
	integration.CheckSkipExact(t)

	address := startTransactionIsolationBroker(t)
	for _, input := range []string{"kafka", "kafka_franz"} {
		t.Run(input, func(t *testing.T) {
			testReadCommittedTransactionIsolation(t, address, input)
		})
	}
}

func startTransactionIsolationBroker(t *testing.T) string {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	portString := strconv.Itoa(port)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: portString}},
		},
		Cmd: []string{
			"redpanda", "start", "--smp", "1", "--overprovisioned",
			"--kafka-addr", "0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%d", port),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(900)

	address := "localhost:" + portString
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(context.Background(), address, "transaction-isolation-ready", 1)
	}))
	return address
}

func testReadCommittedTransactionIsolation(t *testing.T, address, input string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := fmt.Sprintf("transaction-isolation-%s-%d", input, time.Now().UnixNano())
	topic := "topic-" + id
	require.NoError(t, createKafkaTopic(ctx, address, id, 1))

	transactionalProducer, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.TransactionalID("producer-"+id),
	)
	require.NoError(t, err)
	defer transactionalProducer.Close()

	nonTransactionalProducer, err := kgo.NewClient(kgo.SeedBrokers(address))
	require.NoError(t, err)
	defer nonTransactionalProducer.Close()

	produceRecord(t, ctx, nonTransactionalProducer, topic, "before-open-transaction")

	reader := startReadCommittedInput(t, ctx, input, address, topic, "consumer-"+id)
	defer reader.stop(t)
	reader.expect(t, "before-open-transaction")

	// Once an open transaction begins, read_committed consumers must stop at
	// the last stable offset, even when a later non-transactional record exists.
	require.NoError(t, transactionalProducer.BeginTransaction())
	produceRecord(t, ctx, transactionalProducer, topic, "open-transaction")
	produceRecord(t, ctx, nonTransactionalProducer, topic, "after-open-transaction")
	reader.expectNone(t, time.Second)

	require.NoError(t, transactionalProducer.EndTransaction(ctx, kgo.TryAbort))
	reader.expect(t, "after-open-transaction")

	produceTransaction(t, ctx, transactionalProducer, topic, "committed-transaction", kgo.TryCommit)
	reader.expect(t, "committed-transaction")

	produceTransaction(t, ctx, transactionalProducer, topic, "aborted-transaction", kgo.TryAbort)
	produceRecord(t, ctx, nonTransactionalProducer, topic, "after-aborted-transaction")
	reader.expect(t, "after-aborted-transaction")
	reader.expectNone(t, time.Second)
}

func produceTransaction(
	t *testing.T,
	ctx context.Context,
	producer *kgo.Client,
	topic,
	value string,
	end kgo.TransactionEndTry,
) {
	t.Helper()
	require.NoError(t, producer.BeginTransaction())
	produceRecord(t, ctx, producer, topic, value)
	require.NoError(t, producer.EndTransaction(ctx, end))
}

func produceRecord(t *testing.T, ctx context.Context, producer *kgo.Client, topic, value string) {
	t.Helper()
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Value: []byte(value),
	}).FirstErr())
}

type readCommittedInput struct {
	stream   *service.Stream
	messages chan string
	runErr   chan error
}

func startReadCommittedInput(
	t *testing.T,
	ctx context.Context,
	input,
	address,
	topic,
	consumerGroup string,
) *readCommittedInput {
	t.Helper()

	var config string
	switch input {
	case "kafka":
		config = fmt.Sprintf(`
kafka:
  addresses: [ %s ]
  topics: [ %s ]
  consumer_group: %s
  start_from_oldest: true
  checkpoint_limit: 1
  transaction_isolation_level: read_committed
`, address, topic, consumerGroup)
	case "kafka_franz":
		config = fmt.Sprintf(`
kafka_franz:
  seed_brokers: [ %s ]
  topics: [ %s ]
  consumer_group: %s
  start_from_oldest: true
  checkpoint_limit: 1
  transaction_isolation_level: read_committed
`, address, topic, consumerGroup)
	default:
		t.Fatalf("unsupported Kafka input %q", input)
	}

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetLoggerYAML("level: OFF"))
	require.NoError(t, builder.AddInputYAML(config))

	reader := &readCommittedInput{
		messages: make(chan string, 8),
		runErr:   make(chan error, 1),
	}
	require.NoError(t, builder.AddConsumerFunc(func(callbackCtx context.Context, message *service.Message) error {
		body, err := message.AsBytes()
		if err != nil {
			return err
		}
		select {
		case reader.messages <- string(body):
			return nil
		case <-callbackCtx.Done():
			return callbackCtx.Err()
		}
	}))

	var err error
	reader.stream, err = builder.Build()
	require.NoError(t, err)
	go func() {
		reader.runErr <- reader.stream.Run(ctx)
	}()
	return reader
}

func (r *readCommittedInput) expect(t *testing.T, expected string) {
	t.Helper()
	select {
	case actual := <-r.messages:
		require.Equal(t, expected, actual)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for Kafka record %q", expected)
	}
}

func (r *readCommittedInput) expectNone(t *testing.T, duration time.Duration) {
	t.Helper()
	select {
	case actual := <-r.messages:
		t.Fatalf("unexpected Kafka record %q", actual)
	case <-time.After(duration):
	}
}

func (r *readCommittedInput) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, r.stream.Stop(ctx))
	select {
	case err := <-r.runErr:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for Kafka input to stop")
	}
}
