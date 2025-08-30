package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestInputKafkaFranzConfig(t *testing.T) {
	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "single broker",
			conf: `
seed_brokers: [ broker_1 ]
topics: [ test ]
consumer_group: test
`,
		},
		{
			name: "multiple brokers",
			conf: `
seed_brokers: [ broker_1, broker_2 ]
topics: [ test ]
consumer_group: test
`,
		},
		{
			name: "multiple nested brokers",
			conf: `
seed_brokers: [ "broker_1,broker_2", "broker_3" ]
topics: [ test ]
consumer_group: test
`,
		},
		{
			name: "fail no brokers",
			conf: `
seed_brokers: [ ]
topics: [ test ]
consumer_group: test
`,
			errContains: "you must provide at least one address in 'seed_brokers'",
		},
		{
			name: "no seed broker should be empty string",
			conf: `
seed_brokers: [ "", "broker_1" ]
topic: foo
`,
			errContains: "seed broker address cannot be empty",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf, err := franzKafkaInputConfig().ParseYAML(test.conf, nil)
			require.NoError(t, err)

			_, err = newFranzKafkaReaderFromConfig(conf, service.MockResources())
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestInputKafkaFranzRetriableError(t *testing.T) {
	conf, err := franzKafkaInputConfig().ParseYAML(`
seed_brokers: [ localhost:9092 ]
topics: [ foo ]
consumer_group: test-group
reconnect_on_unknown_topic_or_partition: true
batching:
  count: 10
`, nil)
	require.NoError(t, err)

	reconnectReader, err := newFranzKafkaReaderFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	conf, err = franzKafkaInputConfig().ParseYAML(`
seed_brokers: [ localhost:9092 ]
topics: [ foo ]
consumer_group: test-group
checkpoint_limit: 100
batching:
  count: 10
`, nil)
	require.NoError(t, err)
	defaultReader, err := newFranzKafkaReaderFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	testCases := []struct {
		description                    string
		err                            error
		reconnectReaderExpected        bool
		disableReconnectReaderExpected bool
	}{
		{
			description:                    "context canceled error",
			err:                            context.Canceled,
			reconnectReaderExpected:        true,
			disableReconnectReaderExpected: true,
		},
		{
			description:                    "not kerr",
			err:                            errors.New("foo"),
			reconnectReaderExpected:        false,
			disableReconnectReaderExpected: false,
		},
		{
			description:                    "unknown topic or partition error",
			err:                            kerr.UnknownTopicOrPartition,
			reconnectReaderExpected:        false,
			disableReconnectReaderExpected: true,
		},
		{
			description:                    "unknown topic ID error",
			err:                            kerr.UnknownTopicID,
			reconnectReaderExpected:        false,
			disableReconnectReaderExpected: true,
		},
		{
			description: "retriable error",
			// This is a retriable error.
			err:                            kerr.UnstableOffsetCommit,
			reconnectReaderExpected:        true,
			disableReconnectReaderExpected: true,
		},
		{
			description: "not retriable error",
			// This is not a retriable error.
			err:                            kerr.ResourceNotFound,
			reconnectReaderExpected:        false,
			disableReconnectReaderExpected: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			require.Equal(t, testCase.reconnectReaderExpected, reconnectReader.isRetriableError(testCase.err))
			require.Equal(t, testCase.disableReconnectReaderExpected, defaultReader.isRetriableError(testCase.err))
		})
	}
}

// This test asserts that if reconnect_on_unknown_topic_or_partition is set to true
// and the reader sees an unknown topic error, it will force a reconnect.
func TestIntegrationFranzInputDetectUnknownTopicError(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx, cc := context.WithCancel(context.Background())
	defer cc()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "bitnami/kafka",
		Tag:          "latest",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Env: []string{
			"KAFKA_CFG_NODE_ID=0",
			"KAFKA_CFG_PROCESS_ROLES=controller,broker",
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093",
			"KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://:9093",
			"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + kafkaPortStr,
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(900)

	brokerAddress := "localhost:" + kafkaPortStr
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddress))
	require.NoError(t, err)
	defer cl.Close()

	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%d", "localhost", kafkaPort)),
		kgo.RequestRetries(1),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)

	admin := kadm.NewClient(kgoClient)
	require.Eventually(t, func() bool {
		_, err := admin.Metadata(ctx)
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)
	_, err = admin.CreateTopic(ctx, 1, 1, nil, "foo")
	require.NoError(t, err)

	conf, err := franzKafkaInputConfig().ParseYAML(fmt.Sprintf(`
seed_brokers: [ localhost:%v ]
topics: [ foo ]
consumer_group: test-group
checkpoint_limit: 100
commit_period: 1s
batching:
  count: 10
reconnect_on_unknown_topic_or_partition: true
`, kafkaPortStr), nil)
	require.NoError(t, err)

	reader, err := newFranzKafkaReaderFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	err = reader.Connect(ctx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)
	_, err = admin.DeleteTopic(ctx, "foo")
	require.NoError(t, err)

	timeoutCtx, cc := context.WithTimeout(ctx, time.Second*10)
	defer cc()

	timer := time.NewTicker(time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if reader.getBatchChan() == nil {
				// The connect goroutine will set batch chan to nil before it terminates.
				// If the reader was able to terminate the connect loop before the timeout,
				// this means that Connect successfully terminated after
				// seeing the unknown topic partition error.
				return
			}
		case <-timeoutCtx.Done():
			t.Fatalf("connect should've stopped before timeout due to unknown topic error")
		}
	}
}

// This test asserts that if reconnect_on_unknown_topic_or_partition is set to true,
// the input reader can handle recreated topics.
func TestIntegrationFranzInputReconnectUnknownTopicError(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx, cc := context.WithCancel(context.Background())
	defer cc()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "bitnami/kafka",
		Tag:          "latest",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Env: []string{
			"KAFKA_CFG_NODE_ID=0",
			"KAFKA_CFG_PROCESS_ROLES=controller,broker",
			"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093",
			"KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CFG_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://:9093",
			"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:" + kafkaPortStr,
		},
	}
	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(900)

	brokerAddress := "localhost:" + kafkaPortStr
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokerAddress))
	require.NoError(t, err)
	defer cl.Close()

	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%d", "localhost", kafkaPort)),
		kgo.RequestRetries(1),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)

	admin := kadm.NewClient(kgoClient)
	require.Eventually(t, func() bool {
		_, err := admin.Metadata(ctx)
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)

	_, err = admin.CreateTopic(ctx, 1, 1, nil, "foo")
	require.NoError(t, err)

	r := kgoClient.ProduceSync(ctx,
		&kgo.Record{
			Value:     []byte("before_recreate_topic"),
			Timestamp: time.Now(),
			Topic:     "foo",
			Partition: 0,
		},
	)
	require.NoError(t, r.FirstErr())

	inBuilder := service.NewStreamBuilder()
	err = inBuilder.AddInputYAML(fmt.Sprintf(`
kafka_franz:
  seed_brokers: [ localhost:%v ]
  topics: [ foo ]
  consumer_group: test-group
  checkpoint_limit: 100
  reconnect_on_unknown_topic_or_partition: true
  start_from_oldest: true
`, kafkaPortStr))
	require.NoError(t, err)

	var messageCountMut sync.Mutex
	var messageCount int
	require.NoError(t, inBuilder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		recordBytes, err := m.AsBytes()
		require.NoError(t, err)
		switch messageCount {
		case 0:
			require.Equal(t, "before_recreate_topic", string(recordBytes))
		case 1:
			require.Equal(t, "after_recreate_topic", string(recordBytes))
		}

		messageCountMut.Lock()
		messageCount++
		messageCountMut.Unlock()

		return nil
	}))

	inStrm, err := inBuilder.Build()
	require.NoError(t, err)
	go func() {
		assert.NoError(t, inStrm.Run(context.Background()))
	}()

	// Make sure it was able to read from the old topic
	require.Eventually(t, func() bool {
		messageCountMut.Lock()
		defer messageCountMut.Unlock()
		return messageCount == 1
	}, time.Second*20, time.Millisecond*500)

	time.Sleep(3 * time.Second)
	_, err = admin.DeleteTopic(ctx, "foo")
	require.NoError(t, err)
	// Recreate topic
	_, err = admin.CreateTopic(ctx, 1, 1, nil, "foo")
	require.NoError(t, err)

	r = kgoClient.ProduceSync(ctx,
		&kgo.Record{
			Value:     []byte("after_recreate_topic"),
			Timestamp: time.Now(),
			Topic:     "foo",
			Partition: 0,
		},
	)
	require.NoError(t, r.FirstErr())

	require.Eventually(t, func() bool {
		messageCountMut.Lock()
		defer messageCountMut.Unlock()
		return messageCount == 2
	}, time.Second*20, time.Millisecond*500)
}
