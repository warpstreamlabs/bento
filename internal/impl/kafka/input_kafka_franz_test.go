package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

func TestInputKafkaFranzRetriableError(t *testing.T) {
	conf, err := franzKafkaInputConfig().ParseYAML(`
seed_brokers: [ localhost:9092 ]
topics: [ foo ]
consumer_group: test-group
reconnect_on_unknown_topic: true
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
			require.Equal(t, reconnectReader.isRetriableError(testCase.err), testCase.reconnectReaderExpected)
			require.Equal(t, defaultReader.isRetriableError(testCase.err), testCase.disableReconnectReaderExpected)
		})
	}
}

// This test
// kobe
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
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Cmd: []string{
			"redpanda", "start", "--smp 1", "--overprovisioned",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
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
	}, 5*time.Second, 100*time.Millisecond)
	_, err = admin.CreateTopic(ctx, 1, 1, nil, "foo")
	require.NoError(t, err)
	fmt.Println("everything worked!")
	// fmt.Println("topic ID", createdTopic.ID)

	conf, err := franzKafkaInputConfig().ParseYAML(fmt.Sprintf(`
seed_brokers: [ localhost:%v ]
topics: [ foo ]
consumer_group: test-group
checkpoint_limit: 100
commit_period: 1s
batching:
  count: 10
`, kafkaPortStr), nil)
	require.NoError(t, err)

	reader, err := newFranzKafkaReaderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	fmt.Println(reader)

	err = reader.Connect(ctx)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)
	_, err = admin.DeleteTopic(ctx, "foo")
	require.NoError(t, err)

	timeoutCtx, cc := context.WithTimeout(ctx, time.Second*10)
	defer cc()

	timer := time.NewTimer(time.Second)
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
