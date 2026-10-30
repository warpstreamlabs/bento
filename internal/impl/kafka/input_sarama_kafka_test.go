package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type recordingOffsetMarker struct {
	marked []struct {
		topic     string
		partition int32
		offset    int64
	}
}

func (r *recordingOffsetMarker) MarkOffset(topic string, partition int32, offset int64, _ string) {
	r.marked = append(r.marked, struct {
		topic     string
		partition int32
		offset    int64
	}{topic: topic, partition: partition, offset: offset})
}

func TestKafkaBadParams(t *testing.T) {
	testCases := []struct {
		name   string
		topics []string
		errStr string
	}{
		{
			name:   "mixing consumer types",
			topics: []string{"foo", "foo:1"},
			errStr: "it is not currently possible to include balanced and explicit partition topics in the same kafka input",
		},
		{
			name:   "too many partitions",
			topics: []string{"foo:1:2:3"},
			errStr: "topic 'foo:1:2:3' is invalid, only one partition and an optional offset should be specified",
		},
		{
			name:   "bad range",
			topics: []string{"foo:1-2-3"},
			errStr: "partition '1-2-3' is invalid, only one range can be specified",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := iskConfigSpec().ParseYAML(fmt.Sprintf(`
addresses: [ example.com:1234 ]
topics: %v
`, gabs.Wrap(test.topics).String()), nil)
			require.NoError(t, err)

			_, err = newKafkaReaderFromParsed(pConf, service.MockResources())
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.errStr)
		})
	}
}

func TestKafkaAsyncCheckpointerAckErrors(t *testing.T) {
	marker := &recordingOffsetMarker{}
	reader := kafkaReader{
		checkpointLimit: 2,
		mgr:             service.MockResources(),
		session:         marker,
	}
	checkpointer := reader.asyncCheckpointer("foo", 1)
	messages := make(chan asyncMessage, 2)
	batch := service.MessageBatch{service.NewMessage([]byte("message"))}

	require.True(t, checkpointer(context.Background(), messages, batch, 10))
	first := <-messages
	require.NoError(t, first.ackFn(context.Background(), nil))
	assert.Equal(t, []struct {
		topic     string
		partition int32
		offset    int64
	}{{topic: "foo", partition: 1, offset: 10}}, marker.marked)

	reader = kafkaReader{
		checkpointLimit: 2,
		mgr:             service.MockResources(),
		session:         marker,
	}
	checkpointer = reader.asyncCheckpointer("foo", 1)
	marker.marked = nil
	require.True(t, checkpointer(context.Background(), messages, batch, 11))
	failed := <-messages
	require.NoError(t, failed.ackFn(context.Background(), errors.New("delivery failed")))
	assert.Empty(t, marker.marked)

	require.True(t, checkpointer(context.Background(), messages, batch, 12))
	later := <-messages
	require.NoError(t, later.ackFn(context.Background(), nil))
	assert.Empty(t, marker.marked)
}
