package kafka

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

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

func TestKafkaTransactionIsolationLevel(t *testing.T) {
	testCases := []struct {
		name     string
		config   string
		expected sarama.IsolationLevel
	}{
		{
			name:     "defaults to read uncommitted",
			expected: sarama.ReadUncommitted,
		},
		{
			name:     "explicit read uncommitted",
			config:   "transaction_isolation_level: read_uncommitted",
			expected: sarama.ReadUncommitted,
		},
		{
			name:     "read committed",
			config:   "transaction_isolation_level: read_committed",
			expected: sarama.ReadCommitted,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			conf, err := iskConfigSpec().ParseYAML(fmt.Sprintf(`
addresses: [ example.com:1234 ]
topics: [ test ]
consumer_group: test
%s
`, test.config), nil)
			require.NoError(t, err)

			reader, err := newKafkaReaderFromParsed(conf, service.MockResources())
			require.NoError(t, err)
			assert.Equal(t, test.expected, reader.saramConf.Consumer.IsolationLevel)
		})
	}
}

func TestKafkaRejectsInvalidTransactionIsolationLevel(t *testing.T) {
	conf, err := iskConfigSpec().ParseYAML(`
addresses: [ example.com:1234 ]
topics: [ test ]
consumer_group: test
transaction_isolation_level: eventually_consistent
`, nil)
	require.NoError(t, err)

	_, err = newKafkaReaderFromParsed(conf, service.MockResources())
	require.Error(t, err)
	assert.EqualError(t, err, `invalid transaction isolation level: "eventually_consistent"`)
}
