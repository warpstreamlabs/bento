package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestKafkaFranzOutputBadParams(t *testing.T) {
	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "manual partitioner with a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
  partition: '${! metadata("foo").string() }'
`,
		},
		{
			name: "non manual partitioner without a partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
`,
		},
		{
			name: "manual partitioner with no partition",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partitioner: manual
`,
			errContains: "a partition must be specified when the partitioner is set to manual",
		},
		{
			name: "partition without manual partitioner",
			conf: `
kafka_franz:
  seed_brokers: [ foo:1234 ]
  topic: foo
  partition: '${! metadata("foo") }'
`,
			errContains: "a partition cannot be specified unless the partitioner is set to manual",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := service.NewStreamBuilder().AddOutputYAML(test.conf)
			if test.errContains == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestOutputKafkaFranzConfig(t *testing.T) {
	testCases := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "at least one seed broker should be defined",
			conf: `
seed_brokers: [ ]
topic: foo
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
			conf, err := franzKafkaOutputConfig().ParseYAML(test.conf, nil)
			require.NoError(t, err)

			_, err = newFranzKafkaWriterFromConfig(conf, service.MockResources().Logger())
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
