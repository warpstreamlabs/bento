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

func TestFranzWriterInterpolatedKey(t *testing.T) {
	conf := `
seed_brokers: [ foo:1234 ]
topic: foo
key: '${! json("key") }'
`

	pConf, err := franzKafkaOutputConfig().ParseYAML(conf, nil)
	require.NoError(t, err)

	out, err := newFranzKafkaWriterFromConfig(pConf, service.MockResources().Logger())
	require.NoError(t, err)

	msgBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff", "key": 1234}`)),
	}

	ie := out.newInterpolationExecutor(msgBatch)

	_, key, _, err := ie.exec(0)
	require.NoError(t, err)
	assert.Nil(t, key)

	_, key, _, err = ie.exec(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("1234"), key)
}
