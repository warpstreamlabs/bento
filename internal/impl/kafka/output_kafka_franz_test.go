package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

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

func TestFranzWriterCRC32Partitioner(t *testing.T) {
	conf := `
seed_brokers: [ foo:1234 ]
topic: foo
partitioner: crc32_hash
`

	pConf, err := franzKafkaOutputConfig().ParseYAML(conf, nil)
	require.NoError(t, err)

	out, err := newFranzKafkaWriterFromConfig(pConf, service.MockResources().Logger())
	require.NoError(t, err)

	// Partitions observed for these keys on a 24 partition topic produced to by a
	// librdkafka consistent partitioner. Keys such as "32478" have the high bit of
	// their checksum set, which catches signed conversions of the hash.
	expected := map[string]int{
		"1":     23,
		"2":     13,
		"3":     19,
		"4":     16,
		"5":     22,
		"6":     4,
		"7":     18,
		"8":     11,
		"9":     21,
		"10":    9,
		"11":    15,
		"12":    21,
		"13":    11,
		"14":    0,
		"15":    22,
		"383":   6,
		"32478": 7,
		"45889": 19,
		"45890": 6,
	}

	tp := out.partitioner.ForTopic("foo")
	for key, partition := range expected {
		assert.Equal(t, partition, tp.Partition(&kgo.Record{Key: []byte(key)}, 24), "key %q", key)
	}
}

func TestCRC32Hasher(t *testing.T) {
	for _, n := range []int{1, 3, 24, 100} {
		for _, key := range []string{"", "a", "45889", "32478", "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"} {
			p := crc32Hasher([]byte(key), n)
			assert.GreaterOrEqual(t, p, 0)
			assert.Less(t, p, n)
		}
	}
	assert.Equal(t, 19, crc32Hasher([]byte("45889"), 24))
	assert.Equal(t, 932471059%27, crc32Hasher([]byte("45889"), 27))
}
