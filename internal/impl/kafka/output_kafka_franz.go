package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/warpstreamlabs/bento/public/service"
)

func franzKafkaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("1.0.0").
		Summary("A Kafka output using the [Franz Kafka client library](https://github.com/twmb/franz-go).").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.

This output often out-performs the traditional ` + "`kafka`" + ` output as well as providing more useful logs and error messages.
`).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewInterpolatedStringField("topic").
			Description("A topic to write messages to.")).
		Field(service.NewInterpolatedStringField("key").
			Description("An optional key to populate for each message.").Optional()).
		Field(service.NewStringAnnotatedEnumField("partitioner", map[string]string{
			"murmur2_hash":  "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":   "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup":  "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
			"uniform_bytes": "Partitions based on byte size, with options for adaptive partitioning and key-based hashing in the `uniform_bytes_options` component.",
			"manual":        "Manually select a partition for each message, requires the field `partition` to be specified.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional()).
		Field(service.NewObjectField("uniform_bytes_options",
			service.NewStringField("bytes").
				Description("The number of bytes the partitioner will return the same partition for.").
				Default("1MB"),
			service.NewBoolField("adaptive").
				Description("Sets a slight imbalance so that the partitioner can produce more to brokers that are less loaded.").
				Default(false),
			service.NewBoolField("keys").
				Description("If `true`, uses standard hashing based on record key for records with non-nil keys.").
				Default(false),
		).Description("Sets partitioner options when `partitioner` is of type `uniform_bytes`. These values will otherwise be ignored. Note, that future versions will likely see this approach reworked.").Advanced().Optional()).
		Field(service.NewInterpolatedStringField("partition").
			Description("An optional explicit partition to set for each message. This field is only relevant when the `partitioner` is set to `manual`. The provided interpolation string must be a valid integer.").
			Example(`${! metadata("partition") }`).
			Optional()).
		Field(service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("bento").
			Advanced()).
		Field(service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced()).
		Field(service.NewBoolField("idempotent_write").
			Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
			Default(true).
			Advanced()).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(10)).
		Field(service.NewDurationField("timeout").
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced()).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewStringField("max_message_bytes").
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib")).
		Field(service.NewIntField("max_buffered_records").
			Description("Sets the max amount of records the client will buffer, blocking produces until records are finished if this limit is reached. This overrides the `franz-kafka` default of 10,000.").
			Default(10_000).
			Advanced()).
		Field(service.NewDurationField("metadata_max_age").
			Description("This sets the maximum age for the client's cached metadata, to allow detection of new topics, partitions, etc.").
			Default("5m").
			Advanced()).
		Field(service.NewStringEnumField("compression", "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(saslField()).
		LintRule(`
root = if this.partitioner == "manual" {
  if this.partition.or("") == "" {
    "a partition must be specified when the partitioner is set to manual"
  }
} else if this.partition.or("") != "" {
  "a partition cannot be specified unless the partitioner is set to manual"
}`)
}

func init() {
	err := service.RegisterBatchOutput("kafka_franz", franzKafkaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newFranzKafkaWriterFromConfig(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type franzKafkaWriter struct {
	seedBrokers        []string
	topicStr           string
	topic              *service.InterpolatedString
	key                *service.InterpolatedString
	partition          *service.InterpolatedString
	clientID           string
	rackID             string
	idempotentWrite    bool
	tlsConf            *tls.Config
	saslConfs          []sasl.Mechanism
	metaFilter         *service.MetadataFilter
	partitioner        kgo.Partitioner
	timeout            time.Duration
	metadataMaxAge     time.Duration
	produceMaxBytes    int32
	maxBufferedRecords int

	compressionPrefs []kgo.CompressionCodec

	client *kgo.Client

	log *service.Logger
}

func newFranzKafkaWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzKafkaWriter, error) {
	f := franzKafkaWriter{
		log: log,
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		f.seedBrokers = append(f.seedBrokers, strings.Split(b, ",")...)
	}

	for _, b := range f.seedBrokers {
		if b == "" {
			return nil, errInvalidSeedBrokerValue
		}
	}

	if len(f.seedBrokers) == 0 {
		return nil, errInvalidSeedBrokerCount
	}

	if f.topic, err = conf.FieldInterpolatedString("topic"); err != nil {
		return nil, err
	}
	f.topicStr, _ = conf.FieldString("topic")

	if conf.Contains("key") {
		if f.key, err = conf.FieldInterpolatedString("key"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("partition") {
		if rawStr, _ := conf.FieldString("partition"); rawStr != "" {
			if f.partition, err = conf.FieldInterpolatedString("partition"); err != nil {
				return nil, err
			}
		}
	}

	if f.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	if f.metadataMaxAge, err = conf.FieldDuration("metadata_max_age"); err != nil {
		return nil, err
	}

	maxBytesStr, err := conf.FieldString("max_message_bytes")
	if err != nil {
		return nil, err
	}
	maxBytes, err := humanize.ParseBytes(maxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_message_bytes: %w", err)
	}
	if maxBytes > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("invalid max_message_bytes, must not exceed %v", math.MaxInt32)
	}
	f.produceMaxBytes = int32(maxBytes)

	var maxBufferedRecords int
	if maxBufferedRecords, err = conf.FieldInt("max_buffered_records"); err != nil {
		return nil, err
	}
	f.maxBufferedRecords = maxBufferedRecords

	if conf.Contains("compression") {
		cStr, err := conf.FieldString("compression")
		if err != nil {
			return nil, err
		}

		var c kgo.CompressionCodec
		switch cStr {
		case "lz4":
			c = kgo.Lz4Compression()
		case "gzip":
			c = kgo.GzipCompression()
		case "snappy":
			c = kgo.SnappyCompression()
		case "zstd":
			c = kgo.ZstdCompression()
		case "none":
			c = kgo.NoCompression()
		default:
			return nil, fmt.Errorf("compression codec %v not recognised", cStr)
		}
		f.compressionPrefs = append(f.compressionPrefs, c)
	}

	f.partitioner = kgo.StickyKeyPartitioner(nil)
	if conf.Contains("partitioner") {
		partStr, err := conf.FieldString("partitioner")
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			f.partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			f.partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			f.partitioner = kgo.LeastBackupPartitioner()
		case "manual":
			f.partitioner = kgo.ManualPartitioner()
		case "uniform_bytes":
			bytesArg, err := conf.FieldString("uniform_bytes_options", "bytes")
			if err != nil {
				return nil, err
			}

			ubBytes, err := humanize.ParseBytes(bytesArg)
			if err != nil {
				return nil, err
			}

			adaptive, err := conf.FieldBool("uniform_bytes_options", "adaptive")
			if err != nil {
				return nil, err
			}

			keys, err := conf.FieldBool("uniform_bytes_options", "keys")
			if err != nil {
				return nil, err
			}

			f.partitioner = kgo.UniformBytesPartitioner(int(ubBytes), adaptive, keys, nil)
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}

	if f.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	if f.rackID, err = conf.FieldString("rack_id"); err != nil {
		return nil, err
	}

	if f.idempotentWrite, err = conf.FieldBool("idempotent_write"); err != nil {
		return nil, err
	}

	if conf.Contains("metadata") {
		if f.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		f.tlsConf = tlsConf
	}
	if f.saslConfs, err = saslMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	return &f, nil
}

//------------------------------------------------------------------------------

func (f *franzKafkaWriter) Connect(ctx context.Context) error {
	if f.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(f.seedBrokers...),
		kgo.SASL(f.saslConfs...),
		kgo.AllowAutoTopicCreation(), // TODO: Configure this
		kgo.ProducerBatchMaxBytes(f.produceMaxBytes),
		kgo.ProduceRequestTimeout(f.timeout),
		kgo.ClientID(f.clientID),
		kgo.Rack(f.rackID),
		kgo.WithLogger(&kgoLogger{f.log}),
		kgo.MaxBufferedRecords(f.maxBufferedRecords),
		kgo.MetadataMaxAge(f.metadataMaxAge),
	}
	if f.tlsConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.tlsConf))
	}
	if f.partitioner != nil {
		clientOpts = append(clientOpts, kgo.RecordPartitioner(f.partitioner))
	}
	if !f.idempotentWrite {
		clientOpts = append(clientOpts, kgo.DisableIdempotentWrite())
	}
	if len(f.compressionPrefs) > 0 {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(f.compressionPrefs...))
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
	}

	f.client = cl
	return nil
}

func (f *franzKafkaWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	if f.client == nil {
		return service.ErrNotConnected
	}

	batchInterpolator := f.newInterpolationExecutor(b)

	records := make([]*kgo.Record, 0, len(b))
	for i, msg := range b {

		topic, key, partition, execErr := batchInterpolator.exec(i)
		if execErr != nil {
			return execErr
		}

		value, berr := msg.AsBytes()
		if berr != nil {
			return berr
		}

		record := &kgo.Record{
			Value:     value,
			Topic:     topic,
			Key:       key,
			Partition: partition,
		}

		if !f.metaFilter.IsEmpty() {
			_ = f.metaFilter.Walk(msg, func(key, value string) error {
				record.Headers = append(record.Headers, kgo.RecordHeader{
					Key:   key,
					Value: []byte(value),
				})
				return nil
			})
		}
		records = append(records, record)
	}

	// TODO: This is very cool and allows us to easily return granular errors,
	// so we should honor travis by doing it.
	err = f.client.ProduceSync(ctx, records...).FirstErr()
	return
}

func (f *franzKafkaWriter) disconnect() {
	if f.client == nil {
		return
	}
	f.client.Close()
	f.client = nil
}

func (f *franzKafkaWriter) Close(ctx context.Context) error {
	f.disconnect()
	return nil
}

//------------------------------------------------------------------------------

func (f *franzKafkaWriter) newInterpolationExecutor(batch service.MessageBatch) (ie interpolationExecutor) {
	ie.topicExecutor = batch.InterpolationExecutor(f.topic)
	if f.key != nil {
		ie.keyExecutor = batch.InterpolationExecutor(f.key)
	}
	if f.partition != nil {
		ie.partitionExecutor = batch.InterpolationExecutor(f.partition)
	}
	return ie
}

type interpolationExecutor struct {
	topicExecutor     *service.MessageBatchInterpolationExecutor
	keyExecutor       *service.MessageBatchInterpolationExecutor
	partitionExecutor *service.MessageBatchInterpolationExecutor
}

func (ie *interpolationExecutor) exec(i int) (topic string, key []byte, partition int32, err error) {
	topic, err = ie.topicExecutor.TryString(i)
	if err != nil {
		err = fmt.Errorf("topic interpolation error: %w", err)
		return
	}
	if ie.keyExecutor != nil {
		key, err = ie.keyExecutor.TryBytes(i)
		if err != nil {
			err = fmt.Errorf("key interpolation error: %w", err)
			return
		}
	}
	if ie.partitionExecutor != nil {
		var partStr string
		partStr, err = ie.partitionExecutor.TryString(i)
		if err != nil {
			err = fmt.Errorf("partition interpolation error: %w", err)
			return
		}
		var partitionInt int
		partitionInt, err = strconv.Atoi(partStr)
		if err != nil {
			err = fmt.Errorf("partition parse error: %w", err)
			return
		}
		partition = int32(partitionInt)
	}
	return
}
