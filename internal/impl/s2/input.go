package s2

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"

	s2bentobox "github.com/s2-streamstore/s2-sdk-go/s2-bentobox"
	"github.com/warpstreamlabs/bento/public/service"
)

var errCacheNotFound = errors.New("cache not found")

func init() {
	if err := service.RegisterBatchInput(
		s2bentobox.PluginName,
		newInputConfigSpec(),
		func(conf *service.ParsedConfig, r *service.Resources) (service.BatchInput, error) {
			config, err := newInputConfig(conf, r)
			if err != nil {
				return nil, err
			}

			return &Input{
				inner:  nil, // Will be instantiated during `Connect`
				config: config,
				logger: r.Logger(),
			}, nil
		},
	); err != nil {
		panic(err)
	}
}

const (
	streamsField               = "streams"
	cacheField                 = "cache"
	updateStreamsIntervalField = "update_streams_interval"
	backoffDurationField       = "backoff_duration"
	startSeqNumField           = "start_seq_num"

	startSeqNumEarliest = "earliest"
	startSeqNumLatest   = "latest"
)

func newInputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.5.0").
		Categories("Services").
		Fields(
			service.NewStringField(basinField).Description("Basin name"),
			service.NewStringField(accessTokenField).
				Description("Access token for S2 account").
				Secret(),
			service.NewAnyField(streamsField).
				Description("Streams prefix or list of streams to subscribe to"),
			service.NewStringField(cacheField).
				Description("Cache resource label for storing sequence number"),
			service.NewInputMaxInFlightField().Advanced(),
			service.NewDurationField(updateStreamsIntervalField).
				Advanced().
				Default("1m").
				Description("Interval after which the streams list should update dynamically"),
			service.NewDurationField(backoffDurationField).
				Advanced().
				Version("1.6.0").
				Default("100ms").
				Description("Interval to backoff for before reconnecting to a stream"),
			service.NewStringEnumField(startSeqNumField, startSeqNumEarliest, startSeqNumLatest).
				Description("Start consuming the stream from either the earliest or the latest sequence number").
				Version("1.6.0").
				Default(startSeqNumEarliest).
				Advanced(),
		).
		Summary("Consumes records from S2 streams").
		Description(`
Generate an authentication token by logging onto the web console at
[s2.dev](https://s2.dev/dashboard).

### Cache

The plugin requires setting up a caching mechanism to resume the input after
the last acknowledged record.

To know more about setting up a cache resource, see
[Cache docs for Bento](https://warpstreamlabs.github.io/bento/docs/components/caches/about).

### Metadata

This input adds the following metadata fields to each message in addition to the
record headers:

- `+"`s2_basin`"+`: The S2 basin where the origin stream lives.
- `+"`s2_stream`"+`: The origin S2 stream.
- `+"`s2_seq_num`"+`: Sequence number of the record in the origin stream formatted as a string.

All the header values are loosely converted to strings as metadata attributes.

**Note:** An [S2 command record](https://s2.dev/docs/stream#command-records) has no header
name. This is set as the `+"`s2_command`"+` meta key.
`,
		).
		Example(
			"Input with Prefix",
			"Fetch records from all the streams with the prefix `my-favorite-prefix/` in the basin.",
			`
cache_resources:
  - label: s2_seq_num
    file:
      directory: s2_seq_num_cache

input:
  label: s2_input
  s2:
    basin: my-favorite-basin
    streams: my-favorite-prefix/
    access_token: "${S2_ACCESS_TOKEN}"
    cache: s2_seq_num

output:
  label: stdout
  stdout:
    codec: lines
`,
		)
}

func newInputConfig(conf *service.ParsedConfig, r *service.Resources) (*s2bentobox.InputConfig, error) {
	config, err := newConfig(conf)
	if err != nil {
		return nil, err
	}

	var inputStreams s2bentobox.InputStreams

	if streams, err := conf.FieldStringList(streamsField); err != nil {
		// Try just a prefix.
		prefix, err := conf.FieldString(streamsField)
		if err != nil {
			return nil, err
		}

		inputStreams = s2bentobox.PrefixedInputStreams{
			Prefix: prefix,
		}
	} else {
		inputStreams = s2bentobox.StaticInputStreams{
			Streams: streams,
		}
	}

	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, err
	}

	cacheLabel, err := conf.FieldString(cacheField)
	if err != nil {
		return nil, err
	}

	if !r.HasCache(cacheLabel) {
		return nil, fmt.Errorf("%w: %q", errCacheNotFound, cacheLabel)
	}

	cache := &bentoSeqNumCache{
		Resources: r,
		Label:     cacheLabel,
	}

	updateStreamsInterval, err := conf.FieldDuration(updateStreamsIntervalField)
	if err != nil {
		return nil, err
	}

	backoffDuration, err := conf.FieldDuration(backoffDurationField)
	if err != nil {
		return nil, err
	}

	s, err := conf.FieldString(startSeqNumField)
	if err != nil {
		return nil, err
	}

	var startSeqNum s2bentobox.InputStartSeqNum
	switch s {
	case startSeqNumEarliest:
		startSeqNum = s2bentobox.InputStartSeqNumEarliest
	case startSeqNumLatest:
		startSeqNum = s2bentobox.InputStartSeqNumLatest
	default:
		return nil, fmt.Errorf(
			"invalid value for start_seq_num: must be one of '%s' or `%s`",
			startSeqNumEarliest, startSeqNumLatest,
		)
	}

	return &s2bentobox.InputConfig{
		Config:                config,
		Streams:               inputStreams,
		MaxInFlight:           maxInFlight,
		Logger:                &bentoLogger{r.Logger()},
		Cache:                 cache,
		UpdateStreamsInterval: updateStreamsInterval,
		BackoffDuration:       backoffDuration,
		StartSeqNum:           startSeqNum,
	}, nil
}

type Input struct {
	inner  *s2bentobox.MultiStreamInput
	config *s2bentobox.InputConfig
	logger *service.Logger
}

func (i *Input) Connect(ctx context.Context) error {
	i.logger.Debug("Connecting S2 input")

	inner, err := s2bentobox.ConnectMultiStreamInput(ctx, i.config)
	if err != nil {
		return err
	}

	// Initialize inner connection.
	i.inner = inner

	return nil
}

func (i *Input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	i.logger.Debug("Reading batch from S2")

	records, aFn, stream, err := i.inner.ReadBatch(ctx)
	if err != nil {
		if errors.Is(err, s2bentobox.ErrInputClosed) {
			return nil, nil, service.ErrNotConnected
		}

		return nil, nil, err
	}

	messages := make([]*service.Message, 0, len(records))

	for _, record := range records {
		msg := service.NewMessage(record.Body)

		if len(record.Headers) == 1 && len(record.Headers[0].Name) == 0 {
			// Command record
			msg.MetaSet("s2_command", string(record.Headers[0].Value))
		} else {
			for _, header := range record.Headers {
				// TODO: Use `MetaSetMut`.
				msg.MetaSet(string(header.Name), string(header.Value))
			}
		}

		msg.MetaSet("s2_seq_num", strconv.FormatUint(record.SeqNum, 10))
		msg.MetaSet("s2_stream", stream)
		msg.MetaSet("s2_basin", i.config.Basin)

		messages = append(messages, msg)
	}

	return messages, aFn, nil
}

func (i *Input) Close(ctx context.Context) error {
	i.logger.Debug("Closing S2 input")

	if err := i.inner.Close(ctx); err != nil {
		return err
	}

	i.inner = nil

	return nil
}
