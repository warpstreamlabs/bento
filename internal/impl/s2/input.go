package s2

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	s2bentobox "github.com/s2-streamstore/s2-sdk-go/s2-bentobox"
	"github.com/warpstreamlabs/bento/public/service"
)

var (
	//go:embed embeds/input_description.md
	inputDescription string

	//go:embed embeds/input_with_prefix_eg.yaml
	inputWithPrefixExample string
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
)

var (
	streamsFieldSpec = service.NewAnyField(streamsField).
				Description("Streams prefix or list of streams to subscribe to")

	inputMaxInFlightSpec = service.NewInputMaxInFlightField()

	cacheFieldSpec = service.NewStringField(cacheField).
			Description("Cache resource label for storing sequence number")

	updateStreamsIntervalSpec = service.NewDurationField(updateStreamsIntervalField).
					Advanced().
					Default("60s").
					Description("Interval after which the streams list should update dynamically")
)

func newInputConfigSpec() *service.ConfigSpec {
	return newConfigSpec().
		Fields(
			streamsFieldSpec,
			inputMaxInFlightSpec,
			cacheFieldSpec,
			updateStreamsIntervalSpec,
		).
		Summary("Consumes records from S2 streams").
		Description(inputDescription).
		Example(
			"Input with Prefix",
			"Fetch records from all the streams with the prefix `my-favorite-prefix-` in the basin.",
			inputWithPrefixExample,
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

	return &s2bentobox.InputConfig{
		Config:                config,
		Streams:               inputStreams,
		MaxInFlight:           maxInFlight,
		Logger:                &bentoLogger{L: r.Logger()},
		Cache:                 cache,
		UpdateStreamsInterval: updateStreamsInterval,
	}, nil
}

type bentoSeqNumCache struct {
	Resources *service.Resources
	Label     string
}

func streamCacheKey(stream string) string {
	return base64.URLEncoding.EncodeToString([]byte(stream))
}

func (b *bentoSeqNumCache) Get(ctx context.Context, stream string) (uint64, error) {
	var (
		seqNum uint64
		err    error
	)

	if aErr := b.Resources.AccessCache(ctx, b.Label, func(c service.Cache) {
		var seqNumBytes []byte
		seqNumBytes, err = c.Get(ctx, streamCacheKey(stream))
		if err != nil {
			return
		}

		seqNum = binary.BigEndian.Uint64(seqNumBytes)
	}); aErr != nil {
		return 0, aErr
	}

	return seqNum, err
}

func (b *bentoSeqNumCache) Set(ctx context.Context, stream string, seqNum uint64) error {
	var err error

	if aErr := b.Resources.AccessCache(ctx, b.Label, func(c service.Cache) {
		seqNumBytes := binary.BigEndian.AppendUint64(make([]byte, 0, 8), seqNum)

		err = c.Set(ctx, streamCacheKey(stream), seqNumBytes, nil)
	}); aErr != nil {
		return aErr
	}

	return err
}

type Input struct {
	inner  *s2bentobox.Input
	config *s2bentobox.InputConfig
	logger *service.Logger
}

func (i *Input) Connect(ctx context.Context) error {
	i.logger.Debug("Connecting S2 input")

	inner, err := s2bentobox.ConnectInput(ctx, i.config)
	if err != nil {
		return err
	}

	// Initialize inner connection.
	i.inner = inner

	return nil
}

func (i *Input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	i.logger.Debug("Reading batch from S2")

	batch, stream, err := i.inner.ReadBatch(ctx)
	if err != nil {
		if errors.Is(err, s2bentobox.ErrInputClosed) {
			return nil, nil, service.ErrNotConnected
		}

		return nil, nil, err
	}

	messages := make([]*service.Message, 0, len(batch.Records))

	for _, record := range batch.Records {
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

		messages = append(messages, msg)
	}

	return messages, i.inner.AckFunc(stream, batch), nil
}

func (i *Input) Close(ctx context.Context) error {
	i.logger.Debug("Closing S2 input")

	if err := i.inner.Close(ctx); err != nil {
		return err
	}

	i.inner = nil

	return nil
}
