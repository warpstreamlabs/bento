package s2

import (
	"context"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/s2-streamstore/s2-sdk-go/s2"
	s2bentobox "github.com/s2-streamstore/s2-sdk-go/s2-bentobox"
	"github.com/warpstreamlabs/bento/public/service"
)

var (
	//go:embed embeds/output_description.md
	outputDescription string

	//go:embed embeds/output_starwars_eg.yaml
	outputStarwars string
)

var (
	errInvalidBatchingByteSize = errors.New("invalid batch policy byte size")
	errInvalidBatchingCount    = errors.New("invalid batch policy count")
)

func init() {
	if err := service.RegisterBatchOutput(
		s2bentobox.PluginName,
		newOutputConfigSpec(),
		func(conf *service.ParsedConfig, r *service.Resources) (
			service.BatchOutput, service.BatchPolicy, int, error,
		) {
			policy, err := parseBatchPolicy(conf)
			if err != nil {
				return nil, policy, 0, err
			}

			config, err := newOutputConfig(conf)
			if err != nil {
				return nil, policy, 0, err
			}

			return &Output{
				inner:  nil, // Will be instantiated during `Connect`
				config: config,
				logger: r.Logger(),
			}, policy, config.MaxInFlight, nil
		},
	); err != nil {
		panic(err)
	}
}

const (
	batchingField     = "batching"
	streamField       = "stream"
	matchSeqNumField  = "match_seq_num"
	fencingTokenField = "fencing_token"
)

var (
	batchingFieldSpec = service.NewBatchPolicyField(batchingField)

	streamFieldSpec = service.NewStringField(streamField).
			Description("Stream name")

	outputMaxInFlightSpec = service.NewOutputMaxInFlightField()

	fencingTokenFieldSpec = service.NewStringField(fencingTokenField).
				Optional().
				Description("Enforce a fencing token (base64 encoded)").
				Example("aGVsbG8gczI=")
)

func newOutputConfigSpec() *service.ConfigSpec {
	return newConfigSpec().
		Fields(
			batchingFieldSpec,
			streamFieldSpec,
			outputMaxInFlightSpec,
			fencingTokenFieldSpec,
		).
		Summary("Sends messages to an S2 stream.").
		Description(outputDescription).
		Example(
			"ASCII Starwars",
			"Consume a network stream into an S2 stream",
			outputStarwars,
		)
}

func parseBatchPolicy(conf *service.ParsedConfig) (service.BatchPolicy, error) {
	policy, err := conf.FieldBatchPolicy(batchingField)
	if err != nil {
		return policy, err
	}

	// Set required defaults

	if policy.ByteSize <= 0 {
		// TODO: We might need to decrease the size since bento message size != s2
		// record metered size.
		policy.ByteSize = int(s2.MaxBatchBytes)
	}

	if policy.Count <= 0 {
		policy.Count = s2.MaxBatchRecords
	}

	// This feels sensible to have. Not sure if we should let the user have infinite
	// retention until a batch of required size is formed.
	if policy.Period == "" {
		policy.Period = "5ms"
	}

	// Validate limits

	if policy.ByteSize > int(s2.MaxBatchBytes) {
		return policy, fmt.Errorf("%w: must not exceed %d", errInvalidBatchingByteSize, s2.MaxBatchBytes)
	}

	if policy.Count > s2.MaxBatchRecords {
		return policy, fmt.Errorf("%w: must not exceed %d", errInvalidBatchingCount, s2.MaxBatchRecords)
	}

	return policy, nil
}

func newOutputConfig(conf *service.ParsedConfig) (*s2bentobox.OutputConfig, error) {
	config, err := newConfig(conf)
	if err != nil {
		return nil, err
	}

	stream, err := conf.FieldString(streamField)
	if err != nil {
		return nil, err
	}

	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, err
	}

	var fencingToken []byte

	if conf.Contains(fencingTokenField) {
		field, err := conf.FieldString(fencingTokenField)
		if err != nil {
			return nil, err
		}

		fencingToken, err = base64.StdEncoding.DecodeString(field)
		if err != nil {
			return nil, err
		}
	}

	return &s2bentobox.OutputConfig{
		Config:       config,
		Stream:       stream,
		MaxInFlight:  maxInFlight,
		FencingToken: fencingToken,
	}, nil
}

type Output struct {
	inner  *s2bentobox.Output
	config *s2bentobox.OutputConfig
	logger *service.Logger
}

func (o *Output) Connect(ctx context.Context) error {
	o.logger.Debug("Connecting S2 output")

	inner, err := s2bentobox.ConnectOutput(ctx, o.config)
	if err != nil {
		return err
	}

	// Initialize inner connection.
	o.inner = inner

	return nil
}

func (o *Output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	o.logger.Debug("Writing batch to S2")

	recordBatch, err := s2.NewAppendRecordBatch()
	if err != nil {
		panic("empty record batch shouldn't error")
	}

	if err := batch.WalkWithBatchedErrors(func(_ int, m *service.Message) error {
		body, err := m.AsBytes()
		if err != nil {
			return err
		}

		var headers []s2.Header
		if err := m.MetaWalk(func(k, v string) error {
			headers = append(headers, s2.Header{
				Name:  []byte(k),
				Value: []byte(v),
			})

			return nil
		}); err != nil {
			return err
		}

		if !recordBatch.Append(s2.AppendRecord{
			Headers: headers,
			Body:    body,
		}) {
			return s2bentobox.ErrAppendRecordBatchFull
		}

		return nil
	}); err != nil {
		return err
	}

	if err := o.inner.WriteBatch(ctx, recordBatch); err != nil {
		if errors.Is(err, s2bentobox.ErrOutputClosed) {
			return service.ErrNotConnected
		}

		return err
	}

	return nil
}

func (o *Output) Close(ctx context.Context) error {
	o.logger.Debug("Closing S2 output")

	if err := o.inner.Close(ctx); err != nil {
		return err
	}

	o.inner = nil

	return nil
}
