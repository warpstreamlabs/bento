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

// Setting the batch byte size max a bit conservatively since Bento does not
// take metadata size into account. Moreover, the S2 metered size of a record
// will be > bento message size.
const maxBatchBytesConservative = 256 * 1024

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

func newOutputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.5.0").
		Categories("Services").
		Fields(
			service.NewStringField(basinField).Description("Basin name"),
			service.NewStringField(accessTokenField).
				Description("Access token for S2 account").
				Secret(),
			service.NewStringField(streamField).Description("Stream name"),
			service.NewStringField(fencingTokenField).
				Optional().
				Description("Enforce a fencing token (base64 encoded)").
				Example("aGVsbG8gczI="),
			service.NewBatchPolicyField(batchingField).
				Advanced().
				LintRule(
					fmt.Sprintf(`
root = if this.count > %d {
	"the number of messages in a batch cannot exceed 1000"
}
root = if this.byte_size > %d {
	"the amount of bytes in a batch cannot exceed 256KiB"
}
				`,
						s2.MaxBatchRecords,
						maxBatchBytesConservative,
					),
				),
			service.NewOutputMaxInFlightField().Advanced(),
		).
		Summary("Sends messages to an S2 stream.").
		Description(`
Generate an authentication token by logging onto the web console at
[s2.dev](https://s2.dev/dashboard).

### Metadata

The metadata attributes are set as S2 record headers. Currently, only string
attribute values are supported.

### Batching

The plugin expects batched inputs. Messages are batched automatically by Bento.

By default, Bento disables batching based on `+"`count`"+`, `+"`byte_size`"+`, and `+"`period`"+`
parameters, but the plugin enables batching setting both `+"`count`"+` and `+"`byte_size`"+` to
the maximum values supported by S2. It also sets a flush period of `+"`5ms`"+` as a reasonable default.

**Note:** An S2 record batch can be a maximum of 1MiB but the plugin limits the
size of a message to 256KiB since the Bento size limit doesn't take metadata into
account. Moreover, the metered size of the same Bento message will be greater
than the byte size of a Bento message.
`,
		).
		Example(
			"ASCII Starwars",
			"Consume a network stream into an S2 stream",
			`
input:
  label: towel_blinkenlights_nl
  socket:
    network: tcp
    address: towel.blinkenlights.nl:23
    scanner:
      lines: {}

output:
  label: s2_starwars
  s2:
    basin: my-favorite-basin
    stream: starwars
    access_token: "${S2_ACCESS_TOKEN}"
`,
		)
}

func parseBatchPolicy(conf *service.ParsedConfig) (service.BatchPolicy, error) {
	policy, err := conf.FieldBatchPolicy(batchingField)
	if err != nil {
		return policy, err
	}

	// Set required defaults

	if policy.ByteSize <= 0 {
		policy.ByteSize = maxBatchBytesConservative
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

	if policy.ByteSize > maxBatchBytesConservative {
		return policy, fmt.Errorf("%w: must not exceed %d", errInvalidBatchingByteSize, s2.MaxBatchMeteredBytes)
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

	var fencingToken *string

	if conf.Contains(fencingTokenField) {
		field, err := conf.FieldString(fencingTokenField)
		if err != nil {
			return nil, err
		}

		// Decode base64 to get the raw token string
		decoded, err := base64.StdEncoding.DecodeString(field)
		if err != nil {
			return nil, err
		}
		decodedStr := string(decoded)
		fencingToken = &decodedStr
	}

	return &s2bentobox.OutputConfig{
		Config:       config,
		Stream:       stream,
		FencingToken: fencingToken,
		MaxInFlight:  maxInFlight,
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

	records := make([]s2.AppendRecord, 0, len(batch))

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

		records = append(records, s2.AppendRecord{
			Headers: headers,
			Body:    body,
		})

		return nil
	}); err != nil {
		return err
	}

	if err := o.inner.WriteBatch(ctx, records); err != nil {
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
