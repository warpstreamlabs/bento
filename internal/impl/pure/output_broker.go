package pure

import (
	"errors"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/batch/policy"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/output/batcher"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	boFieldCopies   = "copies"
	boFieldPattern  = "pattern"
	boFieldOutputs  = "outputs"
	boFieldBatching = "batching"
)

func brokerOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary(`Allows you to route messages to multiple child outputs using a range of brokering [patterns](#patterns).`).
		Description(`
Outputs can be defined inline with their full configuration or referenced by resource name. Both methods can be mixed:

`+"```yaml"+`
output:
  broker:
    pattern: fan_out
    outputs:
      # Inline output configurations
      - kafka:
          addresses: [ "localhost:9092" ]
          topic: topic_a
      - kafka:
          addresses: [ "localhost:9092" ]
          topic: topic_b
      # Or use resource references
      - resource: my_http_output
`+"```"+`

[Processors](/docs/components/processors/about) can be listed to apply across individual outputs or all outputs:

`+"```yaml"+`
output:
  broker:
    pattern: fan_out
    outputs:
      - resource: foo
      - resource: bar
        # Processors only applied to messages sent to bar.
        processors:
          - resource: bar_processor

  # Processors applied to messages sent to all brokered outputs.
  processors:
    - resource: general_processor
`+"```").
		Footnotes(`
## Patterns

The broker pattern determines the way in which messages are allocated and can be chosen from the following:

### `+"`fan_out`"+`

With the fan out pattern all outputs will be sent every message that passes through Bento in parallel.

If an output applies back pressure it will block all subsequent messages, and if an output fails to send a message it will be retried continuously until completion or service shut down. This mechanism is in place in order to prevent one bad output from causing a larger retry loop that results in a good output from receiving unbounded message duplicates.

Sometimes it is useful to disable the back pressure or retries of certain fan out outputs and instead drop messages that have failed or were blocked. In this case you can wrap outputs with a `+"[`drop_on` output](/docs/components/outputs/drop_on)"+`.

### `+"`fan_out_fail_fast`"+`

The same as the `+"`fan_out`"+` pattern, except that output failures will not be automatically retried. This pattern should be used with caution as busy retry loops could result in unlimited duplicates being introduced into the non-failure outputs.

### `+"`fan_out_sequential`"+`

Similar to the fan out pattern except outputs are written to sequentially, meaning an output is only written to once the preceding output has confirmed receipt of the same message.

If an output applies back pressure it will block all subsequent messages, and if an output fails to send a message it will be retried continuously until completion or service shut down. This mechanism is in place in order to prevent one bad output from causing a larger retry loop that results in a good output from receiving unbounded message duplicates.

### `+"`fan_out_sequential_fail_fast`"+`

The same as the `+"`fan_out_sequential`"+` pattern, except that output failures will not be automatically retried. This pattern should be used with caution as busy retry loops could result in unlimited duplicates being introduced into the non-failure outputs.

### `+"`round_robin`"+`

With the round robin pattern each message will be assigned a single output following their order. If an output applies back pressure it will block all subsequent messages. If an output fails to send a message then the message will be re-attempted with the next input, and so on.

### `+"`greedy`"+`

The greedy pattern results in higher output throughput at the cost of potentially disproportionate message allocations to those outputs. Each message is sent to a single output, which is determined by allowing outputs to claim messages as soon as they are able to process them. This results in certain faster outputs potentially processing more messages at the cost of slower outputs.`).
		Fields(
			service.NewIntField(boFieldCopies).
				Description("The number of copies of each configured output to spawn.").
				Advanced().
				Default(1),
			service.NewStringEnumField(boFieldPattern,
				"fan_out", "fan_out_fail_fast", "fan_out_sequential", "fan_out_sequential_fail_fast", "round_robin", "greedy").
				Description("The brokering pattern to use.").
				Default("fan_out"),
			service.NewOutputListField(boFieldOutputs).
				Description("A list of child outputs to broker. Each item can be either a complete inline output configuration or a reference to an output resource."),
			service.NewBatchPolicyField(boFieldBatching),
		).Example(
		"Send to Multiple Destinations",
		"Send the same data to multiple outputs simultaneously.",
		`
output:
  broker:
    pattern: fan_out
    outputs:
      - gcp_bigquery:
          project: my-project
          dataset: raw_data
          table: events
      - gcp_bigquery:
          project: my-project
          dataset: analytics
          table: events_aggregated
      - file:
          path: /backup/events.jsonl
          codec: lines
`,
	).Example(
		"Load Balance Across Endpoints", "Distribute messages across multiple targets.",
		`
output:
  broker:
    pattern: round_robin
    outputs:
      - http_client:
          url: http://api1.example.com/data
      - http_client:
          url: http://api2.example.com/data
      - http_client:
          url: http://api3.example.com/data
`,
	)
}

// ErrBrokerNoOutputs is returned when creating a Broker type with zero
// outputs.
var ErrBrokerNoOutputs = errors.New("attempting to create broker output type with no outputs")

func init() {
	err := service.RegisterBatchOutput(
		"broker", brokerOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			var bi output.Streamed
			if bi, err = brokerOutputFromParsed(conf, mgr); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(bi)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func brokerOutputFromParsed(conf *service.ParsedConfig, res *service.Resources) (output.Streamed, error) {
	mgr := interop.UnwrapManagement(res)

	copies, err := conf.FieldInt(boFieldCopies)
	if err != nil {
		return nil, err
	}

	pattern, err := conf.FieldString(boFieldPattern)
	if err != nil {
		return nil, err
	}

	var batchPol *policy.Batcher
	{
		batchConf, err := conf.FieldBatchPolicy(boFieldBatching)
		if err != nil {
			return nil, err
		}
		if !batchConf.IsNoop() {
			iBatcher, err := batchConf.NewBatcher(res)
			if err != nil {
				return nil, err
			}
			batchPol = interop.UnwrapBatcher(iBatcher)
		}
	}

	_, isRetryWrapped := map[string]struct{}{
		"fan_out":            {},
		"fan_out_sequential": {},
	}[pattern]

	var outputs []output.Streamed
	{
		pubOutputs, err := conf.FieldOutputList(boFieldOutputs)
		if err != nil {
			return nil, err
		}
		for _, v := range pubOutputs {
			tmpOut := interop.UnwrapOwnedOutput(v)
			if isRetryWrapped {
				if tmpOut, err = RetryOutputIndefinitely(mgr, tmpOut); err != nil {
					return nil, err
				}
			}
			outputs = append(outputs, tmpOut)
		}
	}

	lOutputs := len(outputs) * copies
	if lOutputs <= 0 {
		return nil, ErrBrokerNoOutputs
	}
	if lOutputs == 1 {
		b := outputs[0]
		if batchPol != nil {
			b = batcher.New(batchPol, b, mgr)
		}
		return b, nil
	}

	for j := 1; j < copies; j++ {
		extraChildren, err := conf.FieldOutputList(boFieldOutputs)
		if err != nil {
			return nil, err
		}
		for _, v := range extraChildren {
			tmpOut := interop.UnwrapOwnedOutput(v)
			if isRetryWrapped {
				if tmpOut, err = RetryOutputIndefinitely(mgr, tmpOut); err != nil {
					return nil, err
				}
			}
			outputs = append(outputs, tmpOut)
		}
	}

	var b output.Streamed
	switch pattern {
	case "fan_out", "fan_out_fail_fast":
		b, err = newFanOutOutputBroker(outputs)
	case "fan_out_sequential", "fan_out_sequential_fail_fast":
		b, err = newFanOutSequentialOutputBroker(outputs)
	case "round_robin":
		b, err = newRoundRobinOutputBroker(outputs)
	case "greedy":
		b, err = newGreedyOutputBroker(outputs)
	default:
		return nil, fmt.Errorf("broker pattern was not recognised: %v", pattern)
	}

	if batchPol != nil {
		b = batcher.New(batchPol, b, mgr)
	}
	return b, err
}
