package pure

import (
	"context"
	"errors"
	"fmt"

	"github.com/warpstreamlabs/bento/v1/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/component/interop"
	"github.com/warpstreamlabs/bento/v1/internal/component/output"
	"github.com/warpstreamlabs/bento/v1/internal/log"
	"github.com/warpstreamlabs/bento/v1/internal/message"
	"github.com/warpstreamlabs/bento/v1/public/service"
)

func init() {
	err := service.RegisterBatchOutput(
		"reject", service.NewConfigSpec().
			Stable().
			Categories("Utility").
			Summary(`Rejects all messages, treating them as though the output destination failed to publish them.`).
			Description(`
The routing of messages after this output depends on the type of input it came from. For inputs that support propagating nacks upstream such as AMQP or NATS the message will be nacked. However, for inputs that are sequential such as files or Kafka the messages will simply be reprocessed from scratch.

If you're still scratching your head as to when this output could be useful check out [the examples below](#examples).`).
			Example(
				"Rejecting Failed Messages",
				`
This input is particularly useful for routing messages that have failed during processing, where instead of routing them to some sort of dead letter queue we wish to push the error upstream. We can do this with a switch broker:`,
				`
output:
  switch:
    retry_until_success: false
    cases:
      - check: '!errored()'
        output:
          amqp_1:
            urls: [ amqps://guest:guest@localhost:5672/ ]
            target_address: queue:/the_foos

      - output:
          reject: "processing failed due to: ${! error() }"
`,
			).
			Field(service.NewStringField("").Default("")),
		func(conf *service.ParsedConfig, res *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			var rejMsg string
			if rejMsg, err = conf.FieldString(); err != nil {
				return
			}

			mgr := interop.UnwrapManagement(res)

			var w *rejectWriter
			if w, err = newRejectWriter(mgr, rejMsg); err != nil {
				return
			}

			var s output.Streamed
			if s, err = output.NewAsyncWriter("reject", 1, w, mgr); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(s)
			return
		})
	if err != nil {
		panic(err)
	}
}

type rejectWriter struct {
	errExpr *field.Expression
	log     log.Modular
}

func newRejectWriter(mgr bundle.NewManagement, errorString string) (*rejectWriter, error) {
	if errorString == "" {
		return nil, errors.New("an error message must be provided in order to provide context for the rejection")
	}
	errExpr, err := mgr.BloblEnvironment().NewField(errorString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse error expression: %w", err)
	}
	return &rejectWriter{errExpr: errExpr, log: mgr.Logger()}, nil
}

func (w *rejectWriter) Connect(ctx context.Context) error {
	return nil
}

func (w *rejectWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	errStr, err := w.errExpr.String(0, msg)
	if err != nil {
		// Wow this would be awkward
		w.log.Error("Reject message interpolation error: %v", err)
		return fmt.Errorf("reject message interpolation error: %w", err)
	}
	return errors.New(errStr)
}

func (w *rejectWriter) Close(context.Context) error {
	return nil
}
