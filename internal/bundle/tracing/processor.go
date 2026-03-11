package tracing

import (
	"context"
	"sync/atomic"

	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
)

type tracedProcessor struct {
	e       *events
	errCtr  *uint64
	wrapped iprocessor.V1
}

func traceProcessor(e *events, errCtr *uint64, p iprocessor.V1) iprocessor.V1 {
	t := &tracedProcessor{
		e:       e,
		errCtr:  errCtr,
		wrapped: p,
	}
	return t
}

func (t *tracedProcessor) UnwrapProc() iprocessor.V1 {
	return t.wrapped
}

func (t *tracedProcessor) ProcessBatch(ctx context.Context, m message.Batch) ([]message.Batch, error) {
	if !t.e.IsEnabled() {
		return t.wrapped.ProcessBatch(ctx, m)
	}

	inputParts := make([]*message.Part, m.Len())
	prevErrs := make([]error, m.Len())
	_ = m.Iter(func(i int, part *message.Part) error {
		inputParts[i] = part
		t.e.Add(EventConsumeOf(part))
		prevErrs[i] = part.ErrorGet()
		return nil
	})

	outMsgs, res := t.wrapped.ProcessBatch(ctx, m)

	hasOutput := make([]bool, len(inputParts))

	for _, outMsg := range outMsgs {
		_ = outMsg.Iter(func(i int, part *message.Part) error {
			t.e.Add(EventProduceOf(part))
			fail := part.ErrorGet()
			if fail == nil {
				return nil
			}
			// TODO: Improve mechanism for tracking the introduction of errors?
			if len(prevErrs) <= i || prevErrs[i] == fail {
				return nil
			}
			_ = atomic.AddUint64(t.errCtr, 1)
			t.e.Add(EventErrorOfPart(part, fail))
			return nil
		})

		if outMsg.Len() > 0 {
			for i := 0; i < len(hasOutput) && i < outMsg.Len(); i++ {
				hasOutput[i] = true
			}
		}
	}

	if len(outMsgs) == 0 {
		for _, part := range inputParts {
			if part != nil {
				t.e.Add(EventDeleteOfPart(part))
			}
		}
	}

	return outMsgs, res
}

func (t *tracedProcessor) Close(ctx context.Context) error {
	return t.wrapped.Close(ctx)
}
