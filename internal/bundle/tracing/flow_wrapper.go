package tracing

import (
	"context"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/message"
	internaltracing "github.com/warpstreamlabs/bento/internal/tracing"
)

// flowIDInput wraps an input to ensure all messages have flow IDs initialized.
// This is independent of tracing and ensures flow IDs are always available.
type flowIDInput struct {
	wrapped input.Streamed
	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func wrapWithFlowID(i input.Streamed) input.Streamed {
	f := &flowIDInput{
		wrapped: i,
		tChan:   make(chan message.Transaction),
		shutSig: shutdown.NewSignaller(),
	}
	go f.loop()
	return f
}

func (f *flowIDInput) UnwrapInput() input.Streamed {
	return f.wrapped
}

func (f *flowIDInput) loop() {
	defer close(f.tChan)
	readChan := f.wrapped.TransactionChan()
	for {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-readChan:
			if !open {
				return
			}
		case <-f.shutSig.HardStopChan():
			return
		}

		_ = tran.Payload.Iter(func(i int, part *message.Part) error {
			tran.Payload[i] = internaltracing.EnsureFlowID(part)
			return nil
		})

		select {
		case f.tChan <- tran:
		case <-f.shutSig.HardStopChan():
			return
		}
	}
}

func (f *flowIDInput) TransactionChan() <-chan message.Transaction {
	return f.tChan
}

func (f *flowIDInput) ConnectionStatus() component.ConnectionStatuses {
	return f.wrapped.ConnectionStatus()
}

func (f *flowIDInput) TriggerStopConsuming() {
	f.wrapped.TriggerStopConsuming()
}

func (f *flowIDInput) TriggerCloseNow() {
	f.wrapped.TriggerCloseNow()
	f.shutSig.TriggerHardStop()
}

func (f *flowIDInput) WaitForClose(ctx context.Context) error {
	err := f.wrapped.WaitForClose(ctx)
	f.shutSig.TriggerHardStop()
	return err
}
