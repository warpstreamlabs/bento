package pure

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/message"
)

type greedyOutputBroker struct {
	outputs []output.Streamed
}

func newGreedyOutputBroker(outputs []output.Streamed) (*greedyOutputBroker, error) {
	return &greedyOutputBroker{
		outputs: outputs,
	}, nil
}

func (g *greedyOutputBroker) Consume(ts <-chan message.Transaction) error {
	for _, out := range g.outputs {
		if err := out.Consume(ts); err != nil {
			return err
		}
	}
	return nil
}

func (g *greedyOutputBroker) ConnectionStatus() (s component.ConnectionStatuses) {
	for _, out := range g.outputs {
		s = append(s, out.ConnectionStatus()...)
	}
	return
}

func (g *greedyOutputBroker) TriggerCloseNow() {
	for _, out := range g.outputs {
		out.TriggerCloseNow()
	}
}

func (g *greedyOutputBroker) WaitForClose(ctx context.Context) error {
	for _, out := range g.outputs {
		if err := out.WaitForClose(ctx); err != nil {
			return err
		}
	}
	return nil
}
