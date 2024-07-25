package pure

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/message"
)

type fanOutOutputBroker struct {
	transactions <-chan message.Transaction

	outputTSChans []chan message.Transaction
	outputs       []output.Streamed

	shutSig *shutdown.Signaller
}

func newFanOutOutputBroker(outputs []output.Streamed) (*fanOutOutputBroker, error) {
	o := &fanOutOutputBroker{
		transactions: nil,
		outputs:      outputs,
		shutSig:      shutdown.NewSignaller(),
	}

	o.outputTSChans = make([]chan message.Transaction, len(o.outputs))
	for i := range o.outputTSChans {
		o.outputTSChans[i] = make(chan message.Transaction)
		if err := o.outputs[i].Consume(o.outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (o *fanOutOutputBroker) Consume(transactions <-chan message.Transaction) error {
	if o.transactions != nil {
		return component.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

func (o *fanOutOutputBroker) ConnectionStatus() (s component.ConnectionStatuses) {
	for _, out := range o.outputs {
		s = append(s, out.ConnectionStatus()...)
	}
	return
}

func (o *fanOutOutputBroker) loop() {
	ackInterruptChan := make(chan struct{})
	var ackPending int64

	defer func() {
		// Wait for pending acks to be resolved, or forceful termination
	ackWaitLoop:
		for atomic.LoadInt64(&ackPending) > 0 {
			select {
			case <-ackInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-o.shutSig.HardStopChan():
				break ackWaitLoop
			}
		}
		for _, c := range o.outputTSChans {
			close(c)
		}
		_ = closeAllOutputs(context.Background(), o.outputs)
		o.shutSig.TriggerHasStopped()
	}()

	for {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.shutSig.HardStopChan():
			return
		}

		_ = atomic.AddInt64(&ackPending, 1)
		pendingResponses := int64(len(o.outputTSChans))
		for target := range o.outputTSChans {
			msgCopy, i := ts.Payload.ShallowCopy(), target
			select {
			case o.outputTSChans[i] <- message.NewTransactionFunc(msgCopy, func(ctx context.Context, err error) error {
				if atomic.AddInt64(&pendingResponses, -1) == 0 || err != nil {
					atomic.StoreInt64(&pendingResponses, 0)
					ackErr := ts.Ack(ctx, err)
					_ = atomic.AddInt64(&ackPending, -1)
					select {
					case ackInterruptChan <- struct{}{}:
					default:
					}
					return ackErr
				}
				return nil
			}):
			case <-o.shutSig.HardStopChan():
				return
			}
		}
	}
}

func (o *fanOutOutputBroker) TriggerCloseNow() {
	o.shutSig.TriggerHardStop()
}

func (o *fanOutOutputBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-o.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
