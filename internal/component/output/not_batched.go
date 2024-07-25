package output

import (
	"context"
	"errors"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/batch"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/message"
)

type notBatchedOutput struct {
	out Streamed

	inChan  <-chan message.Transaction
	outChan chan message.Transaction

	shutSig *shutdown.Signaller
}

// OnlySinglePayloads expands message batches into individual payloads,
// respecting the max in flight of the wrapped output. This is a more efficient
// way of feeding messages into an output that handles its own batching
// mechanism internally, or does not support batching at all.
func OnlySinglePayloads(out Streamed) Streamed {
	n := &notBatchedOutput{
		out:     out,
		outChan: make(chan message.Transaction),
		shutSig: shutdown.NewSignaller(),
	}
	return n
}

//------------------------------------------------------------------------------

func (n *notBatchedOutput) breakMessageOut(msg message.Batch) error {
	var wg sync.WaitGroup

	var batchErr *batch.Error
	var batchErrMut sync.Mutex
	addBatchErr := func(i int, err error) {
		if err != nil {
			batchErrMut.Lock()
			if batchErr == nil {
				batchErr = batch.NewError(msg, err)
			}
			batchErr.Failed(i, err)
			batchErrMut.Unlock()
		}
	}

	if err := msg.Iter(func(i int, p *message.Part) error {
		index := i

		tmpResChan := make(chan error, 1)
		tmpMsg := message.Batch{p}

		select {
		case n.outChan <- message.NewTransaction(tmpMsg, tmpResChan):
		case <-n.shutSig.HardStopChan():
			if index == 0 {
				return component.ErrTypeClosed
			}
			addBatchErr(index, component.ErrTypeClosed)
			return nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			select {
			case res := <-tmpResChan:
				err = res
			case <-n.shutSig.HardStopChan():
				err = component.ErrTypeClosed
			}
			addBatchErr(index, err)
		}()
		return nil
	}); err != nil {
		return err
	}

	wg.Wait()
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (n *notBatchedOutput) loop() {
	ctx, done := n.shutSig.HardStopCtx(context.Background())
	defer done()

	defer func() {
		close(n.outChan)
		n.out.TriggerCloseNow()
		_ = n.out.WaitForClose(ctx)
		n.shutSig.TriggerHasStopped()
	}()

	for {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-n.inChan:
			if !open {
				return
			}
		case <-n.shutSig.SoftStopChan():
			return
		}

		if tran.Payload.Len() == 1 {
			select {
			case n.outChan <- tran:
			case <-n.shutSig.HardStopChan():
				return
			}
		} else {
			var res error
			if err := n.breakMessageOut(tran.Payload); err != nil {
				if errors.Is(err, component.ErrTypeClosed) {
					return
				}
				res = err
			}
			_ = tran.Ack(ctx, res)
		}
	}
}

//------------------------------------------------------------------------------

func (n *notBatchedOutput) Consume(ts <-chan message.Transaction) error {
	if n.inChan != nil {
		return component.ErrAlreadyStarted
	}
	if err := n.out.Consume(n.outChan); err != nil {
		return err
	}
	n.inChan = ts
	go n.loop()
	return nil
}

func (n *notBatchedOutput) ConnectionStatus() component.ConnectionStatuses {
	return n.out.ConnectionStatus()
}

func (n *notBatchedOutput) TriggerCloseNow() {
	n.shutSig.TriggerHardStop()
}

// WaitForClose blocks until the File output has closed down.
func (n *notBatchedOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-n.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
