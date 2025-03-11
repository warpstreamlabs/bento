package strict

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/cenkalti/backoff/v4"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

type feedbackPipeline struct {
	pipe processor.Pipeline

	transactionsOut chan message.Transaction
	transactionsIn  <-chan message.Transaction

	retryTransactionCh chan message.Transaction

	shutSig *shutdown.Signaller

	logger log.Modular

	isRetrying atomic.Bool
}

func newBackoff(ctx context.Context) backoff.BackOff {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond * 500
	boff.MaxInterval = time.Second * 300
	boff.MaxElapsedTime = 0

	defer boff.Reset()

	if ctx != nil {
		return backoff.WithContext(boff, ctx)
	}
	return boff

}

func newFeedbackProcessor(pipe processor.Pipeline, mgr bundle.NewManagement) processor.Pipeline {
	return &feedbackPipeline{
		pipe:               pipe,
		transactionsOut:    make(chan message.Transaction),
		retryTransactionCh: make(chan message.Transaction),
		shutSig:            shutdown.NewSignaller(),
		logger:             mgr.Logger(),
	}
}

// newMergeChannels merged a bento stream's input-channel with a retry-channel
// to allow for requeuing failed transactions.
func (p *feedbackPipeline) newMergeChannels(ctx context.Context) <-chan message.Transaction {
	out := make(chan message.Transaction)
	var wg sync.WaitGroup

	shutSig := shutdown.NewSignaller()

	// Stream in data from the original input layer
	wg.Add(1)
	go func() {
		defer func() {
			shutSig.TriggerSoftStop()
			wg.Done()
		}()

		for tran := range p.transactionsIn {
			if p.isRetrying.Load() {
				_ = tran.Ack(ctx, errors.New("retry"))
				continue
			}
			out <- tran
		}
	}()

	// Stream in data from the retry channel
	wg.Add(1)
	go func() {
		boff := newBackoff(ctx)

		defer func() {
			boff.Reset()
			wg.Done()
		}()

		for {
			select {
			case tran, open := <-p.retryTransactionCh:
				if !open {
					return
				}
				backoffDuration := boff.NextBackOff()
				if backoffDuration == backoff.Stop {
					p.logger.Error("Maximum number of transaction processing retries has been met, gracefully terminating retry channel")
					p.shutSig.TriggerSoftStop()
				}

				p.logger.Debug("Retrying transaction in %s", backoffDuration)

				select {
				case <-time.After(backoffDuration):
					out <- tran
				case <-shutSig.SoftStopChan():
					return
				}

			case <-shutSig.SoftStopChan():
				return
			case <-ctx.Done():
				return
			}

		}

	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// loop continually ingests messages from a merged channel that combines
// the original input channel (from the input layer) and a retry channel.
//
// All transactions are wrapped in a custom ackFn where:
// - on nack/reject: a transaction is fed back into the retry channel
// - otherwise: transaction proceeds through the pipeline as normal
func (p *feedbackPipeline) loop() {
	closeNowCtx, cnDone := p.shutSig.HardStopCtx(context.Background())
	defer cnDone()

	closeChOnce := sync.OnceFunc(func() {
		close(p.transactionsOut)
	})

	defer func() {
		p.pipe.TriggerCloseNow()
		if err := p.pipe.WaitForClose(closeNowCtx); err != nil {
			p.logger.Error("Error waiting for pipe close: %v", err)
		}
		closeChOnce()
		p.shutSig.TriggerHasStopped()
	}()

	mergedCh := p.newMergeChannels(closeNowCtx)
	for {
		var tran message.Transaction
		var open bool

		select {
		case tran, open = <-mergedCh:
			if !open {
				return
			}
		case <-p.shutSig.HardStopChan():
			return
		}

		ackFn := func(ctx context.Context, err error) error {
			var ackErr error
			// TODO: Should we be acking every transaction?
			defer func() {
				ackErr = tran.Ack(closeNowCtx, err)
			}()

			if err != nil {
				p.isRetrying.Store(true)

				select {
				case p.retryTransactionCh <- tran:
					return err
				case <-p.shutSig.HardStopChan():
					return nil
				case <-closeNowCtx.Done():
					return closeNowCtx.Err()
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			p.isRetrying.Store(false)
			return ackErr
		}

		select {
		case p.transactionsOut <- message.NewTransactionFunc(tran.Payload.ShallowCopy(), ackFn):
		case <-p.shutSig.HardStopChan():
			return
		}
	}
}

func (p *feedbackPipeline) Consume(msgs <-chan message.Transaction) error {
	if p.transactionsIn != nil {
		return component.ErrAlreadyStarted
	}
	p.transactionsIn = msgs

	go p.loop()
	return p.pipe.Consume(p.transactionsOut)
}

func (p *feedbackPipeline) TransactionChan() <-chan message.Transaction {
	return p.pipe.TransactionChan()
}

func (p *feedbackPipeline) TriggerCloseNow() {
	p.pipe.TriggerCloseNow()
	p.shutSig.TriggerHardStop()
}

func (p *feedbackPipeline) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
