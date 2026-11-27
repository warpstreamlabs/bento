package pure

import (
	"context"
	"fmt"

	"github.com/Jeffail/shutdown"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	errFieldInput = "input"
)

func errorInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Summary("TODO").
		Description("TODO").
		Fields(
			service.NewInputField(errFieldInput).
				Description("TODO"),
		)
}

func init() {
	err := service.RegisterBatchInput("catch_connection_error", errorInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newErrorInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

type errorInput struct {
	child        input.Streamed
	errs         chan error
	transactions chan message.Transaction
	shutSig      *shutdown.Signaller
}

func newErrorInputFromParsed(conf *service.ParsedConfig, res *service.Resources) (input.Streamed, error) {
	mgr := interop.UnwrapManagement(res)

	raw, err := conf.FieldAny(errFieldInput)
	if err != nil {
		return nil, err
	}
	childConf, err := input.FromAny(mgr.Environment(), raw)
	if err != nil {
		return nil, err
	}

	e := &errorInput{
		errs:         make(chan error, 1),
		transactions: make(chan message.Transaction),
		shutSig:      shutdown.NewSignaller(),
	}

	handler := func(err error) {
		select {
		case e.errs <- err:
		default:
		}
	}

	childMgr := mgr.IntoPath(errFieldInput)
	h, ok := childMgr.(interface {
		WithInputErrorHandler(func(error)) bundle.NewManagement
	})
	if !ok {
		return nil, fmt.Errorf("manager %T does not support input error handlers", childMgr)
	}
	childMgr = h.WithInputErrorHandler(handler)

	if e.child, err = childMgr.NewInput(childConf); err != nil {
		return nil, err
	}

	go e.loop()
	return e, nil
}

func (e *errorInput) loop() {
	defer func() {
		e.child.TriggerStopConsuming()
		e.child.TriggerCloseNow()
		_ = e.child.WaitForClose(context.Background())
		close(e.transactions)
		e.shutSig.TriggerHasStopped()
	}()

	childTrans := e.child.TransactionChan()
	for {
		var tran message.Transaction
		select {
		case err := <-e.errs:
			part := message.NewPart(nil)
			part.ErrorSet(err)
			tran = message.NewTransactionFunc(message.Batch{part}, func(context.Context, error) error {
				return nil
			})
		case t, open := <-childTrans:
			if !open {
				return
			}
			tran = t
		case <-e.shutSig.SoftStopChan():
			return
		}

		select {
		case e.transactions <- tran:
		case <-e.shutSig.SoftStopChan():
			return
		}
	}
}

func (e *errorInput) ConnectionStatus() component.ConnectionStatuses {
	return e.child.ConnectionStatus()
}

func (e *errorInput) TransactionChan() <-chan message.Transaction {
	return e.transactions
}

func (e *errorInput) TriggerStopConsuming() {
	e.shutSig.TriggerSoftStop()
}

func (e *errorInput) TriggerCloseNow() {
	e.shutSig.TriggerHardStop()
}

func (e *errorInput) WaitForClose(ctx context.Context) error {
	select {
	case <-e.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
