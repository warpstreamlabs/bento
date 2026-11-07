package pure

import (
	"context"
	"os"
	"sync"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	sdFieldExitCode = "exit_code"
)

func init() {
	err := service.RegisterBatchProcessor("shutdown", service.NewConfigSpec().
		Categories("Utility").
		Summary("Shuts down Bento with the configured exit code when a batch passes through.").
		Field(service.NewIntField(sdFieldExitCode).
			Description("The exit code to terminate the process with.").
			Default(0)),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			exitCode, err := conf.FieldInt(sdFieldExitCode)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p := newShutdown(exitCode, mgr)
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedBatchedProcessor("shutdown", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type shutdownProc struct {
	exitCode int
	log      log.Modular
	once     sync.Once
}

func newShutdown(exitCode int, mgr bundle.NewManagement) *shutdownProc {
	return &shutdownProc{
		exitCode: exitCode,
		log:      mgr.Logger(),
	}
}

func (s *shutdownProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	s.once.Do(func() {
		s.log.Info("Shutdown processor triggered, exiting with code %v", s.exitCode)
		os.Exit(s.exitCode)
	})
	return []message.Batch{msg}, nil
}

func (s *shutdownProc) Close(ctx context.Context) error {
	return nil
}
