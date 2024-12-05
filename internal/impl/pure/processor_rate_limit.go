package pure

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/ratelimit"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	rlimitFieldResource = "resource"
)

func rlimitProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`Throttles the throughput of a pipeline according to a specified ` + "[`rate_limit`](/docs/components/rate_limits/about)" + ` resource. Rate limits are shared across components and therefore apply globally to all processing pipelines.`).
		Field(service.NewStringField(rlimitFieldResource).
			Description("The target [`rate_limit` resource](/docs/components/rate_limits/about)."))
}

func init() {
	err := service.RegisterBatchProcessor(
		"rate_limit", rlimitProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			resStr, err := conf.FieldString(rlimitFieldResource)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			r, err := newRateLimitProc(resStr, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("rate_limit", r, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type rateLimitProc struct {
	rlName string
	mgr    bundle.NewManagement

	closeChan chan struct{}
	closeOnce sync.Once
}

func newRateLimitProc(resStr string, mgr bundle.NewManagement) (*rateLimitProc, error) {
	if !mgr.ProbeRateLimit(resStr) {
		return nil, fmt.Errorf("rate limit resource '%v' was not found", resStr)
	}
	r := &rateLimitProc{
		rlName:    resStr,
		mgr:       mgr,
		closeChan: make(chan struct{}),
	}
	return r, nil
}

func (r *rateLimitProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	for {
		var waitFor time.Duration
		var err error
		if rerr := r.mgr.AccessRateLimit(ctx, r.rlName, func(rl ratelimit.V1) {
			v2, ok := rl.(ratelimit.MessageAwareRateLimit)
			if ok {
				v2.Add(ctx, msg)
			}

			waitFor, err = rl.Access(ctx)

		}); rerr != nil {
			err = rerr
		}
		if ctx.Err() != nil {
			return nil, err
		}
		if err != nil {
			r.mgr.Logger().Error("Failed to access rate limit: %v", err)
			waitFor = time.Second
		}
		if waitFor == 0 {
			return []*message.Part{msg}, nil
		}
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.closeChan:
			return nil, component.ErrTypeClosed
		}
	}
}

func (r *rateLimitProc) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
	return nil
}
