package io

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/codec"
)

// TODO: Fan this out when appropriate?
func getStdinReader() io.ReadCloser {
	return io.NopCloser(os.Stdin)
}

func init() {
	err := service.RegisterBatchInput(
		"stdin", service.NewConfigSpec().
			Stable().
			Categories("Local").
			Summary(`Consumes data piped to stdin, chopping it into individual messages according to the specified scanner.`).
			Fields(codec.DeprecatedCodecFields("lines")...).Field(service.NewAutoRetryNacksToggleField()),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := newStdinConsumerFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

type stdinConsumer struct {
	scanner codec.DeprecatedFallbackStream
}

func newStdinConsumerFromParsed(conf *service.ParsedConfig) (*stdinConsumer, error) {
	c, err := codec.DeprecatedCodecFromParsed(conf)
	if err != nil {
		return nil, err
	}

	s, err := c.Create(getStdinReader(), func(_ context.Context, err error) error {
		return nil
	}, service.NewScannerSourceDetails())
	if err != nil {
		return nil, err
	}
	return &stdinConsumer{scanner: s}, nil
}

func (s *stdinConsumer) Connect(ctx context.Context) error {
	return nil
}

func (s *stdinConsumer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	parts, codecAckFn, err := s.scanner.NextBatch(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			s.scanner.Close(ctx)
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, err
	}
	_ = codecAckFn(ctx, nil)

	if len(parts) == 0 {
		return nil, nil, component.ErrTimeout
	}

	return parts, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (s *stdinConsumer) Close(ctx context.Context) (err error) {
	if s.scanner != nil {
		err = s.scanner.Close(ctx)
	}
	return
}
