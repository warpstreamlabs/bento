package grpc_client

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	grpcClientInputAddress = "address"
	grpcClientInputService = "service"
	grpcClientInputMethod  = "method"
	grpcClientInputPayload = "payload"
)

func grpcClientInputSpec() *service.ConfigSpec {
	// TODO
	return service.NewConfigSpec().
		Summary("TODO").
		Description("TODO").
		Fields(
			service.NewStringField(grpcClientInputAddress).
				Description("TODO").
				Example("TODO"),
			service.NewStringField(grpcClientInputService).
				Description("TODO").
				Example("TODO"),
			service.NewStringField(grpcClientInputMethod).
				Description("TODO").
				Example("TODO"),
			service.NewInterpolatedStringField(grpcClientInputPayload).
				Description("TODO").
				Example("TODO"),
		)
}

func init() {
	err := service.RegisterBatchInput("grpc_client", grpcClientInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.BatchInput, err error) {
			return newGrpcClientInputFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type grpcClientInput struct {
	address string
}

func newGrpcClientInputFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*grpcClientInput, error) {
	address, err := conf.FieldString(grpcClientInputAddress)
	if err != nil {
		return nil, err
	}

	return &grpcClientInput{
		address: address,
	}, nil
}

func (gci *grpcClientInput) Connect(ctx context.Context) error {
	return nil
}

func (gci *grpcClientInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	msgB := service.MessageBatch{service.NewMessage([]byte("hello world"))}
	return msgB, nil, nil
}

func (gci *grpcClientInput) Close(ctx context.Context) error {
	return nil
}
