package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	grpcClientInputPayload   = "payload"
	grpcClientInputRateLimit = "rate_limit"
)

func grpcClientInputSpec() *service.ConfigSpec {
	// TODO
	return service.NewConfigSpec().
		Summary("TODO").
		Description("TODO").
		Fields(grpcCommonFieldSpec()...).
		Fields(
			service.NewInterpolatedStringField(grpcClientInputPayload).
				Description("TODO").
				Example("TODO"),
			service.NewStringField(grpcClientInputRateLimit).
				Description("An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").
				Optional(),
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
	grpcCommon

	payloadExpr *service.InterpolatedString
	rateLimit   string

	mgr *service.Resources
	log *service.Logger
}

func newGrpcClientInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*grpcClientInput, error) {
	gcc, err := grpcCommonConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	payloadExpr, err := conf.FieldInterpolatedString(grpcClientInputPayload)
	if err != nil {
		return nil, err
	}
	var rateLimit string
	if conf.Contains(grpcClientInputRateLimit) {
		rateLimit, err = conf.FieldString(grpcClientInputRateLimit)
		if err != nil {
			return nil, err
		}
	}

	return &grpcClientInput{
		grpcCommon: gcc,

		payloadExpr: payloadExpr,
		rateLimit:   rateLimit,
		mgr:         mgr,
		log:         mgr.Logger(),
	}, nil
}

func (gci *grpcClientInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if gci.conn == nil || gci.method == nil {
		return nil, nil, service.ErrNotConnected
	}

	if gci.rateLimit != "" {
		gci.rateLimitWait(ctx)
	}

	//var err error
	switch gci.rpcType {
	case rpcTypeUnary:
		return gci.unaryHandler(ctx)
	case rpcTypeServerStream:
		return gci.serverStreamHandler(ctx)
	case rpcTypeClientStream:
		return gci.clientStreamHandler(ctx)
	default:
		panic(errors.New("NOT IMPL"))
	}
}

func (gci *grpcClientInput) Close(ctx context.Context) error {
	return nil
}

func (gci *grpcClientInput) rateLimitWait(ctx context.Context) bool {
	for {
		var period time.Duration
		var err error
		if rerr := gci.mgr.AccessRateLimit(ctx, gci.rateLimit, func(rl service.RateLimit) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			gci.log.Errorf("Rate limit error: %v\n", err)
			period = time.Second
		}

		if period > 0 {
			select {
			case <-time.After(period):
			case <-ctx.Done():
				return false
			}
		} else {
			return true
		}
	}
}

//------------------------------------------------------------------------------

func (gci *grpcClientInput) unaryHandler(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	request := dynamic.NewMessage(gci.method.GetInputType())

	payload, err := gci.payloadExpr.TryString(service.NewMessage([]byte(""))) //
	if err != nil {
		err = fmt.Errorf("payload interpolation error: %w", err)
		return nil, nil, err
	}

	err = request.UnmarshalJSON([]byte(payload))
	if err != nil {
		return nil, nil, err
	}

	resProtoMessage, err := gci.stub.InvokeRpc(ctx, gci.method, request)
	if err != nil {
		return nil, nil, err
	}

	dynMsg, ok := resProtoMessage.(*dynamic.Message)
	if !ok {
		// TODO Will need a new error here
		return nil, nil, err
	}

	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return nil, nil, err
	}

	responseMsg := service.MessageBatch{service.NewMessage(jsonBytes)}

	return responseMsg, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (gci *grpcClientInput) serverStreamHandler(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	request := dynamic.NewMessage(gci.method.GetInputType())

	payload, err := gci.payloadExpr.TryString(service.NewMessage([]byte(""))) //
	if err != nil {
		err = fmt.Errorf("payload interpolation error: %w", err)
		return nil, nil, err
	}

	err = request.UnmarshalJSON([]byte(payload))
	if err != nil {
		return nil, nil, err
	}

	serverStream, err := gci.stub.InvokeRpcServerStream(ctx, gci.method, request)
	if err != nil {
		return nil, nil, err
	}

	responseBatch := service.MessageBatch{}

	for {
		resProtoMessage, err := serverStream.RecvMsg()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
			jsonBytes, err := dynMsg.MarshalJSON()
			if err != nil {
				// log error
				continue
			}

			responseBatch = append(responseBatch, service.NewMessage(jsonBytes))
		}
	}

	return responseBatch, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (gci *grpcClientInput) clientStreamHandler(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	rawPayload, err := gci.payloadExpr.TryBytes(service.NewMessage([]byte("")))
	if err != nil {
		return nil, nil, err
	}

	var payloads []json.RawMessage
	if err := json.Unmarshal(rawPayload, &payloads); err != nil {
		return nil, nil, fmt.Errorf("UNABLE TO MARSHAL PAYLOAD FIELD TO []ANY")
	}

	clientStream, err := gci.stub.InvokeRpcClientStream(ctx, gci.method)
	if err != nil {
		return nil, nil, err
	}

	for _, payload := range payloads {
		request := dynamic.NewMessage(gci.method.GetInputType())
		if err := request.UnmarshalJSON(payload); err != nil {
			return nil, nil, err
		}

		err = clientStream.SendMsg(request)
		if err != nil {
			return nil, nil, err
		}
	}

	resProtoMessage, err := clientStream.CloseAndReceive()
	if err != nil {
		return nil, nil, err
	}

	dynMsg, ok := resProtoMessage.(*dynamic.Message)
	if !ok {
		return nil, nil, fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage)
	}

	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal proto response to JSON: %w", err)
	}

	responseMsg := service.MessageBatch{service.NewMessage(jsonBytes)}

	return responseMsg, func(rctx context.Context, res error) error {
		return nil
	}, nil
}
