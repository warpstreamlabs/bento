package grpc

import (
	"context"
	"encoding/json"
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

const grpcClientInputDescription = `

### Expected Message Format

Either the field ` + "`reflection` or `proto_files`" + ` must be supplied, which will provide the protobuf schema Bento will use to marshall the Bento message into protobuf.

`

const grpcClientInputPayloadDescription = `
For ` + "`rpc_type`" + ` values: unary, client_stream & server_stream, the payload field defines the data that is sent as the request.
In the instance of the client_stream rpc_type - this is expected to resolve to an array. The array will be sent as a stream, with the elements
making the individual messages.
`

func grpcClientInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Receives messages from a gRPC server.").
		Description(grpcClientInputDescription).
		Fields(grpcCommonFieldSpec()...).
		Fields(
			service.NewInterpolatedStringField(grpcClientInputPayload).
				Description(grpcClientInputPayloadDescription).
				Optional().
				Example("TODO"),
			service.NewStringField(grpcClientInputRateLimit).
				Description("An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").
				Optional(),
		).
		Field(service.NewAutoRetryNacksToggleField()).
		LintRule(
			`root = match {
  this.reflection == false && (!this.exists("proto_files") || this.proto_files.length() == 0) => "reflection must be true or proto_files must be populated"
  (this.rpc_type == "unary" || this.rpc_type == "client_stream" || this.rpc_type == "server_stream") && this.payload == null => "payload must be set for rpc_types: unary, client_stream, server_stream",
}`,
		)
}

func init() {
	err := service.RegisterBatchInput("grpc_client", grpcClientInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.BatchInput, err error) {
			rdr, err := newGrpcClientInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, rdr)
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

	bidiChan    chan (service.MessageBatch)
	bidiErrChan chan (error)

	mgr *service.Resources
	log *service.Logger
}

func newGrpcClientInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*grpcClientInput, error) {
	gcc, err := grpcCommonConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	var payloadExpr *service.InterpolatedString
	if conf.Contains(grpcClientInputPayload) {
		payloadExpr, err = conf.FieldInterpolatedString(grpcClientInputPayload)
		if err != nil {
			return nil, err
		}
	}

	var rateLimit string
	if conf.Contains(grpcClientInputRateLimit) {
		rateLimit, err = conf.FieldString(grpcClientInputRateLimit)
		if err != nil {
			return nil, err
		}

		if !mgr.HasRateLimit(rateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", rateLimit)
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
		err := gci.rateLimitWait(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	switch gci.rpcType {
	case rpcTypeUnary:
		return gci.unaryHandler(ctx)
	case rpcTypeServerStream:
		return gci.serverStreamHandler(ctx)
	case rpcTypeClientStream:
		return gci.clientStreamHandler(ctx)
	case rpcTypeBidi:
		if gci.bidiChan == nil {
			err := gci.startBidi(ctx)
			if err != nil {
				return nil, nil, err
			}
		}

		select {
		case re := <-gci.bidiChan:
			return re, func(rctx context.Context, res error) error {
				return nil
			}, nil
		case err := <-gci.bidiErrChan:
			return nil, nil, err
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

	default:
		return nil, nil, fmt.Errorf("rpc_type: %v not supported", gci.rpcType)
	}
}

func (gci *grpcClientInput) Close(ctx context.Context) error {
	return nil
}

func (gci *grpcClientInput) rateLimitWait(ctx context.Context) error {
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
				return ctx.Err()
			}
		} else {
			return nil
		}
	}
}

//------------------------------------------------------------------------------

func (gci *grpcClientInput) unaryHandler(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	request := dynamic.NewMessage(gci.method.GetInputType())

	if gci.payloadExpr == nil {
		return nil, nil, fmt.Errorf("payload is required when rpc_type: %v", gci.rpcType)
	}
	payload, err := gci.payloadExpr.TryString(service.NewMessage([]byte("")))
	if err != nil {
		return nil, nil, fmt.Errorf("payload interpolation error: %w", err)
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
		return nil, nil, fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage)
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

	if gci.payloadExpr == nil {
		return nil, nil, fmt.Errorf("payload is required when rpc_type: %v", gci.rpcType)
	}
	payload, err := gci.payloadExpr.TryString(service.NewMessage([]byte("")))
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
				gci.log.Errorf("failed to marshal proto response to JSON: %v", err)
				continue
			}

			responseBatch = append(responseBatch, service.NewMessage(jsonBytes))
		} else {
			gci.log.Errorf("expected dynamic.Message but got %T", resProtoMessage)
			continue
		}
	}

	return responseBatch, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (gci *grpcClientInput) clientStreamHandler(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if gci.payloadExpr == nil {
		return nil, nil, fmt.Errorf("payload is required when rpc_type: %v", gci.rpcType)
	}
	rawPayload, err := gci.payloadExpr.TryBytes(service.NewMessage([]byte("")))
	if err != nil {
		return nil, nil, err
	}

	var payloads []json.RawMessage
	if err := json.Unmarshal(rawPayload, &payloads); err != nil {
		return nil, nil, fmt.Errorf("payload must be an array for client streaming RPC: %w", err)
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

func (gci *grpcClientInput) startBidi(ctx context.Context) (err error) {
	gci.bidiChan = make(chan service.MessageBatch)
	gci.bidiErrChan = make(chan error)

	bidi, err := gci.stub.InvokeRpcBidiStream(ctx, gci.method)
	if err != nil {
		return err
	}

	go func() {
		for {
			resProtoMessage, err := bidi.RecvMsg()
			if err != nil {
				close(gci.bidiChan)
				if err == io.EOF {
					gci.bidiErrChan <- service.ErrNotConnected
					return
				} else {
					gci.bidiErrChan <- err
					return
				}
			}

			dynMsg, ok := resProtoMessage.(*dynamic.Message)
			if !ok {
				gci.bidiErrChan <- fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage)
				return
			}

			jsonBytes, err := dynMsg.MarshalJSON()
			if err != nil {
				gci.bidiErrChan <- fmt.Errorf("failed to marshal proto response to JSON: %w", err)
				return
			}

			responseMsg := service.MessageBatch{service.NewMessage(jsonBytes)}

			gci.bidiChan <- responseMsg
		}
	}()

	return nil
}
