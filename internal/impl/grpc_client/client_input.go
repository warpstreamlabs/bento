package grpc_client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	grpcClientInputAddress    = "address"     // duplicate
	grpcClientInputService    = "service"     // duplicate
	grpcClientInputMethod     = "method"      // duplicate
	grpcClientInputRPCType    = "rpc_type"    // duplicate
	grpcClientInputReflection = "reflection"  // duplicate
	grpcClientInputProtoFiles = "proto_files" // duplicate
	grpcClientInputPayload    = "payload"
	grpcClientInputRateLimit  = "rate_limit"
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
			service.NewStringEnumField(
				grpcClientInputRPCType,
				[]string{rpcTypeUnary, rpcTypeClientStream, rpcTypeServerStream, rpcTypeBidi}...,
			).Default("unary"),
			service.NewBoolField(grpcClientInputReflection).
				Description("If set to true, Bento will acquire the protobuf schema for the method from the server via [gRPC Reflection](https://grpc.io/docs/guides/reflection/).").
				Default(false),
			service.NewStringListField(grpcClientOutputProtoFiles).
				Description("A list of filepaths of .proto files that should contain the schemas necessary for the gRPC method.").
				Default([]any{}).
				Example([]string{"./grpc_test_server/helloworld.proto"}),
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
	address       string
	serviceName   string
	methodName    string
	rpcType       string
	reflection    bool
	protoFiles    []string
	reflectClient *grpcreflect.Client
	payloadExpr   *service.InterpolatedString
	rateLimit     string

	conn *grpc.ClientConn

	stub   grpcdynamic.Stub
	method *desc.MethodDescriptor

	mgr *service.Resources
	log *service.Logger
}

func newGrpcClientInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*grpcClientInput, error) {
	address, err := conf.FieldString(grpcClientInputAddress)
	if err != nil {
		return nil, err
	}
	serviceName, err := conf.FieldString(grpcClientInputService)
	if err != nil {
		return nil, err
	}
	methodName, err := conf.FieldString(grpcClientInputMethod)
	if err != nil {
		return nil, err
	}
	rpcType, err := conf.FieldString(grpcClientInputRPCType)
	if err != nil {
		return nil, err
	}
	reflection, err := conf.FieldBool(grpcClientInputReflection)
	if err != nil {
		return nil, err
	}
	protoFiles, err := conf.FieldStringList(grpcClientInputProtoFiles)
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
		address:     address,
		serviceName: serviceName,
		methodName:  methodName,
		rpcType:     rpcType,
		reflection:  reflection,
		protoFiles:  protoFiles,
		payloadExpr: payloadExpr,
		rateLimit:   rateLimit,
		mgr:         mgr,
		log:         mgr.Logger(),
	}, nil
}

func (gci *grpcClientInput) Connect(ctx context.Context) error {
	var err error

	dialOpts := []grpc.DialOption{}
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	gci.conn, err = grpc.NewClient(gci.address, dialOpts...)
	if err != nil {
		return err
	}

	if gci.reflection {
		gci.reflectClient = grpcreflect.NewClientAuto(ctx, gci.conn)

		serviceDescriptor, err := gci.reflectClient.ResolveService(gci.serviceName)
		if err != nil {
			return err
		}

		if method := serviceDescriptor.FindMethodByName(gci.methodName); method != nil {
			gci.method = method
		} else {
			return fmt.Errorf("method: %v not found", gci.methodName)
		}
	}

	if len(gci.protoFiles) != 0 {
		var parser protoparse.Parser

		fileDescriptors, err := parser.ParseFiles(gci.protoFiles...)
		if err != nil {
			return err
		}

	Found:
		for _, fileDescriptor := range fileDescriptors {
			for _, service := range fileDescriptor.GetServices() {
				if service.GetFullyQualifiedName() == gci.serviceName || service.GetName() == gci.serviceName {
					if method := service.FindMethodByName(gci.methodName); method != nil {
						gci.method = method
						break Found
					}
				}
			}
		}
	}

	gci.stub = grpcdynamic.NewStub(gci.conn)

	return nil
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
