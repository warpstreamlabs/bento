package grpc_client

import (
	"context"
	"errors"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	grpcClientInputAddress    = "address"    // duplicate
	grpcClientInputService    = "service"    // duplicate
	grpcClientInputMethod     = "method"     // duplicate
	grpcClientInputRPCType    = "rpc_type"   // duplicate
	grpcClientInputReflection = "reflection" // duplicate
	grpcClientInputPayload    = "payload"
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
	address       string
	serviceName   string
	methodName    string
	rpcType       string
	reflection    bool
	reflectClient *grpcreflect.Client
	payloadExpr   *service.InterpolatedString

	conn *grpc.ClientConn

	stub   grpcdynamic.Stub
	method *desc.MethodDescriptor
}

func newGrpcClientInputFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*grpcClientInput, error) {
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
	payloadExpr, err := conf.FieldInterpolatedString(grpcClientInputPayload)
	if err != nil {
		return nil, err
	}

	return &grpcClientInput{
		address:     address,
		serviceName: serviceName,
		methodName:  methodName,
		rpcType:     rpcType,
		reflection:  reflection,
		payloadExpr: payloadExpr,
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
	} else {
		return errors.New("NOT IMPL.")
	}

	gci.stub = grpcdynamic.NewStub(gci.conn)

	return nil
}

func (gci *grpcClientInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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

	return responseMsg, nil, nil
}

func (gci *grpcClientInput) Close(ctx context.Context) error {
	return nil
}
