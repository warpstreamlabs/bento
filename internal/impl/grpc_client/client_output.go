package grpc_client

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	grpcClientOutputAddress                = "address"
	grpcClientOutputService                = "service"
	grpcClientOutputMethod                 = "method"
	grpcClientOutputRPCType                = "rpc_type"
	grpcClientOutputReflection             = "reflection"
	grpcClientOutputProtoFiles             = "proto_files"
	grpcClientOutputBatching               = "batching"
	grpcClientOutputPropRes                = "propagate_response"
	grpcClientOutputTls                    = "tls"
	grpcClientOutputHealthCheck            = "health_check"
	grpcClientOutputHealthCheckToggle      = "enabled"
	grpcClientOutputHealthCheckServiceName = "service"
)

const (
	rpcTypeUnary        = "unary"
	rpcTypeClientStream = "client_stream"
	rpcTypeServerStream = "server_stream"
	rpcTypeBidi         = "bidi"
)

const grpcClientOutputDescription = `

### Expected Message Format 

Either the field ` + "`reflection` or `proto_files`" + ` must be supplied, which will provide the protobuf schema Bento will use to marshall the Bento message into protobuf.

### Propagating Responses

It's possible to propagate the response(s) from each gRPC method invocation back to the input source by
setting ` + "`" + `propagate_response` + "` to `true`." + ` Only inputs that support [synchronous responses](/docs/guides/sync_responses)
are able to make use of these propagated responses. Also the  ` + "`" + `rpc_type` + "`" + `effects the behavior of what is returned via a sync_response:

- ` + "`" + rpcTypeUnary + "`" + `: The response propagated is a single message.` + `
- ` + "`" + rpcTypeClientStream + "`" + `: The response propagated is a single message.` + `
- ` + "`" + rpcTypeServerStream + "`" + `: The response propagated is a batch of messages.` + `
- ` + "`" + rpcTypeBidi + "`" + `: Any inbound message from the server is discarded.` + `
`

func grcpClientOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Sends messages to a GRPC server.").
		Description(grpcClientOutputDescription).
		Categories("network").
		Example("HTTP <--> gRPC Reverse Proxy", "Use Bento as a reverse proxy to translate HTTP requests into gRPC calls and return the response", `
input:
  http_server:
    path: /post

output:
  grpc_client:
    address: localhost:51286
    service: helloworld.Greeter
    method: SayHello
    propagate_response: true
    reflection: true
`).
		Fields(
			service.NewStringField(grpcClientOutputAddress).
				Description("The URI of the gRPC target to connect to.").
				Example("localhost:50051"),
			service.NewStringField(grpcClientOutputService).
				Description("The name of the service.").
				Example("helloworld.Greeter"),
			service.NewStringField(grpcClientOutputMethod).
				Description("The name of the method to invoke").
				Example("SayHello"),
			service.NewStringEnumField(
				grpcClientOutputRPCType,
				[]string{rpcTypeUnary, rpcTypeClientStream, rpcTypeServerStream, rpcTypeBidi}...,
			).
				Description("The type of the rpc method.").
				Default("unary"),
			service.NewBoolField(grpcClientOutputReflection).
				Description("If set to true, Bento will acquire the protobuf schema for the method from the server via [gRPC Reflection](https://grpc.io/docs/guides/reflection/).").
				Default(false),
			service.NewStringListField(grpcClientOutputProtoFiles).
				Description("A list of filepaths of .proto files that should contain the schemas necessary for the gRPC method.").
				Default([]any{}).
				Example([]string{"./grpc_test_server/helloworld.proto"}),
			service.NewBoolField(grpcClientOutputPropRes).
				Description("Whether responses from the server should be [propagated back](/docs/guides/sync_responses) to the input.").
				Default(false).
				Advanced(),
			service.NewObjectField(grpcClientOutputHealthCheck,
				service.NewBoolField(grpcClientOutputHealthCheckToggle).
					Description("Whether Bento should healthcheck the unary `Check` rpc endpoint on init connection: [gRPC Health Checking](https://grpc.io/docs/guides/health-checking/)").
					Default(false).
					Advanced(),
				service.NewStringField(grpcClientOutputHealthCheckServiceName).
					Description("The name of the service to healthcheck, note that the default value of \"\", will attempt to check the health of the whole server").
					Default("").
					Advanced(),
			),
			service.NewTLSToggledField(grpcClientOutputTls),
			oAuth2FieldSpec(),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(grpcClientOutputBatching),
		).LintRule(
		`root = match { 
  this.rpc_type == "bidi" && this.propagate_response == true => "cannot set propagate_response to true when rpc_type is bidi",
  this.reflection == false && (!this.exists("proto_files") || this.proto_files.length() == 0) => "reflection must be true or proto_files must be populated"
}`,
	)
}

// TODO: Deduplicate from http package.
const (
	aFieldOAuth2           = "oauth2"
	ao2FieldEnabled        = "enabled"
	ao2FieldClientKey      = "client_key"
	ao2FieldClientSecret   = "client_secret"
	ao2FieldTokenURL       = "token_url"
	ao2FieldScopes         = "scopes"
	ao2FieldEndpointParams = "endpoint_params"
)

func oAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField(aFieldOAuth2,
		service.NewBoolField(ao2FieldEnabled).
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),
		service.NewStringField(ao2FieldClientKey).
			Description("A value used to identify the client to the token provider.").
			Default(""),
		service.NewStringField(ao2FieldClientSecret).
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),
		service.NewURLField(ao2FieldTokenURL).
			Description("The URL of the token provider.").
			Default(""),
		service.NewStringListField(ao2FieldScopes).
			Description("A list of optional requested permissions.").
			Default([]any{}).
			Advanced(),
		service.NewAnyMapField(ao2FieldEndpointParams).
			Description("A list of optional endpoint parameters, values should be arrays of strings.").
			Advanced().
			Example(map[string]any{
				"foo": []string{"meow", "quack"},
				"bar": []string{"woof"},
			}).
			Default(map[string]any{}).
			Optional(),
	).
		Description("Allows you to specify open authentication via OAuth version 2 using the client credentials token flow.").
		Optional().Advanced()
}

type oauth2Config struct {
	enabled        bool
	clientKey      string
	clientSecret   string
	tokenURL       string
	scopes         []string
	endpointParams map[string][]string
}

func init() {
	err := service.RegisterBatchOutput("grpc_client", grcpClientOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}

			out, err = newGrpcClientWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type grpcClientWriter struct {
	address                string
	serviceName            string
	methodName             string
	rpcType                string
	reflection             bool
	reflectClient          *grpcreflect.Client
	protoFiles             []string
	propResponse           bool
	tls                    *tls.Config
	oauth                  oauth2Config
	healthCheckEnabled     bool
	healthCheckServiceName string

	conn *grpc.ClientConn

	stub   grpcdynamic.Stub
	method *desc.MethodDescriptor
}

func newGrpcClientWriterFromParsed(conf *service.ParsedConfig, _ *service.Resources) (*grpcClientWriter, error) {
	address, err := conf.FieldString(grpcClientOutputAddress)
	if err != nil {
		return nil, err
	}
	serviceName, err := conf.FieldString(grpcClientOutputService)
	if err != nil {
		return nil, err
	}
	methodName, err := conf.FieldString(grpcClientOutputMethod)
	if err != nil {
		return nil, err
	}
	rpcType, err := conf.FieldString(grpcClientOutputRPCType)
	if err != nil {
		return nil, err
	}
	reflection, err := conf.FieldBool(grpcClientOutputReflection)
	if err != nil {
		return nil, err
	}
	protoFiles, err := conf.FieldStringList(grpcClientOutputProtoFiles)
	if err != nil {
		return nil, err
	}
	propResponse, err := conf.FieldBool(grpcClientOutputPropRes)
	if err != nil {
		return nil, err
	}
	tls, err := conf.FieldTLS(grpcClientOutputTls)
	if err != nil {
		return nil, err
	}

	healthCheckConf := conf.Namespace(grpcClientOutputHealthCheck)
	healthCheckEnabled, err := healthCheckConf.FieldBool(grpcClientOutputHealthCheckToggle)
	if err != nil {
		return nil, err
	}
	healthCheckServiceName, err := healthCheckConf.FieldString(grpcClientOutputHealthCheckServiceName)
	if err != nil {
		return nil, err
	}

	oauthConf := conf.Namespace(aFieldOAuth2)

	enabled, err := oauthConf.FieldBool(ao2FieldEnabled)
	if err != nil {
		return nil, err
	}
	clientKey, err := oauthConf.FieldString(ao2FieldClientKey)
	if err != nil {
		return nil, err
	}
	clientSecret, err := oauthConf.FieldString(ao2FieldClientSecret)
	if err != nil {
		return nil, err
	}
	tokenURL, err := oauthConf.FieldString(ao2FieldTokenURL)
	if err != nil {
		return nil, err
	}
	scopes, err := oauthConf.FieldStringList(ao2FieldScopes)
	if err != nil {
		return nil, err
	}
	ep, err := oauthConf.FieldAnyMap(ao2FieldEndpointParams)
	if err != nil {
		return nil, err
	}

	endpointParams := map[string][]string{}
	for k, v := range ep {
		if endpointParams[k], err = v.FieldStringList(); err != nil {
			return nil, err
		}
	}

	oauth := oauth2Config{
		enabled:        enabled,
		clientKey:      clientKey,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		scopes:         scopes,
		endpointParams: endpointParams,
	}

	writer := &grpcClientWriter{
		address:      address,
		serviceName:  serviceName,
		methodName:   methodName,
		rpcType:      rpcType,
		reflection:   reflection,
		protoFiles:   protoFiles,
		propResponse: propResponse,
		tls:          tls,
		oauth:        oauth,

		healthCheckEnabled:     healthCheckEnabled,
		healthCheckServiceName: healthCheckServiceName,
	}

	return writer, nil
}

//------------------------------------------------------------------------------

func (gcw *grpcClientWriter) Connect(ctx context.Context) (err error) {
	if gcw.conn != nil && gcw.method != nil {
		return nil
	}

	dialOpts := []grpc.DialOption{}

	if gcw.tls != nil {
		creds := credentials.NewTLS(gcw.tls)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if gcw.oauth.enabled {
		oauth2Conf := &clientcredentials.Config{
			ClientID:       gcw.oauth.clientKey,
			ClientSecret:   gcw.oauth.clientSecret,
			TokenURL:       gcw.oauth.tokenURL,
			Scopes:         gcw.oauth.scopes,
			EndpointParams: gcw.oauth.endpointParams,
		}

		tokenSource := oauth2Conf.TokenSource(ctx)

		_, err := tokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to fetch OAuth2 token: %w", err)
		}

		perRPC := oauth.TokenSource{TokenSource: tokenSource}
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(perRPC))
	}

	if gcw.healthCheckEnabled {
		serviceConf := fmt.Sprintf(`{"healthCheckConfig": {"serviceName": "%v"}}`, gcw.healthCheckServiceName)
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(serviceConf))
	}

	gcw.conn, err = grpc.NewClient(gcw.address, dialOpts...)
	if err != nil {
		return err
	}

	if gcw.healthCheckEnabled {
		healthClient := grpc_health_v1.NewHealthClient(gcw.conn)
		resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{
			Service: gcw.healthCheckServiceName,
		})
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("service %q not healthy: %v", gcw.healthCheckServiceName, resp.GetStatus())
		}
	}

	if gcw.reflection {
		gcw.reflectClient = grpcreflect.NewClientAuto(ctx, gcw.conn)

		serviceDescriptor, err := gcw.reflectClient.ResolveService(gcw.serviceName)
		if err != nil {
			return err
		}

		if method := serviceDescriptor.FindMethodByName(gcw.methodName); method != nil {
			gcw.method = method
		} else {
			return fmt.Errorf("method: %v not found", gcw.methodName)
		}
	}

	if len(gcw.protoFiles) != 0 {
		var parser protoparse.Parser

		fileDescriptors, err := parser.ParseFiles(gcw.protoFiles...)
		if err != nil {
			return err
		}

	Found:
		for _, fileDescriptor := range fileDescriptors {
			for _, service := range fileDescriptor.GetServices() {
				if service.GetFullyQualifiedName() == gcw.serviceName || service.GetName() == gcw.serviceName {
					if method := service.FindMethodByName(gcw.methodName); method != nil {
						gcw.method = method
						break Found
					}
				}
			}
		}
	}

	if gcw.method == nil {
		return fmt.Errorf("unable to find method: %s in provided proto files", gcw.methodName)
	}

	gcw.stub = grpcdynamic.NewStub(gcw.conn)

	return nil
}

func (gcw *grpcClientWriter) WriteBatch(ctx context.Context, msgBatch service.MessageBatch) error {
	if gcw.conn == nil || gcw.method == nil {
		return service.ErrNotConnected
	}

	var err error
	switch gcw.rpcType {
	case rpcTypeUnary:
		err = gcw.unaryHandler(ctx, msgBatch)
	case rpcTypeClientStream:
		err = gcw.clientStreamHandler(ctx, msgBatch)
	case rpcTypeServerStream:
		err = gcw.serverStreamHandler(ctx, msgBatch)
	case rpcTypeBidi:
		err = gcw.bidirectionalHandler(ctx, msgBatch)
	}
	if err != nil {
		return err
	}

	return nil
}

func (gcw *grpcClientWriter) Close(ctx context.Context) (err error) {
	if gcw.reflectClient != nil {
		gcw.reflectClient.Reset()
	}
	if gcw.conn != nil {
		return gcw.conn.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

func (gcw *grpcClientWriter) unaryHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		resProtoMessage, err := gcw.stub.InvokeRpc(ctx, gcw.method, request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		if !gcw.propResponse {
			continue
		}

		dynMsg, ok := resProtoMessage.(*dynamic.Message)
		if !ok {
			batchErrFailed(i, fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage))
			continue
		}

		jsonBytes, err := dynMsg.MarshalJSON()
		if err != nil {
			batchErrFailed(i, fmt.Errorf("failed to marshal proto response to JSON: %w", err))
			continue
		}

		responseMsg := msg.Copy()
		responseMsg.SetBytes(jsonBytes)

		responseBatch := service.MessageBatch{responseMsg}
		if err := responseBatch.AddSyncResponse(); err != nil {
			batchErrFailed(i, err)
			continue
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (gcw *grpcClientWriter) clientStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {

	clientStream, err := gcw.stub.InvokeRpcClientStream(ctx, gcw.method)
	if err != nil {
		return err
	}

	for _, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			return err
		}

		err = clientStream.SendMsg(request)
		if err != nil {
			return err
		}
	}

	resProtoMessage, err := clientStream.CloseAndReceive()
	if err != nil {
		return err
	}

	if !gcw.propResponse {
		return nil
	}

	dynMsg, ok := resProtoMessage.(*dynamic.Message)
	if !ok {
		return fmt.Errorf("expected dynamic.Message but got %T", resProtoMessage)
	}

	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal proto response to JSON: %w", err)
	}

	responseMsg := msgBatch[0].Copy()
	responseMsg.SetBytes(jsonBytes)

	if err := responseMsg.AddSyncResponse(); err != nil {
		return err
	}

	return nil
}

func (gcw *grpcClientWriter) serverStreamHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

msgLoop:
	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		serverStream, err := gcw.stub.InvokeRpcServerStream(ctx, gcw.method, request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		if !gcw.propResponse {
			for {
				_, err := serverStream.RecvMsg()
				if err == io.EOF {
					break
				}
				if err != nil {
					batchErrFailed(i, fmt.Errorf("failed to receive from server stream: %w", err))
					continue msgLoop
				}
			}
			continue
		}

		responseBatch := service.MessageBatch{}

		for {
			resProtoMessage, err := serverStream.RecvMsg()
			if err == io.EOF {
				if len(responseBatch) > 0 {
					if err := responseBatch.AddSyncResponse(); err != nil {
						batchErrFailed(i, err)
						continue msgLoop
					}
				}
				break
			}

			if err != nil {
				batchErrFailed(i, fmt.Errorf("failed to receive from server stream: %w", err))
				continue msgLoop
			}

			if dynMsg, ok := resProtoMessage.(*dynamic.Message); ok {
				jsonBytes, err := dynMsg.MarshalJSON()
				if err != nil {
					batchErrFailed(i, fmt.Errorf("failed to marshal proto response to JSON: %w", err))
					continue msgLoop
				}

				responseMsg := msg.Copy()
				responseMsg.SetBytes(jsonBytes)
				responseBatch = append(responseBatch, responseMsg)
			}
		}
	}
	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	return nil
}

func (gcw *grpcClientWriter) bidirectionalHandler(ctx context.Context, msgBatch service.MessageBatch) error {
	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(msgBatch, err)
		}
		batchErr.Failed(i, err)
	}

	bidi, err := gcw.stub.InvokeRpcBidiStream(ctx, gcw.method)
	if err != nil {
		return err
	}

	for i, msg := range msgBatch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		request := dynamic.NewMessage(gcw.method.GetInputType())
		if err := request.UnmarshalJSON(msgBytes); err != nil {
			batchErrFailed(i, err)
			continue
		}

		err = bidi.SendMsg(request)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}

	if err := bidi.CloseSend(); err != nil {
		return err
	}
	return nil
}
