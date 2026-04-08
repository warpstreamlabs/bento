package grpc

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/warpstreamlabs/bento/public/service"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	grpcClientAddress                = "address"
	grpcClientService                = "service"
	grpcClientMethod                 = "method"
	grpcClientRPCType                = "rpc_type"
	grpcClientReflection             = "reflection"
	grpcClientProtoFiles             = "proto_files"
	grpcClientTLS                    = "tls"
	grpcClientHealthCheck            = "health_check"
	grpcClientHealthCheckToggle      = "enabled"
	grpcClientHealthCheckServiceName = "service"

	rpcTypeUnary        = "unary"
	rpcTypeClientStream = "client_stream"
	rpcTypeServerStream = "server_stream"
	rpcTypeBidi         = "bidi"

	oa2FieldOAuth2         = "oauth2"
	oa2FieldEnabled        = "enabled"
	oa2FieldClientKey      = "client_key"
	oa2FieldClientSecret   = "client_secret"
	oa2FieldTokenURL       = "token_url"
	oa2FieldScopes         = "scopes"
	oa2FieldEndpointParams = "endpoint_params"
)

func grpcCommonFieldSpec() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(grpcClientAddress).
			Description("The URI of the gRPC target to connect to.").
			Example("localhost:50051"),
		service.NewStringField(grpcClientService).
			Description("The name of the service.").
			Example("helloworld.Greeter"),
		service.NewStringField(grpcClientMethod).
			Description("The name of the method to invoke").
			Example("SayHello"),
		service.NewStringEnumField(
			grpcClientRPCType,
			[]string{rpcTypeUnary, rpcTypeClientStream, rpcTypeServerStream, rpcTypeBidi}...,
		).
			Description("The type of the rpc method.").
			Default("unary"),
		service.NewBoolField(grpcClientReflection).
			Description("If set to true, Bento will acquire the protobuf schema for the method from the server via [gRPC Reflection](https://grpc.io/docs/guides/reflection/).").
			Default(false),
		service.NewStringListField(grpcClientProtoFiles).
			Description("A list of filepaths of .proto files that should contain the schemas necessary for the gRPC method.").
			Default([]any{}).
			Example([]string{"./grpc_test_server/helloworld.proto"}),
		service.NewTLSToggledField(grpcClientTLS),
		oAuth2FieldSpec(),
		service.NewObjectField(grpcClientHealthCheck,
			service.NewBoolField(grpcClientHealthCheckToggle).
				Description("Whether Bento should healthcheck the unary `Check` rpc endpoint on init connection: [gRPC Health Checking](https://grpc.io/docs/guides/health-checking/)").
				Default(false).
				Advanced(),
			service.NewStringField(grpcClientHealthCheckServiceName).
				Description("The name of the service to healthcheck, note that the default value of \"\", will attempt to check the health of the whole server").
				Default("").
				Advanced(),
		),
	}
}

type oauth2Config struct {
	enabled        bool
	clientKey      string
	clientSecret   string
	tokenURL       string
	scopes         []string
	endpointParams map[string][]string
}

func oAuth2FieldSpec() *service.ConfigField {
	return service.NewObjectField(oa2FieldOAuth2,
		service.NewBoolField(oa2FieldEnabled).
			Description("Whether to use OAuth version 2 in requests.").
			Default(false),
		service.NewStringField(oa2FieldClientKey).
			Description("A value used to identify the client to the token provider.").
			Default(""),
		service.NewStringField(oa2FieldClientSecret).
			Description("A secret used to establish ownership of the client key.").
			Default("").Secret(),
		service.NewURLField(oa2FieldTokenURL).
			Description("The URL of the token provider.").
			Default(""),
		service.NewStringListField(oa2FieldScopes).
			Description("A list of optional requested permissions.").
			Default([]any{}).
			Advanced(),
		service.NewAnyMapField(oa2FieldEndpointParams).
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

type grpcCommonConfig struct {
	conn                   *grpc.ClientConn
	address                string
	method                 *desc.MethodDescriptor
	methodName             string
	tls                    *tls.Config
	oauth                  oauth2Config
	healthCheckEnabled     bool
	healthCheckServiceName string
	reflection             bool
	reflectClient          *grpcreflect.Client
	serviceName            string
	protoFiles             []string
	stub                   grpcdynamic.Stub
	rpcType                string
}

func grpcCommonConfigFromParsed(conf *service.ParsedConfig) (grpcCommonConfig, error) {
	address, err := conf.FieldString(grpcClientAddress)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	serviceName, err := conf.FieldString(grpcClientService)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	methodName, err := conf.FieldString(grpcClientMethod)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	rpcType, err := conf.FieldString(grpcClientRPCType)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	reflection, err := conf.FieldBool(grpcClientReflection)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	protoFiles, err := conf.FieldStringList(grpcClientProtoFiles)
	if err != nil {
		return grpcCommonConfig{}, err
	}

	tls, err := conf.FieldTLS(grpcClientTLS)
	if err != nil {
		return grpcCommonConfig{}, err
	}

	healthCheckConf := conf.Namespace(grpcClientHealthCheck)
	healthCheckEnabled, err := healthCheckConf.FieldBool(grpcClientHealthCheckToggle)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	healthCheckServiceName, err := healthCheckConf.FieldString(grpcClientHealthCheckServiceName)
	if err != nil {
		return grpcCommonConfig{}, err
	}

	oauthConf := conf.Namespace(oa2FieldOAuth2)

	enabled, err := oauthConf.FieldBool(oa2FieldEnabled)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	clientKey, err := oauthConf.FieldString(oa2FieldClientKey)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	clientSecret, err := oauthConf.FieldString(oa2FieldClientSecret)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	tokenURL, err := oauthConf.FieldString(oa2FieldTokenURL)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	scopes, err := oauthConf.FieldStringList(oa2FieldScopes)
	if err != nil {
		return grpcCommonConfig{}, err
	}
	ep, err := oauthConf.FieldAnyMap(oa2FieldEndpointParams)
	if err != nil {
		return grpcCommonConfig{}, err
	}

	endpointParams := map[string][]string{}
	for k, v := range ep {
		if endpointParams[k], err = v.FieldStringList(); err != nil {
			return grpcCommonConfig{}, err
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

	return grpcCommonConfig{
		address:                address,
		methodName:             methodName,
		tls:                    tls,
		oauth:                  oauth,
		healthCheckEnabled:     healthCheckEnabled,
		healthCheckServiceName: healthCheckServiceName,
		serviceName:            serviceName,
		reflection:             reflection,
		protoFiles:             protoFiles,
		rpcType:                rpcType,
	}, nil

}

func (gcc *grpcCommonConfig) Connect(ctx context.Context) (err error) {
	if gcc.conn != nil && gcc.method != nil {
		return nil
	}

	dialOpts := []grpc.DialOption{}

	if gcc.tls != nil {
		creds := credentials.NewTLS(gcc.tls)
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if gcc.oauth.enabled {
		oauth2Conf := &clientcredentials.Config{
			ClientID:       gcc.oauth.clientKey,
			ClientSecret:   gcc.oauth.clientSecret,
			TokenURL:       gcc.oauth.tokenURL,
			Scopes:         gcc.oauth.scopes,
			EndpointParams: gcc.oauth.endpointParams,
		}

		tokenSource := oauth2Conf.TokenSource(ctx)

		_, err := tokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to fetch OAuth2 token: %w", err)
		}

		perRPC := oauth.TokenSource{TokenSource: tokenSource}
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(perRPC))
	}

	if gcc.healthCheckEnabled {
		serviceConf := fmt.Sprintf(`{"healthCheckConfig": {"serviceName": "%v"}}`, gcc.healthCheckServiceName)
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(serviceConf))
	}

	gcc.conn, err = grpc.NewClient(gcc.address, dialOpts...)
	if err != nil {
		return err
	}

	if gcc.healthCheckEnabled {
		healthClient := grpc_health_v1.NewHealthClient(gcc.conn)
		resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{
			Service: gcc.healthCheckServiceName,
		})
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("service %q not healthy: %v", gcc.healthCheckServiceName, resp.GetStatus())
		}
	}

	if gcc.reflection {
		gcc.reflectClient = grpcreflect.NewClientAuto(ctx, gcc.conn)

		serviceDescriptor, err := gcc.reflectClient.ResolveService(gcc.serviceName)
		if err != nil {
			return err
		}

		if method := serviceDescriptor.FindMethodByName(gcc.methodName); method != nil {
			gcc.method = method
		} else {
			return fmt.Errorf("method: %v not found", gcc.methodName)
		}
	}

	if len(gcc.protoFiles) != 0 {
		var parser protoparse.Parser

		fileDescriptors, err := parser.ParseFiles(gcc.protoFiles...)
		if err != nil {
			return err
		}

	Found:
		for _, fileDescriptor := range fileDescriptors {
			for _, service := range fileDescriptor.GetServices() {
				if service.GetFullyQualifiedName() == gcc.serviceName || service.GetName() == gcc.serviceName {
					if method := service.FindMethodByName(gcc.methodName); method != nil {
						gcc.method = method
						break Found
					}
				}
			}
		}
	}

	if gcc.method == nil {
		return fmt.Errorf("unable to find method: %s in provided proto files", gcc.methodName)
	}

	gcc.stub = grpcdynamic.NewStub(gcc.conn)

	return nil
}
