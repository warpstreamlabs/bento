package grpc

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	rpcTypeUnary        = "unary"
	rpcTypeClientStream = "client_stream"
	rpcTypeServerStream = "server_stream"
	rpcTypeBidi         = "bidi"
)

type oauth2Config struct {
	enabled        bool
	clientKey      string
	clientSecret   string
	tokenURL       string
	scopes         []string
	endpointParams map[string][]string
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
