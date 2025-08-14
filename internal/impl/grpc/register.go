package grpc

import (
	"github.com/warpstreamlabs/bento/public/service"
)

func Register() error {
	if err := registerGRPCInput(); err != nil {
		return err
	}
	if err := registerGRPCOutput(); err != nil {
		return err
	}
	return nil
}

func registerGRPCInput() error {
	spec := service.NewConfigSpec().
		Version("1.6.0").
		Categories("Services").
		Summary("Receive messages from a gRPC server-side stream using a simple proto contract.").
		Description("Subscribes to a server-side stream and emits each received frame as a message. Message body is taken from Frame.data and metadata from Frame.metadata. Supports TLS, bearer token auth, custom per-RPC headers, keepalive parameters, and client options such as authority, user agent, and load balancing policy.").
		Field(service.NewStringField("address").Description("Server address to connect to, e.g. host:port").Default("127.0.0.1:50051")).
		Field(service.NewStringField("consumer_id").Description("Identity sent on Subscribe request").Default("bento-client")).
		Field(service.NewStringField("bearer_token").Description("Bearer token for per-RPC auth").Secret().Optional()).
		Field(service.NewStringMapField("auth_headers").Description("Static headers to include per RPC").Optional()).
		Field(service.NewStringField("authority").Optional()).
		Field(service.NewStringField("user_agent").Optional()).
		Field(service.NewStringField("load_balancing_policy").Default("pick_first")).
		Field(service.NewIntField("max_send_msg_bytes").Default(0)).
		Field(service.NewIntField("max_recv_msg_bytes").Default(0)).
		Field(service.NewDurationField("keepalive_time").Default("0s")).
		Field(service.NewDurationField("keepalive_timeout").Default("0s")).
		Field(service.NewBoolField("keepalive_permit_without_stream").Default(false)).
		Field(service.NewTLSToggledField("tls"))

	spec = spec.Example("Basic subscribe",
		"Consume frames from a gRPC server.",
		`
input:
  grpc:
    address: 127.0.0.1:50051
    consumer_id: example-client
`,
	)

	return service.RegisterInput("grpc", spec, func(conf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
		address, err := conf.FieldString("address")
		if err != nil {
			return nil, err
		}
		consumerID, err := conf.FieldString("consumer_id")
		if err != nil {
			return nil, err
		}
		// Read optional auth/opts
		bearer, _ := conf.FieldString("bearer_token")
		headers, _ := conf.FieldStringMap("auth_headers")
		authority, _ := conf.FieldString("authority")
		userAgent, _ := conf.FieldString("user_agent")
		lb, _ := conf.FieldString("load_balancing_policy")
		maxSend, _ := conf.FieldInt("max_send_msg_bytes")
		maxRecv, _ := conf.FieldInt("max_recv_msg_bytes")
		kaTime, _ := conf.FieldDuration("keepalive_time")
		kaTimeout, _ := conf.FieldDuration("keepalive_timeout")
		kaPermit, _ := conf.FieldBool("keepalive_permit_without_stream")
		tlsConf, tlsEnabled, _ := conf.FieldTLSToggled("tls")
		if !tlsEnabled {
			tlsConf = nil
		}

		opts := clientOpts{
			authority:           authority,
			userAgent:           userAgent,
			loadBalancingPolicy: lb,
			maxSendMsgBytes:     maxSend,
			maxRecvMsgBytes:     maxRecv,
			keepalive: keepaliveConfig{
				time:                kaTime,
				timeout:             kaTimeout,
				permitWithoutStream: kaPermit,
			},
			auth: authConfig{
				bearerToken: bearer,
				headers:     headers,
			},
		}
		return newGRPCInput(address, consumerID, tlsConf, opts, res)
	})
}

func registerGRPCOutput() error {
	spec := service.NewConfigSpec().
		Version("1.6.0").
		Categories("Services").
		Summary("Send messages to a gRPC client-side stream using a simple proto contract.").
		Description("Opens a client-side stream and sends each message body as Frame.data along with message metadata as Frame.metadata. Supports TLS, bearer token auth, custom per-RPC headers, keepalive parameters, and client options such as authority, user agent, and load balancing policy.").
		Field(service.NewStringField("address").Description("Server address to dial, e.g. host:port").Default("127.0.0.1:50051")).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewStringField("bearer_token").Description("Bearer token for per-RPC auth").Secret().Optional()).
		Field(service.NewStringMapField("auth_headers").Description("Static headers to include per RPC").Optional()).
		Field(service.NewStringField("authority").Optional()).
		Field(service.NewStringField("user_agent").Optional()).
		Field(service.NewStringField("load_balancing_policy").Default("pick_first")).
		Field(service.NewIntField("max_send_msg_bytes").Default(0)).
		Field(service.NewIntField("max_recv_msg_bytes").Default(0)).
		Field(service.NewDurationField("keepalive_time").Default("0s")).
		Field(service.NewDurationField("keepalive_timeout").Default("0s")).
		Field(service.NewBoolField("keepalive_permit_without_stream").Default(false)).
		Field(service.NewTLSToggledField("tls").Default(false))

	spec = spec.Example("Basic publish",
		"Send messages to a gRPC server via a client-side stream.",
		`
output:
  grpc:
    address: 127.0.0.1:50051
    max_in_flight: 64
`,
	)

	return service.RegisterOutput("grpc", spec, func(conf *service.ParsedConfig, res *service.Resources) (service.Output, int, error) {
		address, err := conf.FieldString("address")
		if err != nil {
			return nil, 0, err
		}
		maxInFlight, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		// Optional auth/opts
		bearer, _ := conf.FieldString("bearer_token")
		headers, _ := conf.FieldStringMap("auth_headers")
		authority, _ := conf.FieldString("authority")
		userAgent, _ := conf.FieldString("user_agent")
		lb, _ := conf.FieldString("load_balancing_policy")
		maxSend, _ := conf.FieldInt("max_send_msg_bytes")
		maxRecv, _ := conf.FieldInt("max_recv_msg_bytes")
		kaTime, _ := conf.FieldDuration("keepalive_time")
		kaTimeout, _ := conf.FieldDuration("keepalive_timeout")
		kaPermit, _ := conf.FieldBool("keepalive_permit_without_stream")
		tlsConf, tlsEnabled, _ := conf.FieldTLSToggled("tls")
		if !tlsEnabled {
			tlsConf = nil
		}

		opts := clientOpts{
			authority:           authority,
			userAgent:           userAgent,
			loadBalancingPolicy: lb,
			maxSendMsgBytes:     maxSend,
			maxRecvMsgBytes:     maxRecv,
			keepalive: keepaliveConfig{
				time:                kaTime,
				timeout:             kaTimeout,
				permitWithoutStream: kaPermit,
			},
			auth: authConfig{
				bearerToken: bearer,
				headers:     headers,
			},
		}
		out, err := newGRPCOutput(address, tlsConf, opts, res)
		if err != nil {
			return nil, 0, err
		}
		return out, maxInFlight, nil
	})
}
