package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type authConfig struct {
	bearerToken string
	headers     map[string]string
}

type keepaliveConfig struct {
	time                time.Duration
	timeout             time.Duration
	permitWithoutStream bool
}

type clientOpts struct {
	authority           string
	userAgent           string
	loadBalancingPolicy string
	maxSendMsgBytes     int
	maxRecvMsgBytes     int
	keepalive           keepaliveConfig
	auth                authConfig
}

func buildDialOptions(_ context.Context, useTLS bool, cfg clientOpts) ([]grpc.DialOption, error) {
	return newDialOptionsBuilder(useTLS, cfg).
		withTransport().
		withAuthority().
		withUserAgent().
		withLBPolicy().
		withKeepalive().
		withMsgSizeOpts().
		withPerRPCCreds().
		build()
}

// dialOptionsBuilder constructs grpc.DialOptions using a fluent API for readability.
type dialOptionsBuilder struct {
	useTLS   bool
	cfg      clientOpts
	opts     []grpc.DialOption
	callOpts []grpc.CallOption
}

func newDialOptionsBuilder(useTLS bool, cfg clientOpts) *dialOptionsBuilder {
	return &dialOptionsBuilder{
		useTLS: useTLS,
		cfg:    cfg,
	}
}

func (b *dialOptionsBuilder) withTransport() *dialOptionsBuilder {
	if b.useTLS {
		// Use default system roots; for advanced CA/mTLS provide a custom config downstream.
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		b.opts = append(b.opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		b.opts = append(b.opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return b
}

func (b *dialOptionsBuilder) withAuthority() *dialOptionsBuilder {
	if b.cfg.authority != "" {
		b.opts = append(b.opts, grpc.WithAuthority(b.cfg.authority))
	}
	return b
}

func (b *dialOptionsBuilder) withUserAgent() *dialOptionsBuilder {
	if b.cfg.userAgent != "" {
		b.opts = append(b.opts, grpc.WithUserAgent(b.cfg.userAgent))
	}
	return b
}

func (b *dialOptionsBuilder) withLBPolicy() *dialOptionsBuilder {
	if b.cfg.loadBalancingPolicy != "" {
		b.opts = append(b.opts, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy":"%s"}`, b.cfg.loadBalancingPolicy)))
	}
	return b
}

func (b *dialOptionsBuilder) withKeepalive() *dialOptionsBuilder {
	if b.cfg.keepalive.time > 0 || b.cfg.keepalive.timeout > 0 || b.cfg.keepalive.permitWithoutStream {
		b.opts = append(b.opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                b.cfg.keepalive.time,
			Timeout:             b.cfg.keepalive.timeout,
			PermitWithoutStream: b.cfg.keepalive.permitWithoutStream,
		}))
	}
	return b
}

func (b *dialOptionsBuilder) withMsgSizeOpts() *dialOptionsBuilder {
	if b.cfg.maxSendMsgBytes > 0 {
		b.callOpts = append(b.callOpts, grpc.MaxCallSendMsgSize(b.cfg.maxSendMsgBytes))
	}
	if b.cfg.maxRecvMsgBytes > 0 {
		b.callOpts = append(b.callOpts, grpc.MaxCallRecvMsgSize(b.cfg.maxRecvMsgBytes))
	}
	return b
}

func (b *dialOptionsBuilder) withPerRPCCreds() *dialOptionsBuilder {
	if b.cfg.auth.bearerToken != "" || len(b.cfg.auth.headers) > 0 {
		b.opts = append(b.opts, grpc.WithPerRPCCredentials(headerCreds{
			token:      b.cfg.auth.bearerToken,
			headers:    b.cfg.auth.headers,
			secureOnly: true,
		}))
	}
	return b
}

func (b *dialOptionsBuilder) build() ([]grpc.DialOption, error) {
	if len(b.callOpts) > 0 {
		b.opts = append(b.opts, grpc.WithDefaultCallOptions(b.callOpts...))
	}
	return b.opts, nil
}

type headerCreds struct {
	token      string
	headers    map[string]string
	secureOnly bool
}

func (h headerCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	md := map[string]string{}
	if h.token != "" {
		md["authorization"] = "Bearer " + h.token
	}
	for k, v := range h.headers {
		md[k] = v
	}
	return md, nil
}

func (h headerCreds) RequireTransportSecurity() bool { return h.secureOnly }
