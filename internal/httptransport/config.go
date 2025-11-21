package httptransport

import (
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	hcFieldCustomTransportEnabled = "enabled"
	hcFieldCustomTransport        = "custom_transport"
	hcFieldDialContext            = "dial_context"
	hcFieldDialContextTimeout     = "timeout"
	hcFieldDialContextKeepAlive   = "keep_alive"
	hcFieldForceAttemptHTTP2      = "force_http2"
	hcFieldMaxIdleConns           = "max_idle_connections"
	hcFieldIdleConnTimeout        = "idle_connection_timeout"
	hcFieldTLSHandshakeTimeout    = "tls_handshake_timeout"
	hcFieldExpectContinueTimeout  = "expect_continue_timeout"
)

func CustomTransportConfigSpec() *service.ConfigField {
	return service.NewObjectField(hcFieldCustomTransport,
		service.NewBoolField(hcFieldCustomTransportEnabled).
			Description("Enables a custom HTTP transport. When set to `false` (default), Bento will use Go's [DefaultTransport](https://pkg.go.dev/net/http#DefaultTransport) for the underlying net/http transport. When enabled, settings from the `custom_transport` fields will be applied to the underlying transport. Note that other fields that modify the transport, such as TLS & ProxyURL, will always apply to the transport. The Env Var 'BENTO_OVERRIDE_DEFAULT_HTTP_TRANSPORT' can also be used to ensure the DefaultTransport isn't used.").
			Advanced().
			Version("1.13.0").
			Default(false),
		service.NewObjectField(hcFieldDialContext,
			service.NewDurationField(hcFieldDialContextTimeout).
				Description("Timeout for establishing new network connections.").
				Advanced().
				Version("1.13.0").
				Default("30s"),
			service.NewDurationField(hcFieldDialContextKeepAlive).
				Description("Keep-alive period for an active network connections used by the dialer.").
				Advanced().
				Version("1.13.0").
				Default("30s"),
		).
			Description("Settings for the dialer used to create new connections.").
			Advanced().
			Version("1.13.0").
			Optional(),
		service.NewBoolField(hcFieldForceAttemptHTTP2).
			Description("If true, the transport will attempt to use HTTP/2.").
			Advanced().
			Version("1.13.0").
			Default(true),
		service.NewIntField(hcFieldMaxIdleConns).
			Description("Controls the maximum number of idle (keep-alive) connections across all hosts. Zero means no limit.").
			Advanced().
			Version("1.13.0").
			Default(100),
		service.NewDurationField(hcFieldIdleConnTimeout).
			Description("Maximum amount of time an idle (keep-alive) connection will remain idle before closing itself.").
			Advanced().
			Version("1.13.0").
			Default("90s"),
		service.NewDurationField(hcFieldTLSHandshakeTimeout).
			Description("Maximum time allowed for the TLS handshake to complete when establishing connections.").
			Advanced().
			Version("1.13.0").
			Default("10s"),
		service.NewDurationField(hcFieldExpectContinueTimeout).
			Description("Time to wait for a server's first response headers after fully writing the request headers if the request has an 'Expect: 100-continue' header. Zero means no timeout and causes the body to be sent immediately, without waiting for the server to approve.").
			Advanced().
			Version("1.13.0").
			Default("1s"),
	).
		Description("Custom transport options.").
		Advanced()
}

type CustomTransport struct {
	CustomTransportEnabled bool
	DialContextTimeout     time.Duration
	DialContextKeepAlive   time.Duration
	ForceAttemptHTTP2      bool
	MaxIdleConns           int
	IdleConnTimeout        time.Duration
	TlsHandshakeTimeout    time.Duration
	ExpectContinueTimeout  time.Duration
}
