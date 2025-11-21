package httptransport

import (
	"time"

	"github.com/warpstreamlabs/bento/internal/docs"
)

func FieldSpec() docs.FieldSpec {
	return docs.FieldObject(
		"custom_transport",
		"Custom transport options.",
	).Advanced().WithChildren(

		// enabled
		docs.FieldBool(
			"enabled",
			"Enables a custom HTTP transport. When false (default), Bento uses Go's DefaultTransport. "+
				"When true, the custom_transport settings override. TLS and ProxyURL always apply. "+
				"The env var BENTO_OVERRIDE_DEFAULT_HTTP_TRANSPORT=true forces avoiding DefaultTransport.",
		).HasDefault(false).AtVersion("1.13.0").Advanced(),

		// dial_context
		docs.FieldObject(
			"dial_context",
			"Settings for the dialer used to create new connections.",
		).Optional().Advanced().AtVersion("1.13.0").WithChildren(

			docs.FieldString(
				"timeout",
				"Timeout for establishing new network connections.",
			).HasDefault("30s").AtVersion("1.13.0").Advanced(),

			docs.FieldString(
				"keep_alive",
				"Keep-alive period for active network connections used by the dialer.",
			).HasDefault("30s").AtVersion("1.13.0").Advanced(),
		),

		docs.FieldBool(
			"force_http2",
			"If true, the transport will attempt to use HTTP/2.",
		).HasDefault(true).AtVersion("1.13.0").Advanced(),

		docs.FieldInt(
			"max_idle_connections",
			"Maximum number of idle keep-alive connections. Zero = unlimited.",
		).HasDefault(100).AtVersion("1.13.0").Advanced(),

		docs.FieldString(
			"idle_connection_timeout",
			"Maximum time an idle keep-alive connection remains open before closing itself.",
		).HasDefault("90s").AtVersion("1.13.0").Advanced(),

		// tls_handshake_timeout
		docs.FieldString(
			"tls_handshake_timeout",
			"Maximum time allowed for TLS handshake to complete.",
		).HasDefault("10s").AtVersion("1.13.0").Advanced(),

		// expect_continue_timeout
		docs.FieldString(
			"expect_continue_timeout",
			"Time to wait for a server's first response headers after sending request headers when 'Expect: 100-continue' is used. Zero means send body immediately.",
		).HasDefault("1s").AtVersion("1.13.0").Advanced(),
	)
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
