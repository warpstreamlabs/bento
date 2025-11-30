package httptransport

import (
	"github.com/warpstreamlabs/bento/internal/docs"
)

func FieldSpec() docs.FieldSpec {
	return docs.FieldObject(
		"transport",
		"Custom transport options.",
	).Advanced().AtVersion("1.13.0").WithChildren(

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

		docs.FieldString(
			"tls_handshake_timeout",
			"Maximum time allowed for TLS handshake to complete.",
		).HasDefault("10s").AtVersion("1.13.0").Advanced(),

		docs.FieldString(
			"expect_continue_timeout",
			"Time to wait for a server's first response headers after sending request headers when 'Expect: 100-continue' is used. Zero means send body immediately.",
		).HasDefault("1s").AtVersion("1.13.0").Advanced(),
	)
}
