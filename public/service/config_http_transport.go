package service

import (
	"net"
	"net/http"
	"os"
)

const (
	fieldCustomTransportEnabled = "enabled"
	fieldDialContext            = "dial_context"
	fieldDialContextTimeout     = "timeout"
	fieldDialContextKeepAlive   = "keep_alive"
	fieldForceAttemptHTTP2      = "force_http2"
	fieldMaxIdleConns           = "max_idle_connections"
	fieldIdleConnTimeout        = "idle_connection_timeout"
	fieldTLSHandshakeTimeout    = "tls_handshake_timeout"
	fieldExpectContinueTimeout  = "expect_continue_timeout"
)

var overrideDefaultHTTPTransport bool = os.Getenv("BENTO_OVERRIDE_DEFAULT_HTTP_TRANSPORT") == "true"

// FieldHTTPTransport constructs an *http.Transport based on configuration fields found at the given path.
// It returns the transport, a boolean indicating if a custom transport is enabled, and an error if any configuration is invalid.
// The default transport can be overridden by setting the BENTO_OVERRIDE_DEFAULT_HTTP_TRANSPORT environment variable to "true".
func (pConf *ParsedConfig) FieldHTTPTransport(path ...string) (transport *http.Transport, customTransportEnabled bool, err error) {
	tranPConf := pConf.Namespace(path...)

	customTransportEnabled, err = tranPConf.FieldBool(fieldCustomTransportEnabled)
	if err != nil {
		return
	}
	if !customTransportEnabled && !overrideDefaultHTTPTransport {
		return nil, false, nil
	}

	dialContextTimeout, err := tranPConf.FieldDuration([]string{fieldDialContext, fieldDialContextTimeout}...)
	if err != nil {
		return
	}

	dialContextKeepAlive, err := tranPConf.FieldDuration([]string{fieldDialContext, fieldDialContextKeepAlive}...)
	if err != nil {
		return
	}

	forceAttemptHTTP2, err := tranPConf.FieldBool(fieldForceAttemptHTTP2)
	if err != nil {
		return
	}

	maxIdleConns, err := tranPConf.FieldInt(fieldMaxIdleConns)
	if err != nil {
		return
	}

	idleConnTimeout, err := tranPConf.FieldDuration(fieldIdleConnTimeout)
	if err != nil {
		return
	}

	tlsHandshakeTimeout, err := tranPConf.FieldDuration(fieldTLSHandshakeTimeout)
	if err != nil {
		return
	}

	expectContinueTimeout, err := tranPConf.FieldDuration(fieldExpectContinueTimeout)
	if err != nil {
		return
	}

	transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialContextTimeout,
			KeepAlive: dialContextKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     forceAttemptHTTP2,
		MaxIdleConns:          maxIdleConns,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
	}

	return transport, true, nil
}
