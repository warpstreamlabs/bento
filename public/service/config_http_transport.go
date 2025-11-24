package service

import (
	"net"
	"net/http"

	httptran "github.com/warpstreamlabs/bento/internal/httptransport"
)

const (
	fieldDialContext           = "dial_context"
	fieldDialContextTimeout    = "timeout"
	fieldDialContextKeepAlive  = "keep_alive"
	fieldForceAttemptHTTP2     = "force_http2"
	fieldMaxIdleConns          = "max_idle_connections"
	fieldIdleConnTimeout       = "idle_connection_timeout"
	fieldTLSHandshakeTimeout   = "tls_handshake_timeout"
	fieldExpectContinueTimeout = "expect_continue_timeout"
)

// FieldHTTPTransport constructs an *http.Transport based on configuration fields found at the given path.
func (pConf *ParsedConfig) FieldHTTPTransport(path ...string) (transport *http.Transport, err error) {
	tranPConf := pConf.Namespace(path...)

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

	return transport, nil
}

func NewTransportField(name string) *ConfigField {
	tf := httptran.FieldSpec()
	tf.Name = name
	return &ConfigField{field: tf}
}
