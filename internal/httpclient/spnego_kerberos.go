//go:build !windows
// +build !windows

package httpclient

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"os/user"
	"runtime"
	"strings"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/spnego"
)

func kerberosConfigFromPath(path string) (*config.Config, error) {
	file, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	return config.NewFromReader(file)
}

func kerberosConfig() (*config.Config, error) {
	configPath := os.Getenv("KRB5_CONFIG")

	if len(configPath) == 0 {
		switch runtime.GOOS {
		case "linux":
			configPath = "/etc/krb5.conf"
		case "darwin":
			configPath = "opt/local/etc/krb5.conf"
		default:
			configPath = "/etc/krb5/krb5.conf"
		}
		return kerberosConfigFromPath(configPath)
	}

	config, err := kerberosConfigFromPath(configPath)

	if err != nil {
		if os.IsNotExist(err) {
			switch runtime.GOOS {
			case "linux":
				configPath = "/etc/krb5.conf"
			case "darwin":
				configPath = "opt/local/etc/krb5.conf"
			default:
				configPath = "/etc/krb5/krb5.conf"
			}
			return kerberosConfigFromPath(configPath)
		}
	}

	return config, nil
}

type kerberosTransport struct {
	http.RoundTripper
}

func (t *kerberosTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body := bytes.Buffer{}
	if req.Body != nil {
		_, err := body.ReadFrom(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))
	}
	res, err := t.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusUnauthorized {
		return res, err
	}

	wwwAuthenticateHeaders := res.Header.Values("WWW-Authenticate")

	hasAuth := false

	for _, wwwAuthenticate := range wwwAuthenticateHeaders {
		if strings.HasPrefix(wwwAuthenticate, "Negotiate ") {
			hasAuth = true
			break
		}

		if strings.HasPrefix(wwwAuthenticate, "NTLM ") {
			hasAuth = true
			break
		}
	}

	if !hasAuth {
		return res, err
	}

	config, err := kerberosConfig()
	if err != nil {
		return nil, err
	}

	u, err := user.Current()
	if err != nil {
		return nil, err
	}

	ccpath := "/tmp/krb5cc_" + u.Uid

	ccname := os.Getenv("KRB5CCNAME")
	if strings.HasPrefix(ccname, "FILE:") {
		ccpath = strings.SplitN(ccname, ":", 2)[1]
	}

	ccache, err := credentials.LoadCCache(ccpath)
	if err != nil {
		return nil, err
	}

	client, err := client.NewFromCCache(ccache, config, client.DisablePAFXFAST(true))

	if err != nil {
		return nil, err
	}

	spn := req.Host

	if len(spn) == 0 {
		spn = req.URL.Host
	}

	spn = "http/" + spn

	err = spnego.SetSPNEGOHeader(client, req, spn)

	if err != nil {
		return nil, err
	}

	req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

	return t.RoundTripper.RoundTrip(req)
}

func newSPPNEGORoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &kerberosTransport{
		RoundTripper: base,
	}
}
