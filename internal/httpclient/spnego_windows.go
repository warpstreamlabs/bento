package httpclient

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/alexbrainman/sspi/negotiate"
	"github.com/alexbrainman/sspi/ntlm"
)

type windowsTransport struct {
	http.RoundTripper
}

func (t *windowsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
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

	wwwAuthenticateHeaders := res.Header.Values("Www-Authenticate")

	hasNegotiate := false

	for _, wwwAuthenticate := range wwwAuthenticateHeaders {
		if strings.HasPrefix(wwwAuthenticate, "Negotiate") {
			hasNegotiate = true
			break
		}
	}

	if hasNegotiate {
		cred, err := negotiate.AcquireCurrentUserCredentials()
		if err != nil {
			return nil, err
		}
		defer cred.Release()

		targetName := req.Host

		if len(targetName) == 0 {
			targetName = req.URL.Host
		}

		targetName = "http/" + targetName

		secctx, token, err := negotiate.NewClientContext(cred, targetName)
		if err != nil {
			return nil, err
		}
		defer secctx.Release()

		req.Header.Set("Authorization", "Negotiate "+base64.StdEncoding.EncodeToString(token))

		req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

		res, err := t.RoundTripper.RoundTrip(req)

		if err != nil {
			return res, err
		}

		if res.StatusCode != http.StatusOK {
			return res, nil
		}

		wwwAuthenticateHeaders = res.Header.Values("WWW-Authenticate")

		var negotiateToken []byte

		for _, wwwAuthenticate := range wwwAuthenticateHeaders {
			if strings.HasPrefix(wwwAuthenticate, "Negotiate ") {
				firstSpaceIndex := strings.IndexRune(wwwAuthenticate, ' ')
				if firstSpaceIndex < 0 {
					return nil, errors.New("invalid negotiate auth header")
				}

				token, err := base64.StdEncoding.DecodeString(wwwAuthenticate[len("Negotiate "):])
				if err != nil {
					return nil, err
				}

				negotiateToken = token
				break
			}
		}

		if len(negotiateToken) == 0 {
			return nil, errors.New("empty negotiate auth header")
		}

		authCompleted, _, err := secctx.Update(negotiateToken)

		if err != nil {
			return nil, err
		}

		if !authCompleted {
			return nil, errors.New("client authentication not completed")
		}

		return res, nil
	}

	hasNTLM := false

	for _, wwwAuthenticate := range wwwAuthenticateHeaders {
		if strings.HasPrefix(wwwAuthenticate, "NTLM") {
			hasNTLM = true
			break
		}
	}

	if hasNTLM {
		cred, err := ntlm.AcquireCurrentUserCredentials()
		if err != nil {
			return nil, err
		}
		defer cred.Release()

		secctx, token, err := ntlm.NewClientContext(cred)
		if err != nil {
			return nil, err
		}
		defer secctx.Release()

		req.Header.Set("Authorization", "NTLM "+base64.StdEncoding.EncodeToString(token))

		req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

		res, err := t.RoundTripper.RoundTrip(req)

		if err != nil {
			return res, err
		}

		if res.StatusCode != http.StatusUnauthorized {
			return res, nil
		}

		wwwAuthenticateHeaders = res.Header.Values("Www-Authenticate")

		var ntlmToken []byte

		for _, wwwAuthenticate := range wwwAuthenticateHeaders {
			if strings.HasPrefix(wwwAuthenticate, "NTLM ") {
				firstSpaceIndex := strings.IndexRune(wwwAuthenticate, ' ')
				if firstSpaceIndex < 0 {
					return nil, errors.New("invalid NTLM auth header")
				}

				token, err := base64.StdEncoding.DecodeString(wwwAuthenticate[len("NTLM "):])
				if err != nil {
					return nil, err
				}

				ntlmToken = token
				break
			}
		}

		if len(ntlmToken) == 0 {
			return nil, errors.New("empty negotiate auth header")
		}

		authenticate, err := secctx.Update(ntlmToken)

		if err != nil {
			return nil, err
		}

		req.Header.Set("Authorization", "NTLM "+base64.StdEncoding.EncodeToString(authenticate))

		req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

		return t.RoundTripper.RoundTrip(req)
	}

	req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))

	return t.RoundTripper.RoundTrip(req)
}

func newSPPNEGORoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &windowsTransport{
		RoundTripper: base,
	}
}
