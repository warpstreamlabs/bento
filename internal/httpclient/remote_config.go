package httpclient

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	config.SetRemoteConfigFetcher(fetchRemoteConfig)
}

// fetchRemoteConfig parses the given spec bytes as an httpclient config and
// performs an HTTP request to retrieve the main Bento configuration.
func fetchRemoteConfig(ctx context.Context, specBytes []byte) ([]byte, error) {
	spec := service.NewConfigSpec().Field(ConfigField("GET", false))
	pConf, err := spec.ParseYAML(string(specBytes), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse remote config spec: %w", err)
	}

	conf, err := ConfigFromParsed(pConf)
	if err != nil {
		return nil, fmt.Errorf("failed to build remote config client: %w", err)
	}

	msg := service.NewMessage(nil)

	urlStr, err := conf.URL.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve remote config URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, conf.Verb, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid remote config request: %w", err)
	}

	for k, v := range conf.Headers {
		hVal, hErr := v.TryString(msg)
		if hErr != nil {
			return nil, fmt.Errorf("failed to resolve header %v: %w", k, hErr)
		}
		req.Header.Set(k, hVal)
	}

	httpClient := &http.Client{Timeout: conf.Timeout}
	if conf.TLSEnabled && conf.TLSConf != nil {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: conf.TLSConf,
		}
	}
	if conf.authSigner != nil {
		if err := conf.authSigner(nil, req); err != nil {
			return nil, fmt.Errorf("failed to sign remote config request: %w", err)
		}
	}
	if conf.clientCtor != nil {
		httpClient = conf.clientCtor(ctx, httpClient)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch remote config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("remote config server returned status %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}
