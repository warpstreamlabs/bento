package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	if conf.RateLimit != "" {
		return nil, errors.New("rate_limit is not supported when fetching a remote config")
	}

	client, err := NewClientFromOldConfig(conf, service.MockResources())
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = client.Close(ctx)
	}()

	res, err := client.SendToResponse(ctx, service.MessageBatch{service.NewMessage(nil)})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return io.ReadAll(res.Body)
}
