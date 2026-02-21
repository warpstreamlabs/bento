package s2

import (
	s2bentobox "github.com/s2-streamstore/s2-sdk-go/s2-bentobox"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	basinField       = "basin"
	accessTokenField = "access_token"
)

func newConfig(conf *service.ParsedConfig) (*s2bentobox.Config, error) {
	basin, err := conf.FieldString(basinField)
	if err != nil {
		return nil, err
	}

	accessToken, err := conf.FieldString(accessTokenField)
	if err != nil {
		return nil, err
	}

	return &s2bentobox.Config{
		Basin:       basin,
		AccessToken: accessToken,
	}, nil
}

type bentoLogger struct {
	*service.Logger
}

func (bl *bentoLogger) With(keyValuePairs ...any) s2bentobox.Logger {
	return &bentoLogger{
		Logger: bl.Logger.With(keyValuePairs...),
	}
}
