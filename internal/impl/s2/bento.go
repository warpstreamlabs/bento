package s2

import (
	s2bentobox "github.com/s2-streamstore/s2-sdk-go/s2-bentobox"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	basinField     = "basin"
	authTokenField = "auth_token"
)

var (
	basinFieldSpec = service.NewStringField(basinField).
			Description("Basin name")

	authTokenFieldSpec = service.NewStringField(authTokenField).
				Description("Authentication token for S2 account").
				Secret()
)

func newConfigSpec() *service.ConfigSpec {
	// TODO: Add summary, description etc.
	return service.NewConfigSpec().
		Fields(basinFieldSpec, authTokenFieldSpec)
}

func newConfig(conf *service.ParsedConfig) (*s2bentobox.Config, error) {
	basin, err := conf.FieldString(basinField)
	if err != nil {
		return nil, err
	}

	authToken, err := conf.FieldString(authTokenField)
	if err != nil {
		return nil, err
	}

	return &s2bentobox.Config{
		Basin:     basin,
		AuthToken: authToken,
	}, nil
}

type bentoLogger struct {
	L *service.Logger
}

func (bl *bentoLogger) Tracef(template string, args ...any) {
	bl.L.Tracef(template, args...)
}

func (bl *bentoLogger) Trace(message string) {
	bl.L.Trace(message)
}

func (bl *bentoLogger) Debugf(template string, args ...any) {
	bl.L.Debugf(template, args...)
}

func (bl *bentoLogger) Debug(message string) {
	bl.L.Debug(message)
}

func (bl *bentoLogger) Infof(template string, args ...any) {
	bl.L.Infof(template, args...)
}

func (bl *bentoLogger) Info(message string) {
	bl.L.Info(message)
}

func (bl *bentoLogger) Warnf(template string, args ...any) {
	bl.L.Warnf(template, args...)
}

func (bl *bentoLogger) Warn(message string) {
	bl.L.Warn(message)
}

func (bl *bentoLogger) Errorf(template string, args ...any) {
	bl.L.Errorf(template, args...)
}

func (bl *bentoLogger) Error(message string) {
	bl.L.Error(message)
}

func (bl *bentoLogger) With(keyValuePairs ...any) s2bentobox.Logger {
	return &bentoLogger{
		L: bl.L.With(keyValuePairs...),
	}
}
