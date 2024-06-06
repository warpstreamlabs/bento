package common

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/log"
)

type CLIStreamBootstrapFunc func()

type CLIOpts struct {
	Version              string
	DateBuilt            string
	MainConfigSpecCtor   func() docs.FieldSpecs // TODO: This becomes a service.Environment
	OnManagerInitialised func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error
	OnLoggerInit         func(l log.Modular) (log.Modular, error)
}

func NewCLIOpts(version, dateBuilt string) *CLIOpts {
	return &CLIOpts{
		Version:            version,
		DateBuilt:          dateBuilt,
		MainConfigSpecCtor: config.Spec,
		OnManagerInitialised: func(mgr bundle.NewManagement, pConf *docs.ParsedConfig) error {
			return nil
		},
		OnLoggerInit: func(l log.Modular) (log.Modular, error) {
			return l, nil
		},
	}
}
