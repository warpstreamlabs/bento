package sql

import (
	"errors"

	"github.com/warpstreamlabs/bento/public/service"
)

type AzureDSNBuilder func(dsn, driver string) (builtDSN string, err error)

var AzureGetCredentialsGeneratorFn = func(pConf *service.ParsedConfig) (fn AzureDSNBuilder, err error) {
	entraEnabled, err := pConf.FieldBool("azure", "entra_enabled")
	if err != nil {
		return nil, err
	}
	if entraEnabled {
		return nil, errors.New("unable to configure Azure Entra authentication as this binary doesn't import components/azure")
	}
	return
}

var BuildAzureDsn = func(dsn, driver string) (builtDsn string, err error) {
	return dsn, nil
}
