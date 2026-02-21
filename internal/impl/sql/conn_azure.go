package sql

import (
	"errors"
	"sync"

	"github.com/warpstreamlabs/bento/public/service"
)

var (
	azureGetCredentialsGeneratorFn func(*service.ParsedConfig) (DSNBuilder, error)
	getAzureCredentials            = sync.OnceValues(initAzureCredentials)
)

func IsAzureEnabled(c *service.ParsedConfig) (enabled bool, err error) {
	entra, _ := c.FieldBool("azure", "entra_enabled")

	if entra {
		return true, nil
	}
	return false, nil
}

func initAzureCredentials() (func(*service.ParsedConfig) (DSNBuilder, error), error) {
	if azureGetCredentialsGeneratorFn != nil {
		return azureGetCredentialsGeneratorFn, nil
	}
	return defaultAzureGetCredentialsGenerator, nil
}

func AzureGetCredentialsGeneratorFn(c *service.ParsedConfig) (DSNBuilder, error) {
	genFn, err := getAzureCredentials()
	if err != nil {
		return nil, err
	}
	return genFn(c)
}

func SetAzureGetCredentialsGeneratorFn(fn func(*service.ParsedConfig) (DSNBuilder, error)) {
	azureGetCredentialsGeneratorFn = fn
}

func defaultAzureGetCredentialsGenerator(c *service.ParsedConfig) (DSNBuilder, error) {
	entraEnabled, err := IsAzureEnabled(c)
	if err != nil {
		return nil, err
	}
	if entraEnabled {
		return nil, errors.New("unable to configure Azure Entra authentication as this binary doesn't import components/azure")
	}
	return func(dsn, driver string) (string, error) {
		return dsn, nil
	}, nil
}
