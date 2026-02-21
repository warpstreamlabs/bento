package sql

import (
	"errors"
	"sync"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	SQLConnLintRule = `root = match {this.iam_enabled && this.exists("secret_name") => "both iam_enabled and secret_name cannot be set"}`
)

var (
	awsGetCredentialsGeneratorFn func(*service.ParsedConfig) (DSNBuilder, error)
)

func IsAWSEnabled(c *service.ParsedConfig) (enabled bool, err error) {
	iam, _ := c.FieldBool("iam_enabled")

	if c.Contains("secret_name") || iam {
		return true, nil
	}
	return false, nil
}

var (
	getAWSCredentials = sync.OnceValues(initAWSCredentials)
)

func initAWSCredentials() (func(*service.ParsedConfig) (DSNBuilder, error), error) {
	if awsGetCredentialsGeneratorFn != nil {
		return awsGetCredentialsGeneratorFn, nil
	}
	return defaultAWSGetCredentialsGenerator, nil
}

func AWSGetCredentialsGeneratorFn(c *service.ParsedConfig) (DSNBuilder, error) {
	genFn, err := getAWSCredentials()
	if err != nil {
		return nil, err
	}
	return genFn(c)
}

func SetAWSGetCredentialsGeneratorFn(fn func(*service.ParsedConfig) (DSNBuilder, error)) {
	awsGetCredentialsGeneratorFn = fn
}

func defaultAWSGetCredentialsGenerator(c *service.ParsedConfig) (DSNBuilder, error) {
	awsEnabled, err := IsAWSEnabled(c)
	if err != nil {
		return nil, err
	}
	if awsEnabled {
		return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
	}
	return func(dsn, driver string) (string, error) {
		return dsn, nil
	}, nil
}
