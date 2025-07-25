package sql

import (
	"errors"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	SQLConnLintRule = `root = match {this.iam_enabled && this.exists("secret_name") => "both iam_enabled and secret_name cannot be set"}`
)

func IsAWSEnabled(c *service.ParsedConfig) (enabled bool, err error) {
	iam, _ := c.FieldBool("iam_enabled")

	if c.Contains("secret_name") || iam {
		return true, nil
	}
	return false, nil
}

// AWSGetCredentialsGeneratorFn is populated with the child `aws` package when imported.
var AWSGetCredentialsGeneratorFn = func(c *service.ParsedConfig) (fn func(dsn, driver string) (password string, err error), err error) {
	awsEnabled, err := IsAWSEnabled(c)
	if err != nil {
		return nil, err
	}
	if awsEnabled {
		return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
	}
	return
}

var BuildAwsDsn = func(dsn, driver string) (newDsn string, err error) {
	return dsn, nil
}
