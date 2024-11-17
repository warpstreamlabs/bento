package sql

import (
	"errors"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	SqlFieldAWS = "aws"
)

// AWSGetCredentialsGeneratorFn is populated with the child `aws` package when imported.
var AWSGetCredentialsGeneratorFn = func(c *service.ParsedConfig) (fn func(dsn, driver string) (password string, err error), err error) {
	if c.Contains(SqlFieldAWS) {
		return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
	}
	return
}

var BuildAwsDsn = func(dsn, driver string) (password string, err error) {
	return dsn, nil
}
