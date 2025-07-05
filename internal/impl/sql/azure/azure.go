package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/warpstreamlabs/bento/internal/impl/sql"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	var noop sql.AzureDSNBuiler = func(dsn, driver string) (builtDSN string, err error) {
		return dsn, nil
	}

	sql.AzureGetCredentialsGeneratorFn = func(pConf *service.ParsedConfig) (sql.AzureDSNBuiler, error) {
		nsConf := pConf.Namespace("azure")
		entraEnabled, err := nsConf.FieldBool("entra_enabled")
		if err != nil {
			return nil, err
		}

		if !entraEnabled {
			return noop, nil
		}

		getTokenOptions := func() (tro policy.TokenRequestOptions, err error) {
			return parseTokenOptions(pConf)
		}

		wrapDsnBuilder := func(dsn, driver string) (string, error) {
			return BuildEntraDsn(dsn, driver, getTokenOptions)
		}

		return wrapDsnBuilder, nil
	}
}

func BuildEntraDsn(dsn, driver string, getTokenOptions func() (policy.TokenRequestOptions, error)) (builtDsn string, err error) {
	if driver != "postgres" {
		return "", errors.New("entra auth currently only works for postgres DSNs")
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("error parsing DSN URLS: %w", err)
	}

	username := parsedDSN.User.Username()
	host := parsedDSN.Hostname()
	path := parsedDSN.Path[1:]

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", err
	}

	tro, err := getTokenOptions()
	if err != nil {
		return "", err
	}

	token, err := cred.GetToken(context.TODO(), tro)
	if err != nil {
		return "", err
	}

	connectionString := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=require", host, username, token.Token, path)

	return connectionString, nil
}

func parseTokenOptions(pConf *service.ParsedConfig) (policy.TokenRequestOptions, error) {
	nsConf := pConf.Namespace("azure", "token_request_options")

	claims, err := nsConf.FieldString("claims")
	if err != nil {
		return policy.TokenRequestOptions{}, err
	}

	enableCAE, err := nsConf.FieldBool("enable_cae")
	if err != nil {
		return policy.TokenRequestOptions{}, err
	}

	scopes, err := nsConf.FieldStringList("scopes")
	if err != nil {
		return policy.TokenRequestOptions{}, err
	}

	tenantID, err := nsConf.FieldString("tenant_id")
	if err != nil {
		return policy.TokenRequestOptions{}, err
	}

	return policy.TokenRequestOptions{
		Claims:    claims,
		EnableCAE: enableCAE,
		Scopes:    scopes,
		TenantID:  tenantID,
	}, nil
}
