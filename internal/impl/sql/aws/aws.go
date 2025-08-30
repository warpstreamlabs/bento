package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	baws "github.com/warpstreamlabs/bento/internal/impl/aws"
	"github.com/warpstreamlabs/bento/internal/impl/sql"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	noop := func(dsn, driver string) (password string, err error) {
		return "", nil
	}

	fn := func(conf *service.ParsedConfig) (sql.DSNBuilder, error) {

		awsEnabled, err := sql.IsAWSEnabled(conf)
		if err != nil {
			return nil, err
		}

		if !awsEnabled {
			return noop, nil
		}

		awsConfig, err := baws.GetSession(context.TODO(), conf)
		if err != nil {
			return nil, err
		}

		iamEnabled, err := conf.FieldBool("iam_enabled")
		if err != nil {
			return nil, err
		}

		if iamEnabled {

			getCredentials := func(dbEndpoint string, dbUser string) (string, error) {
				return buildIamAuthToken(dbEndpoint, dbUser, awsConfig)
			}

			wrapDsnBuilder := func(dsn, driver string) (string, error) {
				return BuildAWSDsnFromIAMCredentials(dsn, driver, getCredentials)
			}

			return wrapDsnBuilder, nil

		}

		if conf.Contains("secret_name") {
			secret, err := conf.FieldString("secret_name")
			if err != nil {
				return nil, err
			}

			if secret != "" {
				getCredentials := func() (string, error) {
					return getSecretFromAWSSecretManager(secret, awsConfig)
				}

				wrapDsnBuilder := func(dsn, driver string) (string, error) {
					return BuildAWSDsnFromSecret(dsn, driver, getCredentials)
				}

				return wrapDsnBuilder, nil
			}

		}

		return noop, nil
	}

	sql.SetAWSGetCredentialsGeneratorFn(fn)
}

//------------------------------------------------------------------------------

func BuildAWSDsnFromSecret(dsn, driver string, getAWSCredentialsFromSecret func() (string, error)) (string, error) {
	if driver != "postgres" {
		return "", errors.New("secret_name with DSN info currently only works for postgres DSNs")
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("error parsing DSN URL: %w", err)
	}

	username := parsedDSN.User.Username()
	password, _ := parsedDSN.User.Password()
	host := parsedDSN.Hostname()
	port := parsedDSN.Port()
	path := parsedDSN.Path
	rawQuery := parsedDSN.RawQuery

	secretString, err := getAWSCredentialsFromSecret()
	if err != nil {
		return "", fmt.Errorf("error retrieving secret: %w", err)
	}

	var secrets map[string]interface{}
	if err := json.Unmarshal([]byte(secretString), &secrets); err != nil {
		return "", fmt.Errorf("error unmarshalling secret: %w", err)
	}

	if val, ok := secrets["username"].(string); ok && val != "" {
		username = val
	}
	if val, ok := secrets["password"].(string); ok && val != "" {
		password = val
	}

	newDSN := fmt.Sprintf("%s://%s:%s@%s:%s%s", driver, url.QueryEscape(username), url.QueryEscape(password), host, port, path)
	if rawQuery != "" {
		newDSN = fmt.Sprintf("%s?%s", newDSN, rawQuery)
	}

	return newDSN, nil
}

func BuildAWSDsnFromIAMCredentials(dsn string, driver string, generateIAMAuthToken func(dbEndpoint string, dbUser string) (string, error)) (string, error) {
	if driver != "postgres" && driver != "mysql" {
		return "", errors.New("cannot create DSN from IAM when driver is not postgres or mysql")
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("error parsing DSN URL: %w", err)
	}

	username := parsedDSN.User.Username()
	host := parsedDSN.Hostname()
	port := parsedDSN.Port()
	path := parsedDSN.Path
	rawQuery := parsedDSN.RawQuery
	endpoint := fmt.Sprintf("%s:%s", host, port)
	iamToken, err := generateIAMAuthToken(endpoint, username)
	if err != nil {
		return "", fmt.Errorf("error retrieving IAM token: %w", err)
	}

	newDSN := fmt.Sprintf("%s://%s:%s@%s:%s%s", driver, url.QueryEscape(username), url.QueryEscape(iamToken), host, port, path)
	if rawQuery != "" {
		newDSN = fmt.Sprintf("%s?%s", newDSN, rawQuery)
	}

	return newDSN, nil

}

//------------------------------------------------------------------------------

func getSecretFromAWSSecretManager(secretName string, awsConf aws.Config) (string, error) {
	svc := secretsmanager.NewFromConfig(awsConf)

	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}
	result, err := svc.GetSecretValue(context.TODO(), input)
	if err != nil {
		return "", err
	}

	return *result.SecretString, nil
}

func buildIamAuthToken(dbEndpoint, dbUser string, awsConf aws.Config) (string, error) {
	authenticationToken, err := auth.BuildAuthToken(
		context.TODO(), dbEndpoint, awsConf.Region, dbUser, awsConf.Credentials,
	)
	if err != nil {
		return "", err
	}

	return authenticationToken, nil
}
