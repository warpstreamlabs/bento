package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/public/service"
)

type DSNBuilder func(dsn, driver string) (builtDSN string, err error)

var driverField = service.NewStringEnumField("driver", "mysql", "postgres", "clickhouse", "mssql", "sqlite", "oracle", "snowflake", "trino", "gocosmos", "spanner").
	Description("A database [driver](#drivers) to use.")

var dsnField = service.NewStringField("dsn").
	Description(`A Data Source Name to identify the target database.

#### Drivers

The following is a list of supported drivers, their placeholder style, and their respective DSN formats:

| Driver | Data Source Name Format |
|---|---|
` + "| `clickhouse` | [`clickhouse://[username[:password]@][netloc][:port]/dbname[?param1=value1&...&paramN=valueN]`](https://github.com/ClickHouse/clickhouse-go#dsn) |" + `
` + "| `mysql` | `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]` |" + `
` + "| `postgres` | `postgres://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]` |" + `
` + "| `mssql` | `sqlserver://[user[:password]@][netloc][:port][?database=dbname&param1=value1&...]` |" + `
` + "| `sqlite` | `file:/path/to/filename.db[?param&=value1&...]` |" + `
` + "| `oracle` | `oracle://[username[:password]@][netloc][:port]/service_name?server=server2&server=server3` |" + `
` + "| `snowflake` | `username[:password]@account_identifier/dbname/schemaname[?param1=value&...&paramN=valueN]` |" + `
` + "| `spanner` | `projects/[project]/instances/[instance]/databases/dbname` |" + `
` + "| `trino` | [`http[s]://user[:pass]@host[:port][?parameters]`](https://github.com/trinodb/trino-go-client#dsn-data-source-name) |" + `
` + "| `gocosmos` | [`AccountEndpoint=<cosmosdb-endpoint>;AccountKey=<cosmosdb-account-key>[;TimeoutMs=<timeout-in-ms>][;Version=<cosmosdb-api-version>][;DefaultDb/Db=<db-name>][;AutoId=<true/false>][;InsecureSkipVerify=<true/false>]`](https://pkg.go.dev/github.com/microsoft/gocosmos#readme-example-usage) |" + `

Please note that the ` + "`postgres`" + ` driver enforces SSL by default, you can override this with the parameter ` + "`sslmode=disable`" + ` if required.

The ` + "`snowflake`" + ` driver supports multiple DSN formats. Please consult [the docs](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_String) for more details. For [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication), the DSN has the following format: ` + "`<snowflake_user>@<snowflake_account>/<db_name>/<schema_name>?warehouse=<warehouse>&role=<role>&authenticator=snowflake_jwt&privateKey=<base64_url_encoded_private_key>`" + `, where the value for the ` + "`privateKey`" + ` parameter can be constructed from an unencrypted RSA private key file ` + "`rsa_key.p8`" + ` using ` + "`openssl enc -d -base64 -in rsa_key.p8 | basenc --base64url -w0`" + ` (you can use ` + "`gbasenc`" + ` insted of ` + "`basenc`" + ` on OSX if you install ` + "`coreutils`" + ` via Homebrew). If you have a password-encrypted private key, you can decrypt it using ` + "`openssl pkcs8 -in rsa_key_encrypted.p8 -out rsa_key.p8`" + `. Also, make sure fields such as the username are URL-encoded.

The ` + "[`gocosmos`](https://pkg.go.dev/github.com/microsoft/gocosmos)" + ` driver is still experimental, but it has support for [hierarchical partition keys](https://learn.microsoft.com/en-us/azure/cosmos-db/hierarchical-partition-keys) as well as [cross-partition queries](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-query-container#cross-partition-query). Please refer to the [SQL notes](https://github.com/microsoft/gocosmos/blob/main/SQL.md) for details.`).
	Example("clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60").
	Example("foouser:foopassword@tcp(localhost:3306)/foodb").
	Example("postgres://foouser:foopass@localhost:5432/foodb?sslmode=disable").
	Example("oracle://foouser:foopass@localhost:1521/service_name")

func connFields() []*service.ConfigField {

	connFields := []*service.ConfigField{
		service.NewStringListField("init_files").
			Description(`
An optional list of file paths containing SQL statements to execute immediately upon the first connection to the target database. This is a useful way to initialise tables before processing data. Glob patterns are supported, including super globs (double star).

Care should be taken to ensure that the statements are idempotent, and therefore would not cause issues when run multiple times after service restarts. If both ` + "`init_statement` and `init_files` are specified the `init_statement` is executed _after_ the `init_files`." + `

If a statement fails for any reason a warning log will be emitted but the operation of this component will not be stopped.
`).
			Example([]any{`./init/*.sql`}).
			Example([]any{`./foo.sql`, `./bar.sql`}).
			Optional().
			Advanced().
			Version("1.0.0"),
		service.NewStringField("init_statement").
			Description(`
An optional SQL statement to execute immediately upon the first connection to the target database. This is a useful way to initialise tables before processing data. Care should be taken to ensure that the statement is idempotent, and therefore would not cause issues when run multiple times after service restarts.

If both ` + "`init_statement` and `init_files` are specified the `init_statement` is executed _after_ the `init_files`." + `

If the statement fails for any reason a warning log will be emitted but the operation of this component will not be stopped.
`).
			Example(`
CREATE TABLE IF NOT EXISTS some_table (
  foo varchar(50) not null,
  bar integer,
  baz varchar(50),
  primary key (foo)
) WITHOUT ROWID;
`).
			Optional().
			Advanced().
			Version("1.0.0"),
		service.NewBoolField("init_verify_conn").
			Description("Whether to verify the database connection on startup by performing a simple ping, by default this is disabled.").
			Default(false).
			Optional().
			Advanced().
			Version("1.2.0"),
		service.NewDurationField("conn_max_idle_time").
			Description("An optional maximum amount of time a connection may be idle. Expired connections may be closed lazily before reuse. If `value <= 0`, connections are not closed due to a connections idle time.").
			Optional().
			Advanced(),
		service.NewDurationField("conn_max_life_time").
			Description("An optional maximum amount of time a connection may be reused. Expired connections may be closed lazily before reuse. If `value <= 0`, connections are not closed due to a connections age.").
			Optional().
			Advanced(),
		service.NewIntField("conn_max_idle").
			Description("An optional maximum number of connections in the idle connection pool. If conn_max_open is greater than 0 but less than the new conn_max_idle, then the new conn_max_idle will be reduced to match the conn_max_open limit. If `value <= 0`, no idle connections are retained. The default max idle connections is currently 2. This may change in a future release.").
			Default(2).
			Optional().
			Advanced(),
		service.NewIntField("conn_max_open").
			Description("An optional maximum number of open connections to the database. If conn_max_idle is greater than 0 and the new conn_max_open is less than conn_max_idle, then conn_max_idle will be reduced to match the new conn_max_open limit. If `value <= 0`, then there is no limit on the number of open connections. The default is 0 (unlimited).").
			Optional().
			Advanced(),
		service.NewStringField("secret_name").
			Description("An optional field that can be used to get the Username + Password from AWS Secrets Manager. This will overwrite the Username + Password in the DSN with the values from the Secret only if the driver is set to `postgres`.").
			Optional().
			Version("1.3.0").
			Advanced(),
		service.NewBoolField("iam_enabled").
			Description("An optional field used to generate an IAM authentication token to connect to an Amazon Relational Database (RDS) DB instance. This will overwrite the Password in the DSN with the generated token only if the drivers are `mysql` or `postgres`.").
			Optional().
			Default(false).
			Version("1.8.0").
			Advanced(),
		service.NewObjectField("azure",
			service.NewBoolField("entra_enabled").
				Description("An optional field used to generate an entra token to connect to 'Azure Database for PostgreSQL flexible server', This will create a new connection string with the host, user and database from the DSN field - you may need to URL encode the dsn! The [Default Azure Credential Chain](https://learn.microsoft.com/en-gb/azure/developer/go/sdk/authentication/authentication-overview#defaultazurecredential) is used from the Azure SDK.").
				Optional().
				Default(false).
				Version("1.10.0").
				Advanced(),
			service.NewObjectField("token_request_options",
				service.NewStringField("claims").
					Description("Set additional claims for the token.").
					Optional().
					Default("").
					Version("1.10.0").
					Advanced(),
				service.NewBoolField("enable_cae").
					Description("Indicates whether to enable Continuous Access Evaluation (CAE) for the requested token").
					Optional().
					Default(false).
					Version("1.10.0").
					Advanced(),
				service.NewStringListField("scopes").
					Description("Scopes contains the list of permission scopes required for the token.").
					Optional().
					Default([]string{"https://ossrdbms-aad.database.windows.net/.default"}).
					Version("1.10.0").
					Advanced(),
				service.NewStringField("tenant_id").
					Description("tenant_id identifies the tenant from which to request the token. azure credentials authenticate in their configured default tenants when this field isn't set.").
					Optional().
					Default("").
					Version("1.10.0").
					Advanced(),
			)).
			Description("Optional Fields that can be set to use Azure based authentication for Azure Postgres SQL").
			Optional().
			Version("1.10.0").
			Advanced(),
	}

	connFields = append(connFields, config.SessionFields()...)

	return connFields

}

func rawQueryField() *service.ConfigField {
	return service.NewStringField("query").
		Description("The query to execute. The style of placeholder to use depends on the driver, some drivers require question marks (`?`) whereas others expect incrementing dollar signs (`$1`, `$2`, and so on) or colons (`:1`, `:2` and so on). The style to use is outlined in this table:" + `

| Driver | Placeholder Style |
|---|---|
` + "| `clickhouse` | Dollar sign |" + `
` + "| `mysql` | Question mark |" + `
` + "| `postgres` | Dollar sign |" + `
` + "| `mssql` | Question mark |" + `
` + "| `sqlite` | Question mark |" + `
` + "| `oracle` | Colon |" + `
` + "| `snowflake` | Question mark |" + `
` + "| `spanner` | Question mark |" + `
` + "| `trino` | Question mark |" + `
` + "| `gocosmos` | Colon |" + `
`)
}

type connSettings struct {
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
	maxIdleConns    int
	maxOpenConns    int

	initOnce           sync.Once
	initFileStatements [][2]string // (path,statement)
	initStatement      string
	initVerifyConn     bool

	getCredentials func(string, string) (string, string, error)
}

func (c *connSettings) apply(ctx context.Context, db *sql.DB, log *service.Logger) {
	db.SetConnMaxIdleTime(c.connMaxIdleTime)
	db.SetConnMaxLifetime(c.connMaxLifetime)
	db.SetMaxIdleConns(c.maxIdleConns)
	db.SetMaxOpenConns(c.maxOpenConns)

	c.initOnce.Do(func() {
		for _, fileStmt := range c.initFileStatements {
			if _, err := db.ExecContext(ctx, fileStmt[1]); err != nil {
				log.Warnf("Failed to execute init_file '%v': %v", fileStmt[0], err)
			} else {
				log.Debugf("Successfully ran init_file '%v'", fileStmt[0])
			}
		}
		if c.initStatement != "" {
			if _, err := db.ExecContext(ctx, c.initStatement); err != nil {
				log.Warnf("Failed to execute init_statement: %v", err)
			} else {
				log.Debug("Successfully ran init_statement")
			}
		}

	})
}

func getDsnBuilder(
	conf *service.ParsedConfig,
) (func(dsn, driver string) (builtDSN, provider string, err error), error) {
	awsEnabled, err := IsAWSEnabled(conf)
	if err != nil {
		return nil, err
	}

	azureEnabled, err := IsAzureEnabled(conf)
	if err != nil {
		return nil, err
	}

	switch {
	case awsEnabled && azureEnabled:
		return nil, errors.New("cannot enable both AWS and Azure authentication")
	case awsEnabled:
		builder, err := AWSGetCredentialsGeneratorFn(conf)
		if err != nil {
			return nil, err
		}
		return func(dsn, driver string) (builtDSN, provider string, err error) {
			result, err := builder(dsn, driver)
			return result, "AWS", err
		}, nil
	case azureEnabled:
		builder, err := AzureGetCredentialsGeneratorFn(conf)
		if err != nil {
			return nil, err
		}
		return func(dsn, driver string) (builtDSN, provider string, err error) {
			result, err := builder(dsn, driver)
			return result, "Azure", err
		}, nil
	default:
		return func(dsn, driver string) (builtDSN, provider string, err error) {
			return dsn, "", nil
		}, nil
	}
}

func connSettingsFromParsed(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (c *connSettings, err error) {
	c = &connSettings{}

	if conf.Contains("conn_max_life_time") {
		if c.connMaxLifetime, err = conf.FieldDuration("conn_max_life_time"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_idle_time") {
		if c.connMaxIdleTime, err = conf.FieldDuration("conn_max_idle_time"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_idle") {
		if c.maxIdleConns, err = conf.FieldInt("conn_max_idle"); err != nil {
			return
		}
	}

	if conf.Contains("conn_max_open") {
		if c.maxOpenConns, err = conf.FieldInt("conn_max_open"); err != nil {
			return
		}
	}

	if conf.Contains("init_statement") {
		if c.initStatement, err = conf.FieldString("init_statement"); err != nil {
			return
		}
	}

	if conf.Contains("init_files") {
		var tmpFiles []string
		if tmpFiles, err = conf.FieldStringList("init_files"); err != nil {
			return
		}
		if tmpFiles, err = service.Globs(mgr.FS(), tmpFiles...); err != nil {
			err = fmt.Errorf("failed to expand init_files glob patterns: %w", err)
			return
		}
		for _, p := range tmpFiles {
			var statementBytes []byte
			if statementBytes, err = service.ReadFile(mgr.FS(), p); err != nil {
				return
			}
			c.initFileStatements = append(c.initFileStatements, [2]string{
				p, string(statementBytes),
			})
		}
	}

	if conf.Contains("init_verify_conn") {
		if c.initVerifyConn, err = conf.FieldBool("init_verify_conn"); err != nil {
			return
		}
	}

	if c.getCredentials, err = getDsnBuilder(conf); err != nil {
		return
	}

	return
}

func reworkDSN(driver, dsn string) (string, error) {
	if driver == "clickhouse" && strings.HasPrefix(dsn, "tcp") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}

		u.Scheme = "clickhouse"

		uq := u.Query()
		u.Path = uq.Get("database")
		if username, password := uq.Get("username"), uq.Get("password"); username != "" {
			if password != "" {
				u.User = url.User(username)
			} else {
				u.User = url.UserPassword(username, password)
			}
		}

		uq.Del("database")
		uq.Del("username")
		uq.Del("password")

		u.RawQuery = uq.Encode()
		newDSN := u.String()

		return newDSN, nil
	}

	return dsn, nil
}

func sqlOpenWithReworks(ctx context.Context, logger *service.Logger, driver, dsn string, connSettings *connSettings) (*sql.DB, error) {
	updatedDSN, err := reworkDSN(driver, dsn)
	if err != nil {
		return nil, err
	}

	if updatedDSN != dsn {
		logger.Warnf("Detected old-style Clickhouse Data Source Name: '%v', replacing with new style: '%v'", dsn, updatedDSN)
	}

	updatedDSN, provider, err := connSettings.getCredentials(updatedDSN, driver)
	if err != nil {
		return nil, err
	}

	if provider != "" {
		logger.Infof("Updated DSN with info from %s", provider)
	}

	db, err := sql.Open(driver, updatedDSN)
	if err != nil {
		return nil, err
	}

	if connSettings.initVerifyConn {
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("could not establish connection to database: %w", err)
		}
	}

	return db, nil
}
