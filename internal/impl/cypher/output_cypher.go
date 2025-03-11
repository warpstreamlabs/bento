package cypher

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	cypherDatabase    = "database"
	cypherURI         = "uri"
	cypherNoAuth      = "no_auth"
	cypherBasicAuth   = "basic_auth"
	cypherQuery       = "query"
	cypherValues      = "values"
	cypherMaxInFlight = "max_in_flight"
	cypherBatching    = "batching"

	// Basic Auth
	cypherUser     = "user"
	cypherPassword = "password"
	cypherRealm    = "realm"

	cypherTLS = "tls"
)

var cypherOutputDescription string = `
## Executes a Cypher Query

The ` + "`" + `query` + "`" + ` field is expected to be a valid cypher query with 0 or more parameters with the ` + "`" + `$` + "`" + ` syntax:
` + "```" + `
    query: CREATE (p:Person {name: $name, age: $age}) RETURN p
` + "```" + `

The ` + "`" + `values` + "`" + ` field is expected to be a map where the keys are equal to the parameters in the query,
and the values are strings (bloblang interpolations are allowed): 

` + "```" + `
    values: 
      name: ${! json("name") }
      age: ${! json("age") }
` + "```"

func cypherOutputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Version("1.4.0").
		Categories("Services").
		Summary("Executes a Cypher Query").
		Description(cypherOutputDescription).
		Fields(
			service.NewStringField(cypherDatabase).
				Description("The name of the database to connect to.").
				Example("neo4j"),
			service.NewStringField(cypherURI).
				Description("The URL of the database engine.").
				Example("bolt://localhost:7687"),
			service.NewBoolField(cypherNoAuth).
				Description("Set to true to connect without authentication.").
				Default(false),
			service.NewObjectField(cypherBasicAuth, basicAuthSpec()...).
				Description("Basic Authentication fields"),
			service.NewStringField(cypherQuery).
				Description("The cypher query to execute.").
				Example("CREATE (p:Person {name: $name}) RETURN p"),
			service.NewInterpolatedStringMapField(cypherValues).
				Description("A map of strings -> bloblang interpolations that form the values of the references in the query i.e. $name.").
				Default(map[string]any{}).
				Example(map[string]any{
					"name": `${! json("name") }`,
				}),
			service.NewIntField(cypherMaxInFlight).
				Description("The maximum number of queries to run in parallel.").
				Default(64),
			service.NewTLSToggledField(cypherTLS),
		)

	spec = spec.Field(service.NewBatchPolicyField(cypherBatching)).
		Example("Create Node",
			`
Here we execute a cypher query that takes the value of $name from the interpolated field in the values map:`,
			`
output:
  cypher:
    database: neo4j
    uri: bolt://localhost:7687
    basic_auth:
      user: neo4j
      password: password
    query: |
      CREATE (p:Person {name: $name}) RETURN p
    values: 
      name: ${! json("name") }
    batching:
      count: 0
`,
		)
	return spec
}

func basicAuthSpec() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(cypherUser).
			Default("").
			Description("The username for basic auth."),
		service.NewStringField(cypherPassword).
			Default("").
			Secret().
			Description("The password for basic auth."),
		service.NewStringField(cypherRealm).
			Default("").
			Description("The realm for basic auth."),
	}
}

func init() {
	err := service.RegisterBatchOutput(
		"cypher", cypherOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = NewCypherOutputFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//----------------------------------------------------------------------------

type CypherOutput struct {
	database  string
	uri       string
	noAuth    bool
	basicAuth CypherBasicAuth
	tlsConfig *tls.Config
	query     string
	values    map[string]*service.InterpolatedString
	driver    neo4j.DriverWithContext
	session   neo4j.SessionWithContext
}

type CypherBasicAuth struct {
	user     string
	password string
	realm    string
}

func NewCypherOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*CypherOutput, error) {
	database, err := conf.FieldString(cypherDatabase)
	if err != nil {
		return nil, err
	}
	uri, err := conf.FieldString(cypherURI)
	if err != nil {
		return nil, err
	}
	noAuth, err := conf.FieldBool(cypherNoAuth)
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldString(cypherQuery)
	if err != nil {
		return nil, err
	}
	values, err := conf.FieldInterpolatedStringMap(cypherValues)
	if err != nil {
		return nil, err
	}

	err = validateQueryAndValues(query, values)
	if err != nil {
		return nil, err
	}

	var tlsEnabled bool
	var tlsConfig *tls.Config
	if tlsConfig, tlsEnabled, err = conf.FieldTLSToggled(cypherTLS); err != nil {
		return nil, err
	}

	if !noAuth {

		basicAuthMap, err := conf.FieldStringMap(cypherBasicAuth)
		if err != nil {
			return nil, err
		}

		basicAuth := CypherBasicAuth{
			user:     basicAuthMap[cypherUser],
			password: basicAuthMap[cypherPassword],
			realm:    basicAuthMap[cypherRealm],
		}

		if tlsEnabled {
			return &CypherOutput{
				database:  database,
				uri:       uri,
				noAuth:    noAuth,
				basicAuth: basicAuth,
				tlsConfig: tlsConfig,
				query:     query,
				values:    values}, nil
		}

		return &CypherOutput{database: database, uri: uri, noAuth: noAuth, basicAuth: basicAuth, query: query, values: values}, nil
	}

	return &CypherOutput{database: database, uri: uri, noAuth: noAuth, query: query, values: values}, nil
}

func validateQueryAndValues(query string, values map[string]*service.InterpolatedString) error {

	for k := range values {
		if !strings.Contains(query, "$"+k) {
			return fmt.Errorf("value key: $%s, not found in query: %s", k, query)
		}
	}

	re := regexp.MustCompile(`\$\b[a-zA-Z][a-zA-Z0-9]*\b`)
	extractedVariables := re.FindAllString(query, -1)

	for _, param := range extractedVariables {
		if _, ok := values[param[1:]]; !ok {
			return fmt.Errorf("query parameter: %s, not found in value keys", param)
		}
	}

	return nil
}

func (cyp *CypherOutput) Connect(ctx context.Context) error {

	var driver neo4j.DriverWithContext
	var err error

	var connConfigurers []func(*config.Config)
	if cyp.tlsConfig != nil {
		connConfigurers = append(connConfigurers, func(config *config.Config) {
			config.TlsConfig = cyp.tlsConfig
		})
	}

	if cyp.noAuth {
		driver, err = neo4j.NewDriverWithContext(cyp.uri, neo4j.NoAuth())
	} else {
		driver, err = neo4j.NewDriverWithContext(cyp.uri, neo4j.BasicAuth(cyp.basicAuth.user, cyp.basicAuth.password, cyp.basicAuth.realm), connConfigurers...)
	}

	if err != nil {
		return err
	}

	cyp.driver = driver

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	cyp.session = session

	return nil
}

func (cyp *CypherOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {

	values := make([]map[string]any, len(batch))

	for i, msgPart := range batch {

		values[i] = make(map[string]any)

		for k, v := range cyp.values {
			values[i][k] = v.String(msgPart)
		}

		_, err := neo4j.ExecuteQuery(
			ctx,
			cyp.driver,
			cyp.query,
			values[i],
			neo4j.EagerResultTransformer,
			neo4j.ExecuteQueryWithDatabase(cyp.database),
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (cyp *CypherOutput) Close(ctx context.Context) error {
	cyp.driver.Close(ctx)
	cyp.session.Close(ctx)
	return nil
}
