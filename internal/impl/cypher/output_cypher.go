package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/warpstreamlabs/bento/public/service"
)

func cypherOutputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Fields(
			service.NewStringField("database").
				Description("The name of the database to connect to.").
				Example("neo4j"),
			service.NewStringField("url").
				Description("The URL of the database engine.").
				Example("bolt://localhost:7687"),
			service.NewBoolField("no_auth").
				Description("Set to true to connect without authentication.").
				Default(false),
			service.NewObjectField("basic_auth", basicAuthSpec()...).
				Description("Basic Authentication fields"),
			service.NewStringField("query").
				Description("The cypher query to execute.").
				Example("CREATE (p:Person {name: $name}) RETURN p"),
			service.NewInterpolatedStringMapField("values").
				Description("A map of strings -> bloblang interpolations that form the values of the references in the query i.e. $name.").
				Default(map[string]any{}).
				Example(map[string]any{
					"name": `${! json("name") }`,
				}),
			service.NewIntField("max_in_flight").
				Description("The maximum number of queries to run in parallel.").
				Default(64),
		)

	spec = spec.Field(service.NewBatchPolicyField("batching")).
		Version("1.0.0").
		Example("Create Node",
			`
Here we execute a cypher query that takes the value of $name from the interpolated field in the values map:`,
			`
output:
  cypher:
    database: neo4j
    url: bolt://localhost:7687
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
		service.NewStringField("user").
			Default("").
			Description("The username for basic auth."),
		service.NewStringField("password").
			Default("").
			Secret().
			Description("The password for basic auth."),
		service.NewStringField("realm").
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
	url       string
	noAuth    bool
	basicAuth CypherBasicAuth
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
	database, err := conf.FieldString("database")
	if err != nil {
		return nil, err
	}
	url, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	noAuth, err := conf.FieldBool("no_auth")
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}
	values, err := conf.FieldInterpolatedStringMap("values")
	if err != nil {
		return nil, err
	}

	err = validateQueryAndValues(query, values)
	if err != nil {
		return nil, err
	}

	if !noAuth {
		basicAuthMap, _ := conf.FieldStringMap("basic_auth")
		basicAuth := CypherBasicAuth{user: basicAuthMap["user"], password: basicAuthMap["password"], realm: basicAuthMap["realm"]}
		return &CypherOutput{database: database, url: url, noAuth: noAuth, basicAuth: basicAuth, query: query, values: values}, nil
	}

	return &CypherOutput{database: database, url: url, noAuth: noAuth, query: query, values: values}, nil
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

	if cyp.noAuth {
		driver, err = neo4j.NewDriverWithContext(cyp.url, neo4j.NoAuth())
	} else {
		driver, err = neo4j.NewDriverWithContext(cyp.url, neo4j.BasicAuth(cyp.basicAuth.user, cyp.basicAuth.password, cyp.basicAuth.realm))
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

	values := make(map[string]any)

	for _, msgPart := range batch {

		for k, v := range cyp.values {
			values[k] = v.String(msgPart)
		}

		_, err := neo4j.ExecuteQuery(ctx, cyp.driver,
			cyp.query,
			values,
			neo4j.EagerResultTransformer,
			neo4j.ExecuteQueryWithDatabase(cyp.database),
		)

		if err != nil {
			return err
		}

		values = nil
	}

	return nil
}

func (cyp *CypherOutput) Close(ctx context.Context) error {
	cyp.driver.Close(ctx)
	cyp.session.Close(ctx)
	return nil
}
