package cypher

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/warpstreamlabs/bento/public/service"
)

func cypherOutputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Fields(
			service.NewStringField("database").
				Description("The name of the database to connect to.").
				Example("neo4j"),
			service.NewStringField("uri").
				Description("The URI of the database.").
				Example("bolt://localhost:7687"),
			service.NewBoolField("noAuth").
				Description("No Authentication currently implemented, defaults to true.").
				Default(true),
			service.NewStringField("query").
				Description("The cypher query to execute.").
				Example("CREATE (p:Person {name: $name}) RETURN p"),
			service.NewInterpolatedStringMapField("values").
				Description("A map of strings -> bloblang interpolations that form the values of the references in the query i.e. $name.").
				Default(map[string]any{}).
				Example(map[string]any{
					"name": "Alice",
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
    uri: bolt://localhost:7687
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
	database string
	uri      string
	noAuth   bool
	query    string
	values   map[string]*service.InterpolatedString
	driver   neo4j.DriverWithContext
	session  neo4j.SessionWithContext
}

func NewCypherOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*CypherOutput, error) {
	database, err := conf.FieldString("database")
	if err != nil {
		return nil, err
	}
	uri, err := conf.FieldString("uri")
	if err != nil {
		return nil, err
	}
	noAuth, err := conf.FieldBool("noAuth")
	if err != nil {
		return nil, err
	}
	query, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}
	values, _ := conf.FieldInterpolatedStringMap("values")

	return &CypherOutput{database: database, uri: uri, noAuth: noAuth, query: query, values: values}, nil

}

func (cyp *CypherOutput) Connect(ctx context.Context) error {

	driver, err := neo4j.NewDriverWithContext(cyp.uri, neo4j.NoAuth())
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
			panic(err)
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
