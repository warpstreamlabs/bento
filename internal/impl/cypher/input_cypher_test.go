package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func createCypherInputFromYaml(template string) (s input.Streamed, err error) {
	conf, err := testutil.InputFromYAML(template)
	if err != nil {
		return nil, err
	}

	s, err = mock.NewManager().NewInput(conf)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func TestIntegrationCypherInput(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	t.Run("cypher_input_no_auth", func(t *testing.T) {
		localURL := setupNeo4j(t, []string{"NEO4J_AUTH=none"})
		createNodes(localURL, false)
		testCypherInputNoAuth(t, localURL)
	})

	t.Run("cypher_input_basic_auth", func(t *testing.T) {
		localURL := setupNeo4j(t, []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"})
		createNodes(localURL, true)
		testCypherInputBasicAuth(t, localURL)
	})
}

func testCypherInputNoAuth(t *testing.T, localURL string) {
	template := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  no_auth: true
  query: |
    MATCH (n) RETURN n
  `, localURL)

	s, err := createCypherInputFromYaml(template)
	require.NoError(t, err)

	names, err := receiveMessages(s)
	require.NoError(t, err)

	assert.Equal(t, []string{"Alice", "Bob", "Carol", "Dan"}, names)
}

func testCypherInputBasicAuth(t *testing.T, localURL string) {
	template := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  basic_auth:
    user: neo4j
    password: sparkling_brazilian_orange_456
  query: |
    MATCH (n) RETURN n
  `, localURL)

	s, err := createCypherInputFromYaml(template)
	require.NoError(t, err)

	names, err := receiveMessages(s)
	require.NoError(t, err)

	assert.Equal(t, []string{"Alice", "Bob", "Carol", "Dan"}, names)
}

func receiveMessages(s input.Streamed) (names []string, err error) {

	var node struct {
		N struct {
			Props struct {
				Name string `json:"name"`
			} `json:"Props"`
		} `json:"n"`
	}

	for msg := range s.TransactionChan() {
		if err := json.Unmarshal(msg.Payload.Get(0).AsBytes(), &node); err != nil {
			return nil, err
		}

		names = append(names, node.N.Props.Name)

		err := msg.Ack(context.Background(), nil)
		if err != nil {
			return nil, err
		}
	}
	return names, nil
}

func createNodes(localURL string, basicAuth bool) {
	ctx := context.Background()

	var driver neo4j.DriverWithContext
	if basicAuth {
		driver, _ = neo4j.NewDriverWithContext(localURL, neo4j.BasicAuth("neo4j", "sparkling_brazilian_orange_456", ""))
	} else {
		driver, _ = neo4j.NewDriverWithContext(localURL, neo4j.NoAuth())
	}

	_, err := neo4j.ExecuteQuery(
		ctx,
		driver,
		`CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'}), (:Person {name: 'Dan'});`,
		map[string]any{},
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"),
	)
	if err != nil {
		panic(err)
	}
}
