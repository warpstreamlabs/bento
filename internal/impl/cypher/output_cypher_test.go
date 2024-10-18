package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func setupNeo4j(t *testing.T, env []string) string {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 60

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "neo4j",
		Tag:        "latest",
		Env:        env,
	})
	require.NoError(t, err)

	neo4jDockerAddress := fmt.Sprintf("bolt://localhost:%s", resource.GetPort("7687/tcp"))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	return neo4jDockerAddress
}

func createCypherOutputFromYaml(template string) (s output.Streamed, err error) {

	conf, err := testutil.OutputFromYAML(template)
	if err != nil {
		return nil, err
	}

	s, err = mock.NewManager().NewOutput(conf)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func sendMessages(t *testing.T, s output.Streamed) {
	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err := s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		s.TriggerCloseNow()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		assert.NoError(t, s.WaitForClose(ctx))
		done()
	})

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res != nil {
				t.Fatal(res)
			}
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}
	}
}

func sendBatches(t *testing.T, s output.Streamed) {
	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err := s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		s.TriggerCloseNow()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		assert.NoError(t, s.WaitForClose(ctx))
		done()
	})

	inputs := [][]byte{
		[]byte(`{"name":"Alice"}`), []byte(`{"name":"Bob"}`),
		[]byte(`{"name":"Carol"}`), []byte(`{"name":"Dan"}`),
	}

	for i := 0; i < len(inputs); i += 2 {
		testMsg := message.QuickBatch([][]byte{inputs[i], inputs[i+1]})
		select {
		case sendChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res != nil {
				t.Fatal(res)
			}
		case <-time.After(time.Second * 60):
			t.Fatal("Action timed out")
		}
	}
}

func checkNeo4j(localURL string, basicAuth bool) (listOfNodeNames []string) {

	ctx := context.Background()

	var driver neo4j.DriverWithContext
	if basicAuth {
		driver, _ = neo4j.NewDriverWithContext(localURL, neo4j.BasicAuth("neo4j", "sparkling_brazilian_orange_456", ""))
	} else {
		driver, _ = neo4j.NewDriverWithContext(localURL, neo4j.NoAuth())
	}

	results, _ := neo4j.ExecuteQuery(ctx, driver,
		"match (n) return n",
		map[string]any{},
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"),
	)

	var listOfNamesFromDB []string

	for _, record := range results.Records {
		recordMap := record.AsMap()
		node := recordMap["n"].(dbtype.Node)
		listOfNamesFromDB = append(listOfNamesFromDB, node.Props["name"].(string))
	}

	return listOfNamesFromDB
}

func TestIntegrationCypherOutput(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	t.Run("cypher_output_no_auth", func(t *testing.T) {
		localURL := setupNeo4j(t, []string{"NEO4J_AUTH=none"})
		testCypherOutputNoAuth(t, localURL)
	})

	t.Run("cypher_output_basic_auth", func(t *testing.T) {
		localURL := setupNeo4j(t, []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"})
		testCypherOutputBasicAuth(t, localURL)
	})

	t.Run("cypher_output_with_batching", func(t *testing.T) {
		localURL := setupNeo4j(t, []string{"NEO4J_AUTH=none"})
		testCypherOutputWithBatching(t, localURL)
	})
}

func testCypherOutputNoAuth(t *testing.T, localURL string) {
	template := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  no_auth: true
  query: |
    CREATE (p:Person {name: $name}) RETURN p
  values:
    name: ${! json("name") }
  batching:
    count: 0
  `, localURL)

	s, err := createCypherOutputFromYaml(template)
	require.NoError(t, err)

	sendMessages(t, s)

	// check output
	listOfNamesFromDB := checkNeo4j(localURL, false)
	listOfNamesToCheck := []string{"Alice", "Bob", "Carol", "Dan"}

	assert.Equal(t, listOfNamesToCheck, listOfNamesFromDB)
}

func testCypherOutputBasicAuth(t *testing.T, localURL string) {
	template := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  basic_auth:
    user: neo4j
    password: sparkling_brazilian_orange_456
  query: |
    CREATE (p:Person {name: $name}) RETURN p
  values: 
    name: ${! json("name") }
  batching:
    count: 0
  `, localURL)

	s, err := createCypherOutputFromYaml(template)
	require.NoError(t, err)

	sendMessages(t, s)

	// check output
	listOfNamesFromDB := checkNeo4j(localURL, true)
	listOfNamesToCheck := []string{"Alice", "Bob", "Carol", "Dan"}

	assert.Equal(t, listOfNamesToCheck, listOfNamesFromDB)
}

func testCypherOutputWithBatching(t *testing.T, localURL string) {
	template := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  no_auth: true
  query: |
    CREATE (p:Person {name: $name}) RETURN p
  values:
    name: ${! json("name") }
  batching:
    count: 2
  `, localURL)

	s, err := createCypherOutputFromYaml(template)
	require.NoError(t, err)

	sendBatches(t, s)

	// check output
	listOfNamesFromDB := checkNeo4j(localURL, false)
	listOfNamesToCheck := []string{"Alice", "Bob", "Carol", "Dan"}

	assert.Equal(t, listOfNamesToCheck, listOfNamesFromDB)
}

func TestCypherOutputMissingValue(t *testing.T) {
	env := []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"}
	localURL := setupNeo4j(t, env)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  basic_auth:
    user: neo4j
    password: sparkling_brazilian_orange_456
  query: |
    CREATE (p:Person {name: $name, age: $age}) RETURN p
  values: 
    name: ${! json("name") }
  batching:
    count: 0
`, localURL)

	conf, err := testutil.OutputFromYAML(confStr)

	require.NoError(t, err)

	_, err = mock.NewManager().NewOutput(conf)

	require.ErrorContains(t, err, "failed to init output <no label>: query parameter: $age, not found in value keys")
}

func TestCypherOutputMissingParam(t *testing.T) {
	env := []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"}
	localURL := setupNeo4j(t, env)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  uri: %s
  basic_auth:
    user: neo4j
    password: sparkling_brazilian_orange_456
  query: |
    CREATE (p:Person {name: $name}) RETURN p
  values: 
    name: ${! json("name") }
    age: ${! json("name") }
  batching:
    count: 0
`, localURL)

	conf, err := testutil.OutputFromYAML(confStr)

	require.NoError(t, err)

	_, err = mock.NewManager().NewOutput(conf)

	require.ErrorContains(t, err, "failed to init output <no label>: value key: $age, not found in query: CREATE (p:Person {name: $name}) RETURN p")
}
