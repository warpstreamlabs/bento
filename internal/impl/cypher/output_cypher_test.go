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
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func setupNeo4j(env []string) (string, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return "", err
	}
	pool.MaxWait = time.Second * 60

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "neo4j",
		Tag:        "latest",
		Env:        env,
	})
	if err != nil {
		return "", err
	}

	neo4jDockerAddress := fmt.Sprintf("bolt://localhost:%s", resource.GetPort("7687/tcp"))
	_ = resource.Expire(900)

	return neo4jDockerAddress, nil
}

func TestCypherOutputNoAuth(t *testing.T) {
	env := []string{"NEO4J_AUTH=none"}
	localURL, err := setupNeo4j(env)

	require.NoError(t, err)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  url: %s
  no_auth: true
  query: |
    CREATE (p:Person {name: $name}) RETURN p
  values:
    name: ${! json("name") }
  batching:
    count: 0
`, localURL)

	conf, err := testutil.OutputFromYAML(confStr)
	require.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)

	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = s.Consume(sendChan); err != nil {
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

	ctx := context.Background()
	driver, _ := neo4j.NewDriverWithContext(localURL, neo4j.NoAuth())

	results, _ := neo4j.ExecuteQuery(ctx, driver,
		"match (n) return n",
		map[string]any{},
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"),
	)

	var listOfNamesFromDB []string
	listOfNamesToCheck := []string{"Alice", "Bob", "Carol", "Dan"}

	for _, record := range results.Records {
		dictionary := record.AsMap()
		x := dictionary["n"].(dbtype.Node)
		listOfNamesFromDB = append(listOfNamesFromDB, x.Props["name"].(string))
	}

	assert.Equal(t, listOfNamesFromDB, listOfNamesToCheck)
}

func TestCypherOutputBasicAuth(t *testing.T) {
	env := []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"}
	localURL, err := setupNeo4j(env)

	require.NoError(t, err)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  url: %s
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

	conf, err := testutil.OutputFromYAML(confStr)
	require.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)

	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = s.Consume(sendChan); err != nil {
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

	ctx := context.Background()
	driver, _ := neo4j.NewDriverWithContext(localURL, neo4j.BasicAuth("neo4j", "sparkling_brazilian_orange_456", ""))

	results, _ := neo4j.ExecuteQuery(ctx, driver,
		"match (n) return n",
		map[string]any{},
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"),
	)

	var listOfNamesFromDB []string
	listOfNamesToCheck := []string{"Alice", "Bob", "Carol", "Dan"}

	for _, record := range results.Records {
		dictionary := record.AsMap()
		x := dictionary["n"].(dbtype.Node)
		listOfNamesFromDB = append(listOfNamesFromDB, x.Props["name"].(string))
	}

	assert.Equal(t, listOfNamesFromDB, listOfNamesToCheck)
}

func TestCypherOutputMissingValue(t *testing.T) {
	env := []string{"NEO4J_AUTH=neo4j/sparkling_brazilian_orange_456"}
	localURL, err := setupNeo4j(env)

	require.NoError(t, err)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  url: %s
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
	localURL, err := setupNeo4j(env)

	require.NoError(t, err)

	confStr := fmt.Sprintf(`
cypher:
  database: neo4j
  url: %s
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
