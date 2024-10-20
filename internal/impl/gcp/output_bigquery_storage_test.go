package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/warpstreamlabs/bento/public/service"
)

func gcpBigQueryWriteAPIConfFromYAML(t *testing.T, yamlStr string) bigQueryStorageWriterConfig {
	t.Helper()
	spec := gcpBigQueryWriteAPIConfig()
	parsedConf, err := spec.ParseYAML(yamlStr, nil)
	require.NoError(t, err)

	conf, err := bigQueryStorageWriterConfigFromParsed(parsedConf)
	require.NoError(t, err)

	return conf
}

func TestNewGCPBigQueryStorageOutputSchemaConfigOK(t *testing.T) {
	t.Skip("Passing an explicit table schema is not implemented yet")

	config := gcpBigQueryWriteAPIConfFromYAML(t, `
project: foo
dataset: bar
table: baz
schema:
  - name: foobar
    repeeated: True
    type: BOOLEAN
`)

	schema, err := bigquery.SchemaFromJSON([]byte(`[{"name":"foobar","type":"BOOL","mode":"NULLABLE","description":""}]`))
	require.NoError(t, err)

	require.Equal(t, schema, config.tableSchema)
}

func TestNewGCPBigQueryStorageOutputSchemaRepeatedConfigOK(t *testing.T) {
	t.Skip("Passing an explicit table schema is not implemented yet")

	config := gcpBigQueryWriteAPIConfFromYAML(t, `
project: foo
dataset: bar
table: baz
schema:
  - name: foobar
    type: BOOLEAN
    repeated: True
`)

	schema, err := bigquery.SchemaFromJSON([]byte(`[{"name":"foobar","type":"BOOL","mode":"REPEATED","description":""}]`))
	require.NoError(t, err)

	require.Equal(t, schema, config.tableSchema)
}

func TestNewGCPBigQueryStorageOutputNestedSchemaConfigOK(t *testing.T) {
	t.Skip("Passing an explicit table schema is not implemented yet")

	config := gcpBigQueryWriteAPIConfFromYAML(t, `
project: foo
dataset: bar
table: baz
schema:
  - name: nested_record
    description: "Nested nullable RECORD"
    type: RECORD
    fields:
      - name: record_field_1
        type: STRING
        description: "First nested record field"
      - name: record_field_2
        type: INTEGER
        description: "Second nested record field"
        required: True
`)

	jsonSchema := `[{"name":"nested_record","type":"RECORD","mode":"NULLABLE","description":"Nested nullable RECORD","fields":[{"name":"record_field_1","type":"STRING","mode":"NULLABLE","description":"First nested record field"},{"name":"record_field_2","type":"INTEGER","mode":"REQUIRED","description":"Second nested record field"}]}]`
	schema, err := bigquery.SchemaFromJSON([]byte(jsonSchema))
	require.NoError(t, err)

	require.Equal(t, schema, config.tableSchema)
}

func TestNewGCPBigQueryStorageOutputNestedSchemaConfigErr(t *testing.T) {
	t.Skip("Passing an explicit table schema is not implemented yet")

	spec := gcpBigQueryWriteAPIConfig()
	parsedConf, err := spec.ParseYAML(`
project: foo
dataset: bar
table: baz
schema:
  - name: nested_record
    description: "Nested nullable RECORD"
    type: BOOLEAN
    fields:
      - name: record_field_1
        type: STRING
        description: "First nested record field"
      - name: record_field_2
        type: INTEGER
        description: "Second nested record field"
        required: True
`, nil)
	require.NoError(t, err)

	_, err = bigQueryStorageWriterConfigFromParsed(parsedConf)
	require.Error(t, err)

	parsedConf, err = spec.ParseYAML(`
project: foo
dataset: bar
table: baz
schema:
  - name: nested_record
    description: "Nested nullable RECORD"
    type: RECORD
    fields: []
`, nil)
	require.NoError(t, err)

	_, err = bigQueryStorageWriterConfigFromParsed(parsedConf)
	require.Error(t, err)

	parsedConf, err = spec.ParseYAML(`
project: foo
dataset: bar
table: baz
schema:
  - name: nested_record
    description: "Nested nullable RECORD"
    type: RECORD
    required: true
    repeated: true
`, nil)
	require.NoError(t, err)

	_, err = bigQueryStorageWriterConfigFromParsed(parsedConf)
	require.Error(t, err)
}

func TestGCPBigQueryStorageOutputConnectOk(t *testing.T) {
	emulator := setupBigQueryEmulator(t, "project_meow", "dataset_meow", "table_meow", nil)

	config := gcpBigQueryWriteAPIConfFromYAML(t, fmt.Sprintf(`
project: project_meow
dataset: dataset_meow
table: table_meow
endpoint:
  grpc: %s
  http: %s
`, emulator.grpcEndpoint, emulator.httpEndpoint))

	output, err := newBigQueryStorageOutput(config, nil)
	require.NoError(t, err)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryStorageOutputWriteOK(t *testing.T) {
	// Setup some custom schema
	type GithubRepo struct {
		Type   string `bigquery:"type" json:"type"`
		Public bool   `bigquery:"public" json:"public"`
		Repo   struct {
			ID   bigquery.NullInt64 `bigquery:"id" json:"id"`
			Name string             `bigquery:"name" json:"name"`
			URL  string             `bigquery:"url" json:"url"`
		} `bigquery:"repo" json:"repo"`
	}

	schema, err := bigquery.InferSchema(GithubRepo{})
	require.NoError(t, err)

	sampleJSONData := [][]byte{
		[]byte(`{"type": "foo", "public": true, "repo": {"id": 99, "name": "repo_name_1", "url": "https://one.example.com"}}`),
		[]byte(`{"type": "bar", "public": false, "repo": {"id": 101, "name": "repo_name_2", "url": "https://two.example.com"}}`),
		[]byte(`{"type": "baz", "public": true, "repo": {"id": 456, "name": "repo_name_3", "url": "https://three.example.com"}}`),
		[]byte(`{"type": "wow", "public": false, "repo": {"id": 123, "name": "repo_name_4", "url": "https://four.example.com"}}`),
		[]byte(`{"type": "yay", "public": true, "repo": {"name": "repo_name_5", "url": "https://five.example.com"}}`),
	}

	// Create GCP emulator
	emulator := setupBigQueryEmulator(t, "project_meow", "dataset_meow", "table_meow", &schema)

	config := gcpBigQueryWriteAPIConfFromYAML(t, fmt.Sprintf(`
project: project_meow
dataset: dataset_meow
table: table_meow
endpoint:
  grpc: %s
  http: %s
`, emulator.grpcEndpoint, emulator.httpEndpoint))

	output, err := newBigQueryStorageOutput(config, nil)
	require.NoError(t, err)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())
	require.NoError(t, err)

	batch := make(service.MessageBatch, len(sampleJSONData))
	for i, msg := range sampleJSONData {
		batch[i] = service.NewMessage(msg)
	}
	// Write the batch
	err = output.WriteBatch(context.Background(), batch)
	require.NoError(t, err)

	// TODO: Create some utils to easier test whether values are correct

	// Read and check results
	iter := emulator.client.Dataset("dataset_meow").Table("table_meow").Read(context.Background())
	var rows []GithubRepo
	for {
		var row GithubRepo
		if err := iter.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			t.Fatal(err)
		}

		rows = append(rows, row)
	}

	require.Equal(t, len(sampleJSONData), int(iter.TotalRows))
	for i, row := range rows {
		expected := GithubRepo{}
		require.NoError(t, json.Unmarshal(sampleJSONData[i], &expected))
		require.Equal(t, expected, row)
	}

}
