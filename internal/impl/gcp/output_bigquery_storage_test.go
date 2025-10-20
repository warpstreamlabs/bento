package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

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

func TestGCPBigQueryStorageOutputMessageFormatConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         string
		expectedFormat string
	}{
		{
			name: "json format",
			config: `
project: test
dataset: test
table: test
message_format: json`,
			expectedFormat: "json",
		},
		{
			name: "protobuf format",
			config: `
project: test
dataset: test
table: test
message_format: protobuf`,
			expectedFormat: "protobuf",
		},
		{
			name: "default format",
			config: `
project: test
dataset: test
table: test`,
			expectedFormat: "json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := gcpBigQueryWriteAPIConfFromYAML(t, tt.config)
			require.Equal(t, tt.expectedFormat, config.messageFormat)

			_, err := newBigQueryStorageOutput(config, nil)
			require.NoError(t, err)
		})
	}
}

func TestGCPBigQueryStorageOutputProtobufFormat(t *testing.T) {
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

	fileDesc := createTestProtobufDescriptor(t)
	msgDesc := fileDesc.Messages().ByName("TestMessage")
	sampleProtobufData := createSampleProtobufMessages(t, msgDesc)

	emulator := setupBigQueryEmulator(t, "project_meow", "dataset_meow", "table_meow", &schema)

	config := gcpBigQueryWriteAPIConfFromYAML(t, fmt.Sprintf(`
project: project_meow
dataset: dataset_meow
table: table_meow
message_format: protobuf
endpoint:
  grpc: %s
  http: %s
`, emulator.grpcEndpoint, emulator.httpEndpoint))

	output, err := newBigQueryStorageOutput(config, nil)
	require.NoError(t, err)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())
	require.NoError(t, err)

	batch := make(service.MessageBatch, len(sampleProtobufData))
	for i, msgBytes := range sampleProtobufData {
		batch[i] = service.NewMessage(msgBytes)
	}

	err = output.WriteBatch(context.Background(), batch)
	require.NoError(t, err)

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

	require.Equal(t, len(sampleProtobufData), int(iter.TotalRows))

	expectedValues := []GithubRepo{
		{Type: "foo", Public: true, Repo: struct {
			ID   bigquery.NullInt64 `bigquery:"id" json:"id"`
			Name string             `bigquery:"name" json:"name"`
			URL  string             `bigquery:"url" json:"url"`
		}{ID: bigquery.NullInt64{Int64: 99, Valid: true}, Name: "repo_name_1", URL: "https://one.example.com"}},
		{Type: "bar", Public: false, Repo: struct {
			ID   bigquery.NullInt64 `bigquery:"id" json:"id"`
			Name string             `bigquery:"name" json:"name"`
			URL  string             `bigquery:"url" json:"url"`
		}{ID: bigquery.NullInt64{Int64: 101, Valid: true}, Name: "repo_name_2", URL: "https://two.example.com"}},
	}

	for i, row := range rows {
		require.Equal(t, expectedValues[i].Type, row.Type)
		require.Equal(t, expectedValues[i].Public, row.Public)
		require.Equal(t, expectedValues[i].Repo.Name, row.Repo.Name)
		require.Equal(t, expectedValues[i].Repo.URL, row.Repo.URL)
		if expectedValues[i].Repo.ID.Valid {
			require.Equal(t, expectedValues[i].Repo.ID.Int64, row.Repo.ID.Int64)
		}

	}
}

func TestGCPBigQueryStorageOutputProtobufUnmarshalError(t *testing.T) {
	type SimpleRecord struct {
		Name string `bigquery:"name" json:"name"`
	}

	schema, err := bigquery.InferSchema(SimpleRecord{})
	require.NoError(t, err)

	emulator := setupBigQueryEmulator(t, "project_test", "dataset_test", "table_test", &schema)

	config := gcpBigQueryWriteAPIConfFromYAML(t, fmt.Sprintf(`
project: project_test
dataset: dataset_test
table: table_test
message_format: protobuf
endpoint:
  grpc: %s
  http: %s
`, emulator.grpcEndpoint, emulator.httpEndpoint))

	output, err := newBigQueryStorageOutput(config, nil)
	require.NoError(t, err)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())
	require.NoError(t, err)

	invalidProtobufData := []byte("this is not valid protobuf data")
	batch := service.MessageBatch{service.NewMessage(invalidProtobufData)}

	err = output.WriteBatch(context.Background(), batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot parse invalid wire-format data")
}

func createTestProtobufDescriptor(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()

	fileDescProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("type"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("public"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
					},
					{
						Name:     proto.String("repo"),
						Number:   proto.Int32(3),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String(".test.RepoMessage"),
					},
				},
			},
			{
				Name: proto.String("RepoMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("id"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					},
					{
						Name:   proto.String("name"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name:   proto.String("url"),
						Number: proto.Int32(3),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
				},
			},
		},
	}

	fileDesc, err := protodesc.NewFile(fileDescProto, nil)
	require.NoError(t, err)
	return fileDesc
}

func createSampleProtobufMessages(t *testing.T, msgDesc protoreflect.MessageDescriptor) [][]byte {
	t.Helper()

	var messages [][]byte

	sampleData := []map[string]interface{}{
		{
			"type":   "foo",
			"public": true,
			"repo": map[string]interface{}{
				"id":   int64(99),
				"name": "repo_name_1",
				"url":  "https://one.example.com",
			},
		},
		{
			"type":   "bar",
			"public": false,
			"repo": map[string]interface{}{
				"id":   int64(101),
				"name": "repo_name_2",
				"url":  "https://two.example.com",
			},
		},
	}

	for _, data := range sampleData {
		msg := dynamicpb.NewMessage(msgDesc)

		msg.Set(msgDesc.Fields().ByName("type"), protoreflect.ValueOfString(data["type"].(string)))
		msg.Set(msgDesc.Fields().ByName("public"), protoreflect.ValueOfBool(data["public"].(bool)))

		repoDesc := msgDesc.Fields().ByName("repo").Message()
		repoMsg := dynamicpb.NewMessage(repoDesc)
		repoData := data["repo"].(map[string]interface{})

		repoMsg.Set(repoDesc.Fields().ByName("id"), protoreflect.ValueOfInt64(repoData["id"].(int64)))
		repoMsg.Set(repoDesc.Fields().ByName("name"), protoreflect.ValueOfString(repoData["name"].(string)))
		repoMsg.Set(repoDesc.Fields().ByName("url"), protoreflect.ValueOfString(repoData["url"].(string)))

		msg.Set(msgDesc.Fields().ByName("repo"), protoreflect.ValueOfMessage(repoMsg))

		msgBytes, err := proto.Marshal(msg)
		require.NoError(t, err)

		messages = append(messages, msgBytes)
	}

	return messages
}
