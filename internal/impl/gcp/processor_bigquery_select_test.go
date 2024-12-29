package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
)

var testBQProcessorYAML = `
project: job-project
table: bigquery-public-data.samples.shakespeare
columns:
  - word
  - sum(word_count) as total_count
where: length(word) >= ?
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ this.term ]
`

var testBQProcessorDynamicYAML = `
project: job-project
table: ${! this.table }
columns_mapping: root = this.columns
where: ${! this.where }
unsafe_dynamic_query: true
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ this.term ]
`

func TestGCPBigQuerySelectProcessor(t *testing.T) {
	testCases := []struct {
		name            string
		yamlConfig      string
		inputMessages   []string
		expectedResults []map[string]any
		expectedQueries []string
	}{
		{
			name:       "Static Config Test",
			yamlConfig: testBQProcessorYAML,
			inputMessages: []string{
				`{"term": "test1"}`,
				`{"term": "test2"}`,
			},
			expectedResults: []map[string]any{
				{"total_count": 25568, "word": "the"},
				{"total_count": 19649, "word": "and"},
			},
			expectedQueries: []string{"test1", "test2"},
		},
		{
			name:       "Dynamic Config Test",
			yamlConfig: testBQProcessorDynamicYAML,
			inputMessages: []string{
				`{"term": "test1", "table": "bigquery-public-data.samples.shakespeare", "columns": ["word", "sum(word_count) as total_count"], "where": "length(word) >= ?"}`,
				`{"term": "test2", "table": "bigquery-public-data.samples.shakespeare", "columns": ["word", "sum(word_count) as total_count"], "where": "length(word) >= ?"}`,
			},
			expectedResults: []map[string]any{
				{"total_count": 25568, "word": "the"},
				{"total_count": 19649, "word": "and"},
			},
			expectedQueries: []string{"test1", "test2"},
		},
	}
	for _, tc := range testCases {
		spec := newBigQuerySelectProcessorConfig()

		parsed, err := spec.ParseYAML(tc.yamlConfig, nil)
		require.NoError(t, err)

		proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
			clientOptions: []option.ClientOption{option.WithoutAuthentication()},
		})
		require.NoError(t, err)

		mockClient := &mockBQClient{}
		proc.client = mockClient

		expected := tc.expectedResults

		expectedMsg, err := json.Marshal(expected)
		require.NoError(t, err)

		var rows []string
		for _, v := range expected {
			row, err := json.Marshal(v)
			require.NoError(t, err)

			rows = append(rows, string(row))
		}

		iter := &mockBQIterator{
			rows: rows,
		}

		mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

		inbatch := service.MessageBatch{
			service.NewMessage([]byte(tc.inputMessages[0])),
			service.NewMessage([]byte(tc.inputMessages[1])),
		}

		batches, err := proc.ProcessBatch(context.Background(), inbatch)
		require.NoError(t, err)
		require.Len(t, batches, 1)

		// Assert that we generated the right parameters for each BQ query
		mockClient.AssertNumberOfCalls(t, "RunQuery", 2)
		call1 := mockClient.Calls[0]
		args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
		require.ElementsMatch(t, args1, []string{tc.expectedQueries[0]})
		call2 := mockClient.Calls[1]
		args2 := call2.Arguments[1].(*bqQueryBuilderOptions).args
		require.ElementsMatch(t, args2, []string{tc.expectedQueries[1]})

		outbatch := batches[0]
		require.Len(t, outbatch, 2)

		msg1, err := outbatch[0].AsBytes()
		require.NoError(t, err)
		require.JSONEq(t, string(expectedMsg), string(msg1))

		msg2, err := outbatch[0].AsBytes()
		require.NoError(t, err)
		require.JSONEq(t, string(expectedMsg), string(msg2))

		mockClient.AssertExpectations(t)
	}
}

func TestGCPBigQuerySelectProcessor_IteratorError(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	})
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	proc.client = mockClient

	testErr := errors.New("simulated err")
	iter := &mockBQIterator{
		rows:   []string{`{"total_count": 25568, "word": "the"}`},
		err:    testErr,
		errIdx: 1,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	inmsg := []byte(`{"term": "test1"}`)
	inbatch := service.MessageBatch{
		service.NewMessage(inmsg),
	}

	batches, err := proc.ProcessBatch(context.Background(), inbatch)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	// Assert that we generated the right parameters for each BQ query
	mockClient.AssertNumberOfCalls(t, "RunQuery", 1)
	call1 := mockClient.Calls[0]
	args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args1, []string{"test1"})

	outbatch := batches[0]
	require.Len(t, outbatch, 1)

	msg1, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(inmsg), string(msg1))

	msgErr := outbatch[0].GetError()
	require.Contains(t, msgErr.Error(), testErr.Error())

	mockClient.AssertExpectations(t)
}

var dynTrueButWithColumns = `
project: job-project
table: ${! this.table }
columns:
  - name
  - age
where: ${! this.where }
unsafe_dynamic_query: true
args_mapping: |
  root = [ this.term ]
`

var dynFalseButWithColumnsMapping = `
project: job-project
table: ${! this.table }
columns_mapping: root = this.columns
where: ${! this.where }
unsafe_dynamic_query: false
args_mapping: |
  root = [ this.term ]
`

var columnsAndColumnsMapping = `
project: job-project
table: ${! this.table }
columns_mapping: root = this.columns
columns:
  - name
  - age
where: ${! this.where }
unsafe_dynamic_query: true
args_mapping: |
  root = [ this.term ]
`

func TestGCPBigQuerySelectProcessor_ParseConfigs(t *testing.T) {
	testConfigs := []struct {
		name          string
		yamlConfig    string
		expectedError string
	}{
		{
			name:          "Unsafe dynamic query set to true but columns provided",
			yamlConfig:    dynTrueButWithColumns,
			expectedError: "failed to parse config: invalid gcp_bigquery_select config: unsafe_dynamic_query set to true but no columns_mapping provided",
		},
		{
			name:          "Unsafe dynamic query set to false but columns_mapping provided",
			yamlConfig:    dynFalseButWithColumnsMapping,
			expectedError: "failed to parse config: invalid gcp_bigquery_select config: unsafe_dynamic_query set to false but columns_mapping provided",
		},
		{
			name:          "Unsafe dynamic query set to false but columns_mapping provided",
			yamlConfig:    columnsAndColumnsMapping,
			expectedError: "failed to parse config: invalid gcp_bigquery_select config: cannot set both columns_mapping and columns field",
		},
	}

	for _, tc := range testConfigs {

		spec := newBigQuerySelectProcessorConfig()

		parsed, err := spec.ParseYAML(tc.yamlConfig, nil)
		require.NoError(t, err)

		_, err = newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
			clientOptions: []option.ClientOption{option.WithoutAuthentication()},
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), tc.expectedError)
	}
}
