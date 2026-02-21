package opensnowcat

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestExtractSchemasFromEvent(t *testing.T) {
	proc := &opensnowcatProcessor{
		columnIndexMap: make(map[string]int),
		log:            service.MockResources().Logger(),
	}

	for i, col := range opensnowcatColumns {
		proc.columnIndexMap[col] = i
	}

	tests := []struct {
		name            string
		contexts        string
		derivedContexts string
		unstructEvent   string
		expectedSchemas []string
	}{
		{
			name: "single context schema",
			contexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [{
					"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
					"data": {"id": "test-page-id"}
				}]
			}`,
			derivedContexts: "",
			unstructEvent:   "",
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
			},
		},
		{
			name: "multiple contexts",
			contexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [
					{
						"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
						"data": {"id": "page-1"}
					},
					{
						"schema": "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0",
						"data": {"useragentFamily": "Chrome"}
					}
				]
			}`,
			derivedContexts: "",
			unstructEvent:   "",
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0",
			},
		},
		{
			name:            "unstruct event",
			contexts:        "",
			derivedContexts: "",
			unstructEvent: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"data": {
					"schema": "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
					"data": {"pageUrl": "https://example.com"}
				}
			}`,
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
			},
		},
		{
			name:     "derived contexts",
			contexts: "",
			derivedContexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [{
					"schema": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1",
					"data": {"sessionId": "session-123"}
				}]
			}`,
			unstructEvent: "",
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1",
			},
		},
		{
			name: "all three fields combined",
			contexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [{
					"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
					"data": {"id": "page-1"}
				}]
			}`,
			derivedContexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [{
					"schema": "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0",
					"data": {"useragentFamily": "Chrome"}
				}]
			}`,
			unstructEvent: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"data": {
					"schema": "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
					"data": {"pageUrl": "https://example.com"}
				}
			}`,
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
			},
		},
		{
			name: "duplicate schemas (should be deduplicated)",
			contexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [
					{
						"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
						"data": {"id": "page-1"}
					},
					{
						"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
						"data": {"id": "page-2"}
					}
				]
			}`,
			derivedContexts: "",
			unstructEvent:   "",
			expectedSchemas: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
			},
		},
		{
			name:            "empty fields",
			contexts:        "",
			derivedContexts: "",
			unstructEvent:   "",
			expectedSchemas: []string{},
		},
		{
			name:            "invalid JSON (should not crash)",
			contexts:        "not valid json",
			derivedContexts: "",
			unstructEvent:   "",
			expectedSchemas: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := make([]string, len(opensnowcatColumns))

			if contextIdx, ok := proc.columnIndexMap["contexts"]; ok {
				columns[contextIdx] = tt.contexts
			}
			if derivedIdx, ok := proc.columnIndexMap["derived_contexts"]; ok {
				columns[derivedIdx] = tt.derivedContexts
			}
			if unstructIdx, ok := proc.columnIndexMap["unstruct_event"]; ok {
				columns[unstructIdx] = tt.unstructEvent
			}

			schemas := proc.extractSchemasFromEvent(columns)

			assert.ElementsMatch(t, tt.expectedSchemas, schemas, "Schema URIs should match")
		})
	}
}

func TestExtractSchemasRecursive(t *testing.T) {
	proc := &opensnowcatProcessor{
		log: service.MockResources().Logger(),
	}

	tests := []struct {
		name            string
		jsonData        interface{}
		expectedSchemas []string
	}{
		{
			name: "nested schemas",
			jsonData: map[string]interface{}{
				"schema": "iglu:com.example/outer/jsonschema/1-0-0",
				"data": map[string]interface{}{
					"nested": map[string]interface{}{
						"schema": "iglu:com.example/inner/jsonschema/1-0-0",
						"data":   map[string]interface{}{},
					},
				},
			},
			expectedSchemas: []string{
				"iglu:com.example/outer/jsonschema/1-0-0",
				"iglu:com.example/inner/jsonschema/1-0-0",
			},
		},
		{
			name: "array of schemas",
			jsonData: []interface{}{
				map[string]interface{}{
					"schema": "iglu:com.example/schema1/jsonschema/1-0-0",
				},
				map[string]interface{}{
					"schema": "iglu:com.example/schema2/jsonschema/1-0-0",
				},
			},
			expectedSchemas: []string{
				"iglu:com.example/schema1/jsonschema/1-0-0",
				"iglu:com.example/schema2/jsonschema/1-0-0",
			},
		},
		{
			name: "no schema fields",
			jsonData: map[string]interface{}{
				"data": "some data",
				"id":   123,
			},
			expectedSchemas: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemasMap := make(map[string]bool)
			proc.extractSchemasRecursive(tt.jsonData, schemasMap)

			schemas := make([]string, 0, len(schemasMap))
			for schema := range schemasMap {
				schemas = append(schemas, schema)
			}

			assert.ElementsMatch(t, tt.expectedSchemas, schemas)
		})
	}
}

func TestExtractSchemasAsJSON(t *testing.T) {
	proc := &opensnowcatProcessor{
		columnIndexMap: make(map[string]int),
		log:            service.MockResources().Logger(),
	}

	for i, col := range opensnowcatColumns {
		proc.columnIndexMap[col] = i
	}

	tests := []struct {
		name            string
		contexts        string
		derivedContexts string
		unstructEvent   string
		expectInJSON    []string
	}{
		{
			name: "simple example",
			contexts: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"data": [{
					"schema": "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
					"data": {"id": "test"}
				}]
			}`,
			derivedContexts: "",
			unstructEvent: `{
				"schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"data": {
					"schema": "iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
					"data": {}
				}
			}`,
			expectInJSON: []string{
				"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
				"iglu:com.snowplowanalytics.snowplow/page_view/jsonschema/1-0-0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := make([]string, len(opensnowcatColumns))

			if contextIdx, ok := proc.columnIndexMap["contexts"]; ok {
				columns[contextIdx] = tt.contexts
			}
			if derivedIdx, ok := proc.columnIndexMap["derived_contexts"]; ok {
				columns[derivedIdx] = tt.derivedContexts
			}
			if unstructIdx, ok := proc.columnIndexMap["unstruct_event"]; ok {
				columns[unstructIdx] = tt.unstructEvent
			}

			jsonStr, err := proc.extractSchemasAsJSON(columns)
			assert.NoError(t, err)

			var schemas []string
			err = json.Unmarshal([]byte(jsonStr), &schemas)
			assert.NoError(t, err)

			assert.ElementsMatch(t, tt.expectInJSON, schemas)

			t.Logf("Extracted schemas as JSON: %s", jsonStr)
		})
	}
}
