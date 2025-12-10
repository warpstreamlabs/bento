package opensnowcat

import (
	"encoding/json"
	"strings"
)

func (o *opensnowcatProcessor) extractSchemasFromEvent(columns []string) []string {
	schemasMap := make(map[string]bool)

	// Extract from contexts
	o.extractSchemasFromField(columns, "contexts", schemasMap)

	// Extract from derived_contexts
	o.extractSchemasFromField(columns, "derived_contexts", schemasMap)

	// Extract from unstruct_event
	o.extractSchemasFromField(columns, "unstruct_event", schemasMap)

	// Convert map to slice
	schemas := make([]string, 0, len(schemasMap))
	for schema := range schemasMap {
		schemas = append(schemas, schema)
	}

	return schemas
}

func (o *opensnowcatProcessor) extractSchemasFromField(columns []string, fieldName string, schemasMap map[string]bool) {
	colIndex, exists := o.columnIndexMap[fieldName]
	if !exists || colIndex >= len(columns) {
		return
	}

	jsonValue := columns[colIndex]
	if jsonValue == "" {
		return
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonValue), &data); err != nil {
		// Silently skip invalid JSON
		return
	}

	// Recursively extract schema URIs
	o.extractSchemasRecursive(data, schemasMap)
}

func (o *opensnowcatProcessor) extractSchemasRecursive(data interface{}, schemasMap map[string]bool) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Check if this object has a "schema" field with an Iglu URI
		if schemaVal, ok := v["schema"].(string); ok {
			if strings.HasPrefix(schemaVal, "iglu:") {
				schemasMap[schemaVal] = true
			}
		}

		for _, value := range v {
			o.extractSchemasRecursive(value, schemasMap)
		}

	case []interface{}:
		for _, item := range v {
			o.extractSchemasRecursive(item, schemasMap)
		}
	}
}

func (o *opensnowcatProcessor) extractSchemasAsJSON(columns []string) (string, error) {
	schemas := o.extractSchemasFromEvent(columns)

	jsonBytes, err := json.Marshal(schemas)
	if err != nil {
		return "[]", err
	}

	return string(jsonBytes), nil
}
