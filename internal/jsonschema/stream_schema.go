package jsonschema

import (
	"encoding/json"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
)

func compSpecsToDefinition(specs []docs.ComponentSpec, typeFields map[string]docs.FieldSpec) map[string]any {
	generalFields := map[string]any{}
	for k, v := range typeFields {
		generalFields[k] = v.JSONSchema()
	}

	var componentDefs []any
	for _, s := range specs {
		componentDefs = append(componentDefs, map[string]any{
			"type": "object",
			"properties": map[string]any{
				s.Name: s.Config.JSONSchema(),
			},
		})
	}

	return map[string]any{
		"allOf": []any{
			map[string]any{
				"anyOf": componentDefs, // TODO: Convert this to oneOf once issues are resolved.
			},
			map[string]any{
				"type":       "object",
				"properties": generalFields,
			},
		},
	}
}

// Marshal attempts to marshal a JSON Schema definition containing the entire
// config and plugin ecosystem such that other applications can potentially
// execute their own linting and generation tools with it.
func Marshal(fields docs.FieldSpecs, env *bundle.Environment) ([]byte, error) {
	defs := map[string]any{
		"input":      compSpecsToDefinition(env.InputDocs(), docs.ReservedFieldsByType(docs.TypeInput)),
		"buffer":     compSpecsToDefinition(env.BufferDocs(), docs.ReservedFieldsByType(docs.TypeBuffer)),
		"cache":      compSpecsToDefinition(env.CacheDocs(), docs.ReservedFieldsByType(docs.TypeCache)),
		"processor":  compSpecsToDefinition(env.ProcessorDocs(), docs.ReservedFieldsByType(docs.TypeProcessor)),
		"rate_limit": compSpecsToDefinition(env.RateLimitDocs(), docs.ReservedFieldsByType(docs.TypeRateLimit)),
		"output":     compSpecsToDefinition(env.OutputDocs(), docs.ReservedFieldsByType(docs.TypeOutput)),
		"metrics":    compSpecsToDefinition(env.MetricsDocs(), docs.ReservedFieldsByType(docs.TypeMetrics)),
		"tracer":     compSpecsToDefinition(env.TracersDocs(), docs.ReservedFieldsByType(docs.TypeTracer)),
		"scanner":    compSpecsToDefinition(env.ScannerDocs(), docs.ReservedFieldsByType(docs.TypeScanner)),
	}

	schemaObj := map[string]any{
		"properties":  fields.JSONSchema(),
		"definitions": defs,
	}

	return json.Marshal(schemaObj)
}
