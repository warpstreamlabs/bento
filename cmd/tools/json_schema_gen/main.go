package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/warpstreamlabs/bento/internal/config/schema"
	"github.com/warpstreamlabs/bento/internal/docs"

	// Import all plugins defined within the repo.
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

// JSONSchema represents the root JSON Schema structure
type JSONSchema struct {
	Schema     string              `json:"$schema"`
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties"`
}

// Property represents a JSON Schema property
type Property struct {
	Type        string              `json:"type,omitempty"`
	Description string              `json:"description,omitempty"`
	Properties  map[string]Property `json:"properties,omitempty"`
	Items       *Property           `json:"items,omitempty"`
	Examples    []interface{}       `json:"examples,omitempty"`
}

// convertFieldSpecToProperty converts a FieldSpec to a JSON Schema Property
func convertFieldSpecToProperty(field docs.FieldSpec) Property {
	prop := Property{
		Description: field.Description,
		Examples:    field.Examples,
	}

	// Handle default values by appending to description
	if field.Default != nil {
		defaultStr := fmt.Sprintf(" Default: %v", *field.Default)
		if prop.Description != "" {
			prop.Description += defaultStr
		} else {
			prop.Description = defaultStr
		}
	}

	// Map Bento field types to JSON Schema types
	switch field.Type {
	case docs.FieldTypeString:
		// Check if this is actually an array field
		if field.Kind == docs.KindArray {
			prop.Type = "array"
		} else {
			prop.Type = "string"
		}
	case docs.FieldTypeInt:
		prop.Type = "integer"
	case docs.FieldTypeFloat:
		prop.Type = "number"
	case docs.FieldTypeBool:
		prop.Type = "boolean"
	case docs.FieldTypeObject:
		if field.Kind == docs.KindArray {
			prop.Type = "array"
		} else {
			prop.Type = "object"
			if len(field.Children) > 0 {
				prop.Properties = make(map[string]Property)
				for _, child := range field.Children {
					prop.Properties[child.Name] = convertFieldSpecToProperty(child)
				}
			}
		}
	case docs.FieldTypeInput, docs.FieldTypeOutput, docs.FieldTypeProcessor,
		docs.FieldTypeBuffer, docs.FieldTypeCache, docs.FieldTypeMetrics,
		docs.FieldTypeTracer, docs.FieldTypeScanner:
		if field.Kind == docs.KindArray {
			prop.Type = "array"
		} else {
			prop.Type = "object"
		}
		// For component types, expand their children if available
		if len(field.Children) > 0 {
			prop.Properties = make(map[string]Property)
			for _, child := range field.Children {
				prop.Properties[child.Name] = convertFieldSpecToProperty(child)
			}
		}
	}

	// Handle array types
	switch field.Kind {
	case docs.KindArray:
		if prop.Type == "" {
			prop.Type = "array"
		}
		itemProp := Property{}
		if field.Type != "" {
			// Convert the field type to the items type
			itemProp.Type = mapFieldTypeToJSONType(field.Type)
		} else {
			itemProp.Type = "object"
		}
		prop.Items = &itemProp
	case docs.Kind2DArray:
		prop.Type = "array"
		innerItem := Property{Type: "array"}
		itemProp := Property{Items: &innerItem}
		prop.Items = &itemProp
	case docs.KindMap:
		prop.Type = "object"
		// For maps, we treat as object with arbitrary properties
	}

	return prop
}

// mapFieldTypeToJSONType maps Bento field types to JSON Schema types
func mapFieldTypeToJSONType(fieldType docs.FieldType) string {
	switch fieldType {
	case docs.FieldTypeString:
		return "string"
	case docs.FieldTypeInt:
		return "integer"
	case docs.FieldTypeFloat:
		return "number"
	case docs.FieldTypeBool:
		return "boolean"
	case docs.FieldTypeObject:
		return "object"
	default:
		return "object" // Default to object for component types
	}
}

func main() {
	// Create the schema using the New function
	schemaData := schema.New("1.0", "2026-01-16")

	// Create the root JSON Schema structure
	jsonSchema := JSONSchema{
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Type:       "object",
		Properties: make(map[string]Property),
	}

	// Convert ALL configuration fields
	configProps := make(map[string]Property)

	for _, field := range schemaData.Config {
		// Include ALL fields in the configuration
		prop := convertFieldSpecToProperty(field)

		// For component-type fields, add the actual component configurations
		switch field.Type {
		case docs.FieldTypeInput:
			if len(schemaData.Inputs) > 0 {
				prop.Properties = make(map[string]Property)
				for _, input := range schemaData.Inputs {
					prop.Properties[input.Name] = convertFieldSpecToProperty(input.Config)
				}
			}
		case docs.FieldTypeOutput:
			if len(schemaData.Outputs) > 0 {
				prop.Properties = make(map[string]Property)
				for _, output := range schemaData.Outputs {
					prop.Properties[output.Name] = convertFieldSpecToProperty(output.Config)
				}
			}
		case docs.FieldTypeBuffer:
			if len(schemaData.Buffers) > 0 {
				prop.Properties = make(map[string]Property)
				for _, buffer := range schemaData.Buffers {
					prop.Properties[buffer.Name] = convertFieldSpecToProperty(buffer.Config)
				}
			}
		case docs.FieldTypeMetrics:
			if len(schemaData.Metrics) > 0 {
				prop.Properties = make(map[string]Property)
				for _, metric := range schemaData.Metrics {
					prop.Properties[metric.Name] = convertFieldSpecToProperty(metric.Config)
				}
			}
		case docs.FieldTypeTracer:
			if len(schemaData.Tracers) > 0 {
				prop.Properties = make(map[string]Property)
				for _, tracer := range schemaData.Tracers {
					prop.Properties[tracer.Name] = convertFieldSpecToProperty(tracer.Config)
				}
			}
		}

		// Handle special array fields that should contain component configurations
		if field.Name == "processors" && field.Kind == docs.KindArray && len(schemaData.Processors) > 0 {
			// Ensure the type is set to array (it should already be from convertFieldSpecToProperty)
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			// Add processor configurations to the items
			prop.Items.Properties = make(map[string]Property)
			for _, processor := range schemaData.Processors {
				prop.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
			}
		} else if field.Name == "cache_resources" && field.Kind == docs.KindArray && len(schemaData.Caches) > 0 {
			// Fix cache_resources type to be array and add cache configurations
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			// Add cache configurations to the items
			prop.Items.Properties = make(map[string]Property)
			for _, cache := range schemaData.Caches {
				prop.Items.Properties[cache.Name] = convertFieldSpecToProperty(cache.Config)
			}
		} else if field.Name == "rate_limit_resources" && field.Kind == docs.KindArray && len(schemaData.RateLimits) > 0 {
			// Add rate limit configurations to the items
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			// Add rate limit configurations to the items
			prop.Items.Properties = make(map[string]Property)
			for _, rateLimit := range schemaData.RateLimits {
				prop.Items.Properties[rateLimit.Name] = convertFieldSpecToProperty(rateLimit.Config)
			}
		} else if field.Name == "processor_resources" && field.Kind == docs.KindArray && len(schemaData.Processors) > 0 {
			// Fix processor_resources type to be array and add processor configurations
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			// Add processor configurations to the items
			prop.Items.Properties = make(map[string]Property)
			for _, processor := range schemaData.Processors {
				prop.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
			}
		} else if field.Name == "pipeline" && len(field.Children) > 0 {
			// Handle processors array within pipeline
			for _, child := range field.Children {
				if child.Name == "processors" && child.Kind == docs.KindArray && len(schemaData.Processors) > 0 {
					// Find the processors property in the pipeline properties
					if processorsProp, exists := prop.Properties["processors"]; exists {
						// Ensure the type is set to array
						processorsProp.Type = "array"
						if processorsProp.Items == nil {
							processorsProp.Items = &Property{Type: "object"}
						}
						// Add processor configurations to the items
						processorsProp.Items.Properties = make(map[string]Property)
						for _, processor := range schemaData.Processors {
							processorsProp.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
						}
						// Update the property in the map
						prop.Properties["processors"] = processorsProp
					}
				}
			}
		}

		configProps[field.Name] = prop
	}

	jsonSchema.Properties = configProps

	// Output the JSON Schema
	output, err := json.MarshalIndent(jsonSchema, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(output))
}
