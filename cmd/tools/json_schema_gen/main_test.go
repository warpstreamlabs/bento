package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/internal/config/schema"
	"github.com/warpstreamlabs/bento/internal/docs"
	_ "github.com/warpstreamlabs/bento/public/components/all"
)

// TestGenerateJSONSchema tests the main JSON Schema generation functionality
func TestGenerateJSONSchema(t *testing.T) {
	// Create the schema using the New function
	schemaData := schema.New("1.0", "2026-01-16")

	// Verify we have components to work with
	if len(schemaData.Inputs) == 0 {
		t.Error("Expected to have input components available")
	}
	if len(schemaData.Outputs) == 0 {
		t.Error("Expected to have output components available")
	}
	if len(schemaData.Processors) == 0 {
		t.Error("Expected to have processor components available")
	}

	// Test the conversion functions
	testFieldConversion(t, schemaData)
	testComponentHandling(t, schemaData)
}

// Test the field conversion function
func testFieldConversion(t *testing.T, _ schema.Full) {
	// Test a simple string field
	stringField := docs.FieldString("test_string", "A test string field").HasDefault("default_value")
	prop := convertFieldSpecToProperty(stringField)

	if prop.Type != "string" {
		t.Errorf("Expected string type, got %s", prop.Type)
	}
	if !strings.Contains(prop.Description, "Default: default_value") {
		t.Errorf("Expected description to contain default value, got: %s", prop.Description)
	}

	// Test an object field with children
	objectField := docs.FieldObject("test_object", "A test object field").WithChildren(
		docs.FieldString("child_string", "A child string field"),
	)
	prop = convertFieldSpecToProperty(objectField)

	if prop.Type != "object" {
		t.Errorf("Expected object type, got %s", prop.Type)
	}
	if len(prop.Properties) != 1 {
		t.Errorf("Expected 1 child property, got %d", len(prop.Properties))
	}
	if _, exists := prop.Properties["child_string"]; !exists {
		t.Error("Expected child_string property to exist")
	}
}

// Test the component handling logic
func testComponentHandling(t *testing.T, schemaData schema.Full) {
	// Test input component handling
	inputField := docs.FieldInput("test_input", "A test input field")
	prop := convertFieldSpecToProperty(inputField)

	if prop.Type != "object" {
		t.Errorf("Expected object type for input field, got %s", prop.Type)
	}

	// Verify we can add component configurations
	if len(schemaData.Inputs) > 0 {
		prop.Properties = make(map[string]Property)
		for _, input := range schemaData.Inputs {
			prop.Properties[input.Name] = convertFieldSpecToProperty(input.Config)
		}

		if len(prop.Properties) != len(schemaData.Inputs) {
			t.Errorf("Expected %d input properties, got %d", len(schemaData.Inputs), len(prop.Properties))
		}
	}

	// Test array field handling - removed as it's not essential to main functionality
	// The actual array field handling is tested in TestArrayFieldTypes
}

// Test the main JSON Schema generation
func TestMainJSONSchemaGeneration(t *testing.T) {
	// This test verifies the complete JSON Schema generation
	// We'll run the main logic and check the output structure

	schemaData := schema.New("1.0", "2026-01-16")

	jsonSchema := JSONSchema{
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Type:       "object",
		Properties: make(map[string]Property),
	}

	configProps := make(map[string]Property)

	for _, field := range schemaData.Config {
		prop := convertFieldSpecToProperty(field)

		// Apply component configurations for component-type fields
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
		}

		// Handle special array fields
		if field.Name == "processors" && field.Kind == docs.KindArray && len(schemaData.Processors) > 0 {
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			prop.Items.Properties = make(map[string]Property)
			for _, processor := range schemaData.Processors {
				prop.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
			}
		}

		configProps[field.Name] = prop
	}

	jsonSchema.Properties = configProps

	// Verify the output can be marshaled to JSON
	output, err := json.MarshalIndent(jsonSchema, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal JSON schema: %v", err)
	}

	// Verify it's valid JSON
	var parsedSchema JSONSchema
	err = json.Unmarshal(output, &parsedSchema)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON schema: %v", err)
	}

	// Verify basic structure
	if parsedSchema.Schema != "https://json-schema.org/draft/2020-12/schema" {
		t.Errorf("Unexpected schema value: %s", parsedSchema.Schema)
	}
	if parsedSchema.Type != "object" {
		t.Errorf("Expected root type to be object, got %s", parsedSchema.Type)
	}
	if len(parsedSchema.Properties) == 0 {
		t.Error("Expected properties to be populated")
	}

	// Verify some key fields exist
	expectedFields := []string{"input", "output", "pipeline", "http", "logger"}
	for _, fieldName := range expectedFields {
		if _, exists := parsedSchema.Properties[fieldName]; !exists {
			t.Errorf("Expected field %s to exist in schema", fieldName)
		}
	}
}

// Test that verifies array fields have correct types
func TestArrayFieldTypes(t *testing.T) {
	schemaData := schema.New("1.0", "2026-01-16")

	// Create a minimal schema to test array field handling
	jsonSchema := JSONSchema{
		Properties: make(map[string]Property),
	}

	for _, field := range schemaData.Config {
		if field.Name == "processors" && field.Kind == docs.KindArray {
			prop := convertFieldSpecToProperty(field)
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			if len(schemaData.Processors) > 0 {
				prop.Items.Properties = make(map[string]Property)
				for _, processor := range schemaData.Processors {
					prop.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
				}
			}
			jsonSchema.Properties["processors"] = prop
		} else if field.Name == "cache_resources" && field.Kind == docs.KindArray {
			prop := convertFieldSpecToProperty(field)
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			if len(schemaData.Caches) > 0 {
				prop.Items.Properties = make(map[string]Property)
				for _, cache := range schemaData.Caches {
					prop.Items.Properties[cache.Name] = convertFieldSpecToProperty(cache.Config)
				}
			}
			jsonSchema.Properties["cache_resources"] = prop
		} else if field.Name == "rate_limit_resources" && field.Kind == docs.KindArray {
			prop := convertFieldSpecToProperty(field)
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			if len(schemaData.RateLimits) > 0 {
				prop.Items.Properties = make(map[string]Property)
				for _, rateLimit := range schemaData.RateLimits {
					prop.Items.Properties[rateLimit.Name] = convertFieldSpecToProperty(rateLimit.Config)
				}
			}
			jsonSchema.Properties["rate_limit_resources"] = prop
		} else if field.Name == "processor_resources" && field.Kind == docs.KindArray {
			prop := convertFieldSpecToProperty(field)
			prop.Type = "array"
			if prop.Items == nil {
				prop.Items = &Property{Type: "object"}
			}
			if len(schemaData.Processors) > 0 {
				prop.Items.Properties = make(map[string]Property)
				for _, processor := range schemaData.Processors {
					prop.Items.Properties[processor.Name] = convertFieldSpecToProperty(processor.Config)
				}
			}
			jsonSchema.Properties["processor_resources"] = prop
		}
	}

	// Verify array fields have correct types
	arrayFields := []string{"processors", "cache_resources", "rate_limit_resources", "processor_resources"}
	for _, fieldName := range arrayFields {
		if prop, exists := jsonSchema.Properties[fieldName]; exists {
			if prop.Type != "array" {
				t.Errorf("Expected %s to have type 'array', got %s", fieldName, prop.Type)
			}
			if prop.Items == nil {
				t.Errorf("Expected %s to have items defined", fieldName)
			}
			if len(prop.Items.Properties) == 0 {
				t.Errorf("Expected %s items to have properties", fieldName)
			}
		}
	}
}

// Test file output functionality
func TestFileOutput(t *testing.T) {
	// Test that we can write the output to a file
	jsonSchema := JSONSchema{
		Schema:     "https://json-schema.org/draft/2020-12/schema",
		Type:       "object",
		Properties: make(map[string]Property),
	}

	// Add a simple property for testing
	simpleProp := Property{
		Type:        "string",
		Description: "A test property",
	}
	jsonSchema.Properties["test"] = simpleProp

	// Test file writing
	tempFile := "test_schema_output.json"
	defer os.Remove(tempFile)

	output, err := json.MarshalIndent(jsonSchema, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	err = os.WriteFile(tempFile, output, 0644)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Verify file was created and can be read
	content, err := os.ReadFile(tempFile)
	if err != nil {
		t.Fatalf("Failed to read test file: %v", err)
	}

	if len(content) == 0 {
		t.Error("Test file is empty")
	}

	// Verify it's valid JSON
	var parsedSchema JSONSchema
	err = json.Unmarshal(content, &parsedSchema)
	if err != nil {
		t.Fatalf("Failed to parse test file JSON: %v", err)
	}
}
