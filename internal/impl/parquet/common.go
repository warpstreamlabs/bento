package parquet

import (
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
)

func readLenient(rdr parquet.RowReader, schema *parquet.Schema, rows []any) (n int, err error) {
	parquetRows := make([]parquet.Row, len(rows))

	n, err = rdr.ReadRows(parquetRows)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	// Build array field detection map once for all rows
	arrayFields := buildArrayFieldMap(schema)

	// Convert each parquet.Row to a nested map structure
	for i := 0; i < n; i++ {
		rows[i] = buildNestedStructure(parquetRows[i], schema, arrayFields)
	}

	return n, err
}

// buildArrayFieldMap creates a map of paths that should be unwrapped as arrays
// Detects non-standard LIST encoding where fields are wrapped in .array/.list/.element
func buildArrayFieldMap(schema *parquet.Schema) map[string]bool {
	arrayFields := make(map[string]bool)
	var inspect func(fields []parquet.Field, parentPath []string)

	inspect = func(fields []parquet.Field, parentPath []string) {
		for _, field := range fields {
			currentPath := append(parentPath, field.Name())

			if !field.Leaf() {
				// Check if this is a non-standard LIST wrapper pattern
				children := field.Fields()
				if len(children) == 1 {
					childName := children[0].Name()
					if childName == "array" || childName == "list" || childName == "element" {
						// Mark the parent path (without the wrapper) as an array field
						pathKey := strings.Join(currentPath, ".")
						arrayFields[pathKey] = true
					}
				}

				// Recurse into children
				inspect(children, currentPath)
			}
		}
	}

	inspect(schema.Fields(), []string{})
	return arrayFields
}

func convertSingleValue(val parquet.Value) any {
	if val.IsNull() {
		return nil
	}

	switch val.Kind() {
	case parquet.Boolean:
		return val.Boolean()
	case parquet.Int32:
		return val.Int32()
	case parquet.Int64:
		return val.Int64()
	case parquet.Float:
		return val.Float()
	case parquet.Double:
		return val.Double()
	case parquet.ByteArray:
		b := val.ByteArray()
		if utf8.Valid(b) {
			return string(b)
		}
		return b
	case parquet.FixedLenByteArray:
		return val.ByteArray()
	default:
		return val.String()
	}
}

// buildNestedStructure reconstructs nested map structure from flat parquet values
func buildNestedStructure(parquetRow parquet.Row, schema *parquet.Schema, arrayFields map[string]bool) map[string]any {
	result := make(map[string]any)

	// Group values by their full column path
	pathValues := make(map[string][]parquet.Value)
	for _, val := range parquetRow {
		colIndex := val.Column()
		path := getColumnPath(schema, colIndex)
		if len(path) == 0 {
			continue
		}
		pathKey := strings.Join(path, ".")
		pathValues[pathKey] = append(pathValues[pathKey], val)
	}

	// Build nested structure with non-standard LIST unwrapping
	for pathKey, values := range pathValues {
		path := strings.Split(pathKey, ".")
		isArrayField := false

		// Detect and unwrap non-standard LIST encoding patterns
		// AWS Security Lake: parent.fieldname.array
		// Standard parquet-go: parent.fieldname.list.element
		// Keep unwrapping while the path ends with wrapper keywords
		for len(path) >= 2 {
			lastSegment := path[len(path)-1]
			if lastSegment != "array" && lastSegment != "list" && lastSegment != "element" {
				break // Not a wrapper
			}

			// Check parent path to see if it's a known array field
			parentPath := path[:len(path)-1]
			parentKey := strings.Join(parentPath, ".")

			// Unwrap if: multiple values, OR this is a known array field from schema
			if len(values) > 1 || arrayFields[parentKey] {
				// Unwrap: remove the wrapper level
				path = parentPath
				isArrayField = true // Mark that this is an array field
			} else {
				break // Not an array field, keep the wrapper
			}
		}

		// For null/empty arrays, treat as null instead of {"wrapper": null}
		if len(values) == 1 && values[0].IsNull() && isArrayField {
			values = nil
		}

		setNestedValue(result, path, values, isArrayField)
	}

	return result
}

// getColumnPath returns the full path for a leaf column
func getColumnPath(schema *parquet.Schema, colIndex int) []string {
	return findColumnPath(schema.Fields(), colIndex, []string{}, 0)
}

// findColumnPath recursively finds the path to a column index
func findColumnPath(fields []parquet.Field, targetCol int, currentPath []string, colOffset int) []string {
	currentCol := colOffset

	for _, field := range fields {
		fieldPath := append([]string(nil), currentPath...)
		fieldPath = append(fieldPath, field.Name())

		if field.Leaf() {
			// This is a leaf column
			if currentCol == targetCol {
				return fieldPath
			}
			currentCol++
		} else {
			// This is a group - recurse into children
			numLeaves := countLeafColumns(field)
			if targetCol < currentCol+numLeaves {
				return findColumnPath(field.Fields(), targetCol, fieldPath, currentCol)
			}
			currentCol += numLeaves
		}
	}

	return nil
}

// countLeafColumns counts the number of leaf columns in a field
func countLeafColumns(field parquet.Field) int {
	if field.Leaf() {
		return 1
	}

	count := 0
	for _, child := range field.Fields() {
		count += countLeafColumns(child)
	}
	return count
}

// setNestedValue sets a value in a nested map structure
func setNestedValue(root map[string]any, path []string, values []parquet.Value, isArrayField bool) {
	if len(path) == 0 {
		return
	}

	// Navigate to parent
	current := root
	for i := 0; i < len(path)-1; i++ {
		key := path[i]

		if _, exists := current[key]; !exists {
			current[key] = make(map[string]any)
		}

		currentMap, ok := current[key].(map[string]any)
		if !ok {
			// Type mismatch - overwrite with new map
			currentMap = make(map[string]any)
			current[key] = currentMap
		}

		current = currentMap
	}

	// Set the leaf value
	leafKey := path[len(path)-1]
	current[leafKey] = convertParquetValues(values, isArrayField)
}

// convertParquetValues converts a slice of parquet values to appropriate Go types
func convertParquetValues(values []parquet.Value, isArrayField bool) any {
	if len(values) == 0 {
		return nil
	}

	// Handle arrays (multiple values OR single value in an array field)
	if len(values) > 1 || isArrayField {
		result := make([]any, 0, len(values))
		for _, val := range values {
			if !val.IsNull() {
				result = append(result, convertSingleValue(val))
			}
		}
		return result
	}

	// Single value (non-array field)
	return convertSingleValue(values[0])
}
