package iceberg

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// columnTypes are the Iceberg types a column can have, by their Iceberg name.
var columnTypes = map[string]iceberg.Type{
	"boolean":     iceberg.PrimitiveTypes.Bool,
	"int":         iceberg.PrimitiveTypes.Int32,
	"long":        iceberg.PrimitiveTypes.Int64,
	"float":       iceberg.PrimitiveTypes.Float32,
	"double":      iceberg.PrimitiveTypes.Float64,
	"string":      iceberg.PrimitiveTypes.String,
	"date":        iceberg.PrimitiveTypes.Date,
	"timestamp":   iceberg.PrimitiveTypes.Timestamp,
	"timestamptz": iceberg.PrimitiveTypes.TimestampTz,
}

func columnTypeNames() []string {
	names := make([]string, 0, len(columnTypes))
	for name := range columnTypes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type columnConfig struct {
	name string
	typ  string
}

// schemaFromConfig builds the schema used to create the table. Field IDs are
// assigned in config order. All columns are optional.
func schemaFromConfig(cols []columnConfig) (*iceberg.Schema, error) {
	if len(cols) == 0 {
		return nil, errors.New("schema has no columns")
	}
	seen := make(map[string]bool, len(cols))
	fields := make([]iceberg.NestedField, 0, len(cols))
	for i, c := range cols {
		if c.name == "" {
			return nil, fmt.Errorf("schema column %d has no name", i)
		}
		if seen[c.name] {
			return nil, fmt.Errorf("schema column %q is defined twice", c.name)
		}
		seen[c.name] = true
		typ, ok := columnTypes[c.typ]
		if !ok {
			return nil, fmt.Errorf("schema column %q has unsupported type %q, expected one of %s", c.name, c.typ, strings.Join(columnTypeNames(), ", "))
		}
		fields = append(fields, iceberg.NestedField{ID: i + 1, Name: c.name, Type: typ})
	}
	return iceberg.NewSchema(0, fields...), nil
}

// Partition field IDs start at 1000 by Iceberg convention.
const firstPartitionFieldID = 1000

// partitionExpr matches `column`, `transform(column)` and
// `transform(N, column)`.
var partitionExpr = regexp.MustCompile(`^\s*(?:(\w+)\s*\(\s*(?:(\d+)\s*,\s*)?([^\s(),]+)\s*\)|([^\s(),]+))\s*$`)

// partitionSpecFromConfig parses expressions such as `day(created_at)`,
// `bucket(16, user_id)` or `country` (identity) into a partition spec.
func partitionSpecFromConfig(exprs []string, schema *iceberg.Schema) (iceberg.PartitionSpec, error) {
	fields := make([]iceberg.PartitionField, 0, len(exprs))
	for i, expr := range exprs {
		m := partitionExpr.FindStringSubmatch(expr)
		if m == nil {
			return iceberg.PartitionSpec{}, fmt.Errorf("invalid partition_by entry %q, expected forms like day(col), bucket(16, col) or col", expr)
		}
		name, width, column := strings.ToLower(m[1]), m[2], m[3]
		if m[4] != "" {
			name, column = "identity", m[4]
		}

		col, ok := schema.FindFieldByName(column)
		if !ok {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: no column %q in the schema", expr, column)
		}

		transformName, fieldName := name, column+"_"+name
		switch name {
		case "identity":
			fieldName = column
		case "year", "month", "day", "hour":
		case "bucket", "truncate":
			if width == "" {
				return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: %s needs a number, as in %s(16, %s)", expr, name, name, column)
			}
			transformName = name + "[" + width + "]"
			if name == "truncate" {
				fieldName = column + "_trunc"
			}
		default:
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: unknown transform %q, expected identity, year, month, day, hour, bucket or truncate", expr, name)
		}
		if width != "" && name != "bucket" && name != "truncate" {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: %s takes no number", expr, name)
		}

		transform, err := iceberg.ParseTransform(transformName)
		if err != nil {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: %w", expr, err)
		}
		if !transform.CanTransform(col.Type) {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_by entry %q: %s can't be applied to a %s column", expr, name, col.Type)
		}
		fields = append(fields, iceberg.PartitionField{
			SourceIDs: []int{col.ID},
			FieldID:   firstPartitionFieldID + i,
			Name:      fieldName,
			Transform: transform,
		})
	}
	return iceberg.NewPartitionSpec(fields...), nil
}

// defaultTableProperties are set when the output creates the table. Properties
// from the config are applied on top.
func defaultTableProperties() iceberg.Properties {
	return iceberg.Properties{
		table.PropertyFormatVersion:               "2",
		table.ParquetCompressionKey:               "zstd",
		table.ManifestMergeEnabledKey:             "true",
		table.MetadataDeleteAfterCommitEnabledKey: "true",
		table.MetadataPreviousVersionsMaxKey:      strconv.Itoa(commitPointersKept),
		table.CommitNumRetriesKey:                 "10",
	}
}
