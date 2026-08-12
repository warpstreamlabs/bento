package parquet

import (
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	"github.com/warpstreamlabs/bento/public/service"
)

type schemaField struct {
	Name string
	Type string

	DecimalPrecision int
	DecimalScale     int

	Repeated bool
	Optional bool
	Fields   []schemaField
}

func extract[T any](key string, m map[string]any) (T, error) {
	var zero T

	val, ok := m[key]
	if !ok {
		return zero, fmt.Errorf("required '%s'", key)
	}

	fieldValue, ok := val.(T)
	if !ok {
		return zero, fmt.Errorf("required '%s' to be of type '%T'", key, zero)
	}

	return fieldValue, nil
}

func extractOr[T any](key string, m map[string]any, def T) (T, error) {
	if _, ok := m[key]; !ok {
		return def, nil
	}
	return extract[T](key, m)
}

func parseSchema(conf *service.ParsedConfig) ([]schemaField, error) {
	v, err := conf.FieldAny("schema")
	if err != nil {
		return nil, err
	}

	raw, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected 'schema' to be a list of fields, got %T", v)
	}

	return toFields(raw)
}

func toFields(raw []any) ([]schemaField, error) {
	fields := make([]schemaField, 0, len(raw))

	for i, v := range raw {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected fields[%d] to be an object, got %T", i, v)
		}

		f, err := toField(m)
		if err != nil {
			return nil, fmt.Errorf("fields[%d]: %w", i, err)
		}

		fields = append(fields, *f)
	}

	return fields, nil
}

func toField(m map[string]any) (*schemaField, error) {
	name, err := extract[string]("name", m)
	if err != nil {
		return nil, err
	}

	if name == "" {
		return nil, errors.New("'name' must not be empty")
	}

	f := &schemaField{Name: name}

	if f.Type, err = extractOr("type", m, ""); err != nil {
		return nil, err
	}
	if f.Repeated, err = extractOr("repeated", m, false); err != nil {
		return nil, err
	}
	if f.Optional, err = extractOr("optional", m, false); err != nil {
		return nil, err
	}
	if f.DecimalPrecision, err = extractOr("decimal_precision", m, 0); err != nil {
		return nil, err
	}
	if f.DecimalScale, err = extractOr("decimal_scale", m, 0); err != nil {
		return nil, err
	}

	raw, ok := m["fields"]
	if !ok {
		if f.Type == "" {
			return nil, errors.New("requires either 'type' or 'fields'")
		}
		return f, nil
	}

	children, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("required 'fields' to be a list, got %T", raw)
	}

	if f.Fields, err = toFields(children); err != nil {
		return nil, err
	}

	return f, nil
}

//------------------------------------------------------------------------------

type parquetSchemaOpts struct {
	encoding    encoding.Encoding
	compression compress.Codec
}

func parseSchemaOpts(conf *service.ParsedConfig) (parquetSchemaOpts, error) {
	var opts parquetSchemaOpts

	enc, err := conf.FieldString("default_encoding")
	if err != nil {
		return opts, err
	}

	if opts.encoding, err = encodingFromString(enc); err != nil {
		return opts, err
	}

	comp, err := conf.FieldString("default_compression")
	if err != nil {
		return opts, err
	}

	opts.compression, err = compressionFromString(comp)
	return opts, err
}

func encodingFromString(s string) (encoding.Encoding, error) {
	switch s {
	case "":
		return nil, nil
	case "DELTA_LENGTH_BYTE_ARRAY":
		return &parquet.DeltaLengthByteArray, nil
	case "PLAIN":
		return &parquet.Plain, nil
	case "RLE_DICTIONARY":
		return &parquet.RLEDictionary, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", s)
	}
}

func compressionFromString(s string) (compress.Codec, error) {
	switch s {
	case "", "uncompressed":
		return nil, nil
	case "snappy":
		return &parquet.Snappy, nil
	case "gzip":
		return &parquet.Gzip, nil
	case "brotli":
		return &parquet.Brotli, nil
	case "zstd":
		return &parquet.Zstd, nil
	case "lz4raw":
		return &parquet.Lz4Raw, nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", s)
	}
}

func canEncode(enc encoding.Encoding, kind parquet.Kind) bool {
	if enc == nil {
		return false
	}

	switch enc.Encoding() {
	case format.Plain, format.DeltaLengthByteArray:
		return kind == parquet.ByteArray
	case format.RLEDictionary:
		return kind != parquet.Boolean
	default:
		return true
	}
}

func toParquetSchema(fields []schemaField, opts parquetSchemaOpts) (*parquet.Schema, error) {
	root, err := toParquetGroup(fields, opts)
	if err != nil {
		return nil, err
	}

	return parquet.NewSchema("", root), nil
}

func toParquetGroup(fields []schemaField, opts parquetSchemaOpts) (parquet.Group, error) {
	g := make(parquet.Group, len(fields))

	for i := range fields {
		f := &fields[i]

		if _, ok := g[f.Name]; ok {
			return nil, fmt.Errorf("duplicate field name %q", f.Name)
		}

		n, err := toParquetNode(f, opts)
		if err != nil {
			return nil, err
		}

		g[f.Name] = n
	}

	return g, nil
}

func toParquetNode(f *schemaField, opts parquetSchemaOpts) (parquet.Node, error) {
	if f.Optional && f.Repeated {
		return nil, fmt.Errorf("field %q: cannot be both optional and repeated", f.Name)
	}

	node, err := baseNode(f, opts)
	if err != nil {
		return nil, err
	}

	if node.Leaf() {
		if canEncode(opts.encoding, node.Type().Kind()) {
			node = parquet.Encoded(node, opts.encoding)
		}

		if opts.compression != nil {
			node = parquet.Compressed(node, opts.compression)
		}
	}

	switch {
	case f.Optional:
		node = parquet.Optional(node)
	case f.Repeated:
		node = parquet.Repeated(node)
	}

	return node, nil
}

func baseNode(f *schemaField, opts parquetSchemaOpts) (parquet.Node, error) {
	switch f.Type {
	case "", "STRUCT":
		if len(f.Fields) == 0 {
			return nil, fmt.Errorf("field %q: STRUCT requires child fields", f.Name)
		}
		return toParquetGroup(f.Fields, opts)
	case "BOOLEAN":
		return parquet.Leaf(parquet.BooleanType), nil
	case "INT8":
		return parquet.Int(8), nil
	case "INT16":
		return parquet.Int(16), nil
	case "INT32":
		return parquet.Int(32), nil
	case "INT64":
		return parquet.Int(64), nil
	case "FLOAT":
		return parquet.Leaf(parquet.FloatType), nil
	case "DOUBLE":
		return parquet.Leaf(parquet.DoubleType), nil
	case "BYTE_ARRAY":
		return parquet.Leaf(parquet.ByteArrayType), nil
	case "UTF8":
		return parquet.String(), nil
	case "DECIMAL32":
		return decimalNode(f, parquet.Int32Type, 9)
	case "DECIMAL64":
		return decimalNode(f, parquet.Int64Type, 18)
	case "LIST":
		return listNode(f, opts)
	case "MAP":
		return mapNode(f, opts)
	default:
		return nil, fmt.Errorf("field %q: unsupported type %q", f.Name, f.Type)
	}
}

func listNode(f *schemaField, opts parquetSchemaOpts) (parquet.Node, error) {
	if len(f.Fields) != 1 {
		return nil, fmt.Errorf("field %q: LIST wants exactly one child, got %d", f.Name, len(f.Fields))
	}

	elem, err := toParquetNode(&f.Fields[0], opts)
	if err != nil {
		return nil, fmt.Errorf("field %q: %w", f.Name, err)
	}

	return parquet.List(elem), nil
}

func mapNode(f *schemaField, opts parquetSchemaOpts) (parquet.Node, error) {
	if len(f.Fields) != 2 {
		return nil, fmt.Errorf("field %q: MAP wants exactly two children (key and value), got %d", f.Name, len(f.Fields))
	}

	keyNode, err := toParquetNode(&f.Fields[0], opts)
	if err != nil {
		return nil, fmt.Errorf("field %q: %w", f.Name, err)
	}

	valueNode, err := toParquetNode(&f.Fields[1], opts)
	if err != nil {
		return nil, fmt.Errorf("field %q: %w", f.Name, err)
	}

	return parquet.Map(keyNode, valueNode), nil
}

func decimalNode(f *schemaField, typ parquet.Type, maxPrecision int) (parquet.Node, error) {
	switch {
	case f.DecimalPrecision <= 0:
		return nil, fmt.Errorf("field %q: %s requires decimal_precision", f.Name, f.Type)
	case f.DecimalPrecision > maxPrecision:
		return nil, fmt.Errorf("field %q: %s supports a precision of at most %d, got %d", f.Name, f.Type, maxPrecision, f.DecimalPrecision)
	case f.DecimalScale < 0 || f.DecimalScale > f.DecimalPrecision:
		return nil, fmt.Errorf("field %q: decimal_scale must be between 0 and decimal_precision", f.Name)
	}

	return parquet.Decimal(f.DecimalScale, f.DecimalPrecision, typ), nil
}
