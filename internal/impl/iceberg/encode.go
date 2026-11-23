package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// errUnknownField marks a message with a field the table has no column for.
var errUnknownField = errors.New("no such column in the table")

// Iceberg timestamps are microseconds. `timestamptz` is stored in UTC.
var (
	arrowTimestamp   = &arrow.TimestampType{Unit: arrow.Microsecond}
	arrowTimestampTz = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
)

type column struct {
	name     string
	typ      iceberg.Type
	required bool
}

func arrowTypeOf(t iceberg.Type) (arrow.DataType, bool) {
	switch t.(type) {
	case iceberg.BooleanType:
		return arrow.FixedWidthTypes.Boolean, true
	case iceberg.Int32Type:
		return arrow.PrimitiveTypes.Int32, true
	case iceberg.Int64Type:
		return arrow.PrimitiveTypes.Int64, true
	case iceberg.Float32Type:
		return arrow.PrimitiveTypes.Float32, true
	case iceberg.Float64Type:
		return arrow.PrimitiveTypes.Float64, true
	case iceberg.StringType:
		return arrow.BinaryTypes.String, true
	case iceberg.DateType:
		return arrow.FixedWidthTypes.Date32, true
	case iceberg.TimestampType:
		return arrowTimestamp, true
	case iceberg.TimestampTzType:
		return arrowTimestampTz, true
	}
	return nil, false
}

// rowEncoder appends structured messages to an Arrow record batch with the
// table's columns. Message fields are matched to columns by name.
type rowEncoder struct {
	cols          []column
	index         map[string]int
	builder       *array.RecordBuilder
	rejectUnknown bool
}

// newRowEncoder returns an encoder for the table schema. It fails if the
// table has a column of a type the output can't write.
func newRowEncoder(mem memory.Allocator, schema *iceberg.Schema, rejectUnknown bool) (*rowEncoder, error) {
	fields := schema.Fields()
	e := &rowEncoder{
		cols:          make([]column, 0, len(fields)),
		index:         make(map[string]int, len(fields)),
		rejectUnknown: rejectUnknown,
	}
	arrowFields := make([]arrow.Field, 0, len(fields))
	for i, f := range fields {
		at, ok := arrowTypeOf(f.Type)
		if !ok {
			return nil, fmt.Errorf("column %q has type %s, which this output can't write", f.Name, f.Type)
		}
		e.cols = append(e.cols, column{name: f.Name, typ: f.Type, required: f.Required})
		e.index[f.Name] = i
		arrowFields = append(arrowFields, arrow.Field{Name: f.Name, Type: at, Nullable: !f.Required})
	}
	e.builder = array.NewRecordBuilder(mem, arrow.NewSchema(arrowFields, nil))
	return e, nil
}

// Append adds one row. On error nothing is appended, so a bad message never
// leaves a half-written row behind.
func (e *rowEncoder) Append(row map[string]any) error {
	if e.rejectUnknown {
		for name := range row {
			if _, ok := e.index[name]; !ok {
				return fmt.Errorf("field %s: %w", name, errUnknownField)
			}
		}
	}

	values := make([]any, len(e.cols))
	for i, col := range e.cols {
		raw, ok := row[col.name]
		if !ok || raw == nil {
			if col.required {
				return fmt.Errorf("field %s: required column has no value", col.name)
			}
			continue
		}
		v, err := convertValue(col.typ, raw)
		if err != nil {
			return fmt.Errorf("field %s: %w", col.name, err)
		}
		values[i] = v
	}

	for i := range e.cols {
		fb := e.builder.Field(i)
		switch v := values[i].(type) {
		case nil:
			fb.AppendNull()
		case bool:
			fb.(*array.BooleanBuilder).Append(v)
		case int32:
			fb.(*array.Int32Builder).Append(v)
		case int64:
			fb.(*array.Int64Builder).Append(v)
		case float32:
			fb.(*array.Float32Builder).Append(v)
		case float64:
			fb.(*array.Float64Builder).Append(v)
		case string:
			fb.(*array.StringBuilder).Append(v)
		case arrow.Date32:
			fb.(*array.Date32Builder).Append(v)
		case time.Time:
			fb.(*array.TimestampBuilder).Append(arrow.Timestamp(v.UnixMicro()))
		}
	}
	return nil
}

// NewRecordBatch returns the rows appended so far and resets the encoder.
// The caller owns the returned batch and must Release it.
func (e *rowEncoder) NewRecordBatch() arrow.RecordBatch {
	return e.builder.NewRecordBatch()
}

func (e *rowEncoder) Release() {
	e.builder.Release()
}

// convertValue converts a message value to the Go value appended for a column
// of type t. It accepts the types structured messages hold natively as well as
// those produced by parsing JSON (float64, json.Number, RFC 3339 strings).
func convertValue(t iceberg.Type, raw any) (any, error) {
	switch t.(type) {
	case iceberg.BooleanType:
		switch v := raw.(type) {
		case bool:
			return v, nil
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("expected boolean, got %q", v)
			}
			return b, nil
		}
	case iceberg.Int32Type:
		n, err := toInt64(raw)
		if err != nil {
			return nil, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, fmt.Errorf("integer %d out of 32-bit range", n)
		}
		return int32(n), nil
	case iceberg.Int64Type:
		return toInt64(raw)
	case iceberg.Float32Type:
		f, err := toFloat64(raw)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case iceberg.Float64Type:
		return toFloat64(raw)
	case iceberg.StringType:
		switch v := raw.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		case json.Number:
			return v.String(), nil
		case bool, int, int32, int64, uint, uint32, uint64, float32, float64:
			return fmt.Sprint(v), nil
		case time.Time:
			return v.Format(time.RFC3339Nano), nil
		}
		// Objects and arrays are stored as JSON.
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("cannot serialize %T to JSON: %w", raw, err)
		}
		return string(b), nil
	case iceberg.DateType:
		ts, err := toTime(raw, "2006-01-02")
		if err != nil {
			return nil, err
		}
		return arrow.Date32FromTime(ts), nil
	case iceberg.TimestampTzType:
		ts, err := toTime(raw)
		if err != nil {
			return nil, err
		}
		return ts.UTC(), nil
	case iceberg.TimestampType:
		// A timestamp without zone keeps its wall-clock reading.
		ts, err := toTime(raw, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999")
		if err != nil {
			return nil, err
		}
		_, offset := ts.Zone()
		return ts.Add(time.Duration(offset) * time.Second).UTC(), nil
	}
	return nil, fmt.Errorf("unexpected type %T", raw)
}

func toInt64(raw any) (int64, error) {
	switch v := raw.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint32:
		return int64(v), nil
	case float64:
		if v != math.Trunc(v) || v < math.MinInt64 || v > math.MaxInt64 {
			return 0, fmt.Errorf("expected integer, got %v", v)
		}
		return int64(v), nil
	case json.Number:
		i, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("expected integer, got %q", v.String())
		}
		return i, nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("expected integer, got %q", v)
		}
		return i, nil
	}
	return 0, fmt.Errorf("expected integer, got %T", raw)
}

func toFloat64(raw any) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("expected number, got %q", v.String())
		}
		return f, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("expected number, got %q", v)
		}
		return f, nil
	}
	return 0, fmt.Errorf("expected number, got %T", raw)
}

// toTime accepts a time.Time, or a string in RFC 3339 or one of extraLayouts.
func toTime(raw any, extraLayouts ...string) (time.Time, error) {
	switch v := raw.(type) {
	case time.Time:
		return v, nil
	case string:
		for _, layout := range append([]string{time.RFC3339Nano}, extraLayouts...) {
			if t, err := time.Parse(layout, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("expected RFC 3339 timestamp, got %q", v)
	}
	return time.Time{}, fmt.Errorf("expected timestamp, got %T", raw)
}
