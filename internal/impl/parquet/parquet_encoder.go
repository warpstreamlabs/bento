package parquet

import (
	"fmt"
	"reflect"

	"github.com/parquet-go/parquet-go"
)

type parquetEncoder interface {
	Encode(contents map[string]any) (any, error)
}

//------------------------------------------------------------------------------

type nodeEncoder struct {
	schema *parquet.Schema
}

func (n *nodeEncoder) Encode(contents map[string]any) (any, error) {
	return transformDataWithSchemaV2(contents, n.schema.Fields()...)
}

//------------------------------------------------------------------------------

type reflectionEncoder struct {
	messageType reflect.Type
}

func (r *reflectionEncoder) Encode(contents map[string]any) (any, error) {
	v := reflect.New(r.messageType)

	if err := MapToStruct(contents, v.Interface()); err != nil {
		return nil, fmt.Errorf("conversion to struct failed, err: %w", err)
	}

	return v.Interface(), nil
}
