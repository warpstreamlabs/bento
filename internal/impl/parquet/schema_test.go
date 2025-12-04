package parquet

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateStructTypeAsPtrs(t *testing.T) {
	randSource = rand.New(rand.NewSource(42))
	t.Cleanup(func() {
		// Resets after test complete
		rand.New(rand.NewSource(time.Now().UnixNano()))
	})

	tests := []struct {
		name       string
		yaml       string
		wantFields map[string]struct {
			fieldType reflect.Type
			tag       string
		}
		wantErr bool
	}{
		{
			name: "basic types",
			yaml: `
schema:
  - { name: str, type: UTF8 }
  - { name: num, type: INT64 }
  - { name: smallNum, type: INT16 }
  - { name: tinyNum, type: INT8 }
  - { name: flt, type: FLOAT }
  - { name: bool, type: BOOLEAN }
  - { name: dec32, type: DECIMAL32, decimal_precision: 3}
  - { name: dec64, type: DECIMAL64, decimal_scale: 4, decimal_precision: 10}  
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Str":      {reflect.TypeOf(""), `parquet:"str" json:"str"`},
				"Num":      {reflect.TypeOf(int64(0)), `parquet:"num" json:"num"`},
				"SmallNum": {reflect.TypeOf(int16(0)), `parquet:"smallNum" json:"smallNum"`},
				"TinyNum":  {reflect.TypeOf(int8(0)), `parquet:"tinyNum" json:"tinyNum"`},
				"Flt":      {reflect.TypeOf(float32(0)), `parquet:"flt" json:"flt"`},
				"Bool":     {reflect.TypeOf(false), `parquet:"bool" json:"bool"`},
				"Dec32":    {reflect.TypeOf(int32(0)), `parquet:"dec32,decimal(0:3)" json:"dec32"`},
				"Dec64":    {reflect.TypeOf(int64(0)), `parquet:"dec64,decimal(4:10)" json:"dec64"`},
			},
		},
		{
			name: "nested struct",
			yaml: `
schema:
  - name: nested
    fields:
      - { name: a, type: UTF8 }
      - { name: b, type: INT64 }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Nested": {
					fieldType: reflect.StructOf([]reflect.StructField{
						{Name: "A", Type: reflect.TypeOf(""), Tag: `parquet:"a" json:"a"`},
						{Name: "B", Type: reflect.TypeOf(int64(0)), Tag: `parquet:"b" json:"b"`},
					}),
					tag: `parquet:"nested" json:"nested"`,
				},
			},
		},
		{
			name: "optional and repeated",
			yaml: `
schema:
  - { name: req, type: INT64 }
  - { name: opt, type: INT64, optional: true }
  - { name: arr, type: FLOAT, repeated: true }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Req": {reflect.TypeOf(int64(0)), `parquet:"req" json:"req"`},
				"Opt": {reflect.PointerTo(reflect.TypeOf(int64(0))), `parquet:"opt" json:"opt"`},
				"Arr": {reflect.SliceOf(reflect.TypeOf(float32(0))), `parquet:"arr" json:"arr"`},
			},
		},
		{
			name: "whacky naming",
			yaml: `
schema:
  - { name: _timestamp, type: UTF8 }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				// deterministic random field name
				"HRUKPTTUEZPTNEU": {reflect.TypeOf(""), `parquet:"_timestamp" json:"_timestamp"`},
			},
		},
		{
			name: "map and list types",
			yaml: `
schema:
  - name: mymap1
    type: MAP
    fields:
      - { name: key, type: UTF8 }
      - { name: value, type: INT64 }
  - name: mymap2
    type: MAP
    optional: true
    fields:
      - { name: key, type: UTF8 }
      - { name: value, type: INT64 }
  - name: mylist1
    type: LIST
    fields:
      - { name: element, type: UTF8 }
  - name: mylist2
    type: LIST
    fields:
      - { name: element, type: UTF8, optional: true }
  - name: mylist3
    type: LIST
    optional: true
    fields:
      - { name: element, type: UTF8, optional: true }
  - name: mylist4
    type: LIST
    optional: true
    fields:
      - { name: element, type: UTF8 }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Mymap1": {
					fieldType: reflect.MapOf(reflect.TypeOf(""), reflect.TypeOf(int64(0))),
					tag:       `parquet:"mymap1" json:"mymap1"`,
				},
				"Mymap2": {
					fieldType: reflect.PointerTo(reflect.MapOf(reflect.TypeOf(""), reflect.TypeOf(int64(0)))),
					tag:       `parquet:"mymap2" json:"mymap2"`,
				},
				"Mylist1": {
					fieldType: reflect.SliceOf(reflect.TypeOf("")),
					tag:       `parquet:"mylist1,list" json:"mylist1"`,
				},
				"Mylist2": {
					fieldType: reflect.SliceOf(reflect.PointerTo(reflect.TypeOf(""))),
					tag:       `parquet:"mylist2,list" json:"mylist2"`,
				},
				"Mylist3": {
					fieldType: reflect.PointerTo(reflect.SliceOf(reflect.PointerTo(reflect.TypeOf("")))),
					tag:       `parquet:"mylist3,list" json:"mylist3"`,
				},
				"Mylist4": {
					fieldType: reflect.PointerTo(reflect.SliceOf(reflect.TypeOf(""))),
					tag:       `parquet:"mylist4,list" json:"mylist4"`,
				},
			},
		},
		{
			name: "complex example",
			yaml: `
schema:
  - name: testmap
    type: MAP
    fields:
      - { name: key, type: UTF8 }
      - { name: value, type: FLOAT }
  - { name: id, type: INT64 }
  - { name: as, type: FLOAT, repeated: true }
  - { name: g, type: INT64, optional: true }
  - { name: h, type: INT16, optional: true }
  - name: withchild
    optional: true
    fields:
      - { name: a_stuff, type: UTF8 }
      - { name: b_stuff, type: BOOLEAN }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Testmap": {
					fieldType: reflect.MapOf(reflect.TypeOf(""), reflect.TypeOf(float32(0))),
					tag:       `parquet:"testmap" json:"testmap"`,
				},
				"Id": {
					fieldType: reflect.TypeOf(int64(0)),
					tag:       `parquet:"id" json:"id"`,
				},
				"As": {
					fieldType: reflect.SliceOf(reflect.TypeOf(float32(0))),
					tag:       `parquet:"as" json:"as"`,
				},
				"G": {
					fieldType: reflect.PointerTo(reflect.TypeOf(int64(0))),
					tag:       `parquet:"g" json:"g"`,
				},
				"H": {
					fieldType: reflect.PointerTo(reflect.TypeOf(int16(0))),
					tag:       `parquet:"h" json:"h"`,
				},
				"Withchild": {
					fieldType: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
						{Name: "A_stuff", Type: reflect.TypeOf(""), Tag: `parquet:"a_stuff" json:"a_stuff"`},
						{Name: "B_stuff", Type: reflect.TypeOf(false), Tag: `parquet:"b_stuff" json:"b_stuff"`},
					})),
					tag: `parquet:"withchild" json:"withchild"`,
				},
			},
		},
		{
			name: "error: invalid map, missing value type",
			yaml: `
schema:
  - name: mymap
    type: MAP
    fields:
      - { name: key, type: INT64 }
`,
			wantErr: true,
		},
		{
			name: "explicit STRUCT type",
			yaml: `
schema:
  - name: cloud
    type: STRUCT
    optional: true
    fields:
      - { name: provider, type: UTF8 }
      - { name: region, type: UTF8, optional: true }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Cloud": {
					fieldType: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
						{Name: "Provider", Type: reflect.TypeOf(""), Tag: `parquet:"provider" json:"provider"`},
						{Name: "Region", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: `parquet:"region" json:"region"`},
					})),
					tag: `parquet:"cloud" json:"cloud"`,
				},
			},
		},
		{
			name: "nested STRUCT types",
			yaml: `
schema:
  - name: cloud
    type: STRUCT
    optional: true
    fields:
      - { name: provider, type: UTF8 }
      - { name: region, type: UTF8, optional: true }
      - name: account
        type: STRUCT
        optional: true
        fields:
          - { name: uid, type: UTF8, optional: true }
          - { name: name, type: UTF8, optional: true }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Cloud": {
					fieldType: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
						{Name: "Provider", Type: reflect.TypeOf(""), Tag: `parquet:"provider" json:"provider"`},
						{Name: "Region", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: `parquet:"region" json:"region"`},
						{Name: "Account", Type: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
							{Name: "Uid", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: `parquet:"uid" json:"uid"`},
							{Name: "Name", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: `parquet:"name" json:"name"`},
						})), Tag: `parquet:"account" json:"account"`},
					})),
					tag: `parquet:"cloud" json:"cloud"`,
				},
			},
		},
		{
			name: "STRUCT with complex nested types",
			yaml: `
schema:
  - name: metadata
    type: STRUCT
    optional: true
    fields:
      - { name: version, type: UTF8 }
      - name: profiles
        type: LIST
        optional: true
        fields:
          - { name: element, type: UTF8 }
      - name: product
        type: STRUCT
        optional: true
        fields:
          - { name: name, type: UTF8 }
          - { name: version, type: UTF8, optional: true }
`,
			wantFields: map[string]struct {
				fieldType reflect.Type
				tag       string
			}{
				"Metadata": {
					fieldType: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
						{Name: "Version", Type: reflect.TypeOf(""), Tag: `parquet:"version" json:"version"`},
						{Name: "Profiles", Type: reflect.PointerTo(reflect.SliceOf(reflect.TypeOf(""))), Tag: `parquet:"profiles,list" json:"profiles"`},
						{Name: "Product", Type: reflect.PointerTo(reflect.StructOf([]reflect.StructField{
							{Name: "Name", Type: reflect.TypeOf(""), Tag: `parquet:"name" json:"name"`},
							{Name: "Version", Type: reflect.PointerTo(reflect.TypeOf("")), Tag: `parquet:"version" json:"version"`},
						})), Tag: `parquet:"product" json:"product"`},
					})),
					tag: `parquet:"metadata" json:"metadata"`,
				},
			},
		},
		{
			name: "error: STRUCT without fields",
			yaml: `
schema:
  - name: mystruct
    type: STRUCT
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parquetEncodeProcessorConfig().ParseYAML(tt.yaml, nil)
			require.NoError(t, err)
			got, err := GenerateStructType(config, schemaOpts{optionalAsPtrs: true})
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateStructType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.Kind() != reflect.Struct {
				t.Errorf("GenerateStructType() returned non-struct type: %v", got)
				return
			}

			for i := 0; i < got.NumField(); i++ {
				field := got.Field(i)
				want, ok := tt.wantFields[field.Name]
				if !ok {
					t.Errorf("unexpected field: %s", field.Name)
					continue
				}

				if field.Type != want.fieldType {
					t.Errorf("field %s: got type %v, want %v", field.Name, field.Type, want.fieldType)
				}
				if string(field.Tag) != want.tag {
					t.Errorf("field %s: got tag %q, want %q", field.Name, field.Tag, want.tag)
				}
			}

			if got.NumField() != len(tt.wantFields) {
				t.Errorf("wrong number of fields: got %d, want %d", got.NumField(), len(tt.wantFields))
			}
		})
	}
}
