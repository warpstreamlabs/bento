package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/gabs/v2"
	jsonschema "github.com/xeipuuv/gojsonschema"

	"github.com/warpstreamlabs/bento/internal/value"
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"all",
		"Checks each element of an array against a query and returns true if all elements passed. An error occurs if the target is not an array, or if any element results in the provided query returning a non-boolean result. Returns false if the target array is empty.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.all_over_21 = this.patrons.all(patron -> patron.age >= 21)`,
			`{"patrons":[{"id":"1","age":18},{"id":"2","age":23}]}`,
			`{"all_over_21":false}`,
			`{"patrons":[{"id":"1","age":45},{"id":"2","age":23}]}`,
			`{"all_over_21":true}`,
		),
	).Param(ParamQuery("test", "A test query to apply to each element.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		queryFn, err := args.FieldQuery("test")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			arr, ok := res.([]any)
			if !ok {
				return nil, value.NewTypeError(res, value.TArray)
			}
			if len(arr) == 0 {
				return false, nil
			}
			for i, v := range arr {
				res, err := queryFn.Exec(ctx.WithValue(v))
				if err != nil {
					return nil, fmt.Errorf("element %v: %w", i, err)
				}
				b, ok := res.(bool)
				if !ok {
					return nil, fmt.Errorf("element %v: %w", i, value.NewTypeError(res, value.TBool))
				}
				if !b {
					return false, nil
				}
			}
			return true, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"any",
		"Checks the elements of an array against a query and returns true if any element passes. An error occurs if the target is not an array, or if an element results in the provided query returning a non-boolean result. Returns false if the target array is empty.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.any_over_21 = this.patrons.any(patron -> patron.age >= 21)`,
			`{"patrons":[{"id":"1","age":18},{"id":"2","age":23}]}`,
			`{"any_over_21":true}`,
			`{"patrons":[{"id":"1","age":10},{"id":"2","age":12}]}`,
			`{"any_over_21":false}`,
		),
	).Param(ParamQuery("test", "A test query to apply to each element.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		queryFn, err := args.FieldQuery("test")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			arr, ok := res.([]any)
			if !ok {
				return nil, value.NewTypeError(res, value.TArray)
			}

			if len(arr) == 0 {
				return false, nil
			}

			for i, v := range arr {
				res, err := queryFn.Exec(ctx.WithValue(v))
				if err != nil {
					return nil, fmt.Errorf("element %v: %w", i, err)
				}
				b, ok := res.(bool)
				if !ok {
					return nil, fmt.Errorf("element %v: %w", i, value.NewTypeError(res, value.TBool))
				}
				if b {
					return true, nil
				}
			}

			return false, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"append",
		"Returns an array with new elements appended to the end.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root.foo = this.foo.append("and", "this")`,
			`{"foo":["bar","baz"]}`,
			`{"foo":["bar","baz","and","this"]}`,
		),
	).VariadicParams(),
	func(args *ParsedParams) (simpleMethod, error) {
		argsList := args.Raw()
		return func(res any, ctx FunctionContext) (any, error) {
			arr, ok := res.([]any)
			if !ok {
				return nil, value.NewTypeError(res, value.TArray)
			}
			copied := make([]any, 0, len(arr)+len(argsList))
			copied = append(copied, arr...)
			return append(copied, argsList...), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"collapse", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Collapse an array or object into an object of key/value pairs for each field, where the key is the full path of the structured field in dot path notation. Empty arrays an objects are ignored by default.",
		NewExampleSpec("",
			`root.result = this.collapse()`,
			`{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`,
			`{"result":{"foo.0.bar":"1","foo.2.bar":"2"}}`,
		),
		NewExampleSpec(
			"An optional boolean parameter can be set to true in order to include empty objects and arrays.",
			`root.result = this.collapse(include_empty: true)`,
			`{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`,
			`{"result":{"foo.0.bar":"1","foo.1.bar":{},"foo.2.bar":"2","foo.3.bar":[]}}`,
		),
	).Param(ParamBool("include_empty", "Whether to include empty objects and arrays in the resulting object.").Default(false)),
	func(args *ParsedParams) (simpleMethod, error) {
		includeEmpty, err := args.FieldBool("include_empty")
		if err != nil {
			return nil, err
		}
		return func(v any, ctx FunctionContext) (any, error) {
			gObj := gabs.Wrap(v)
			if includeEmpty {
				return gObj.FlattenIncludeEmpty()
			}
			return gObj.Flatten()
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"contains", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Checks whether an array contains an element matching the argument, or an object contains a value matching the argument, and returns a boolean result. Numerical comparisons are made irrespective of the representation type (float versus integer).",
		NewExampleSpec("",
			`root.has_foo = this.thing.contains("foo")`,
			`{"thing":["this","foo","that"]}`,
			`{"has_foo":true}`,
			`{"thing":["this","bar","that"]}`,
			`{"has_foo":false}`,
		),
		NewExampleSpec("",
			`root.has_bar = this.thing.contains(20)`,
			`{"thing":[10.3,20.0,"huh",3]}`,
			`{"has_bar":true}`,
			`{"thing":[2,3,40,67]}`,
			`{"has_bar":false}`,
		),
	).InCategory(
		MethodCategoryStrings,
		"Checks whether a string contains a substring and returns a boolean result.",
		NewExampleSpec("",
			`root.has_foo = this.thing.contains("foo")`,
			`{"thing":"this foo that"}`,
			`{"has_foo":true}`,
			`{"thing":"this bar that"}`,
			`{"has_foo":false}`,
		),
	).Param(ParamAny("value", "A value to test against elements of the target.")),
	func(args *ParsedParams) (simpleMethod, error) {
		compareRight, err := args.Field("value")
		if err != nil {
			return nil, err
		}
		sub := value.IToString(compareRight)
		bsub := value.IToBytes(compareRight)
		return func(v any, ctx FunctionContext) (any, error) {
			switch t := v.(type) {
			case string:
				return strings.Contains(t, sub), nil
			case []byte:
				return bytes.Contains(t, bsub), nil
			case []any:
				for _, compareLeft := range t {
					if value.ICompare(compareRight, compareLeft) {
						return true, nil
					}
				}
			case map[string]any:
				for _, compareLeft := range t {
					if value.ICompare(compareRight, compareLeft) {
						return true, nil
					}
				}
			default:
				return nil, value.NewTypeError(v, value.TString, value.TArray, value.TObject)
			}
			return false, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"enumerated",
		"Converts an array into a new array of objects, where each object has a field index containing the `index` of the element and a field `value` containing the original value of the element.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.foo = this.foo.enumerated()`,
			`{"foo":["bar","baz"]}`,
			`{"foo":[{"index":0,"value":"bar"},{"index":1,"value":"baz"}]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}
			enumerated := make([]any, 0, len(arr))
			for i, ele := range arr {
				enumerated = append(enumerated, map[string]any{
					"index": int64(i),
					"value": ele,
				})
			}
			return enumerated, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"exists",
		"Checks that a field, identified via a [dot path][field_paths], exists in an object.",
		NewExampleSpec("",
			`root.result = this.foo.exists("bar.baz")`,
			`{"foo":{"bar":{"baz":"yep, I exist"}}}`,
			`{"result":true}`,
			`{"foo":{"bar":{}}}`,
			`{"result":false}`,
			`{"foo":{}}`,
			`{"result":false}`,
		),
	).Param(ParamString("path", "A [dot path][field_paths] to a field.")),
	func(args *ParsedParams) (simpleMethod, error) {
		pathStr, err := args.FieldString("path")
		if err != nil {
			return nil, err
		}
		path := gabs.DotPathToSlice(pathStr)
		return func(v any, ctx FunctionContext) (any, error) {
			return gabs.Wrap(v).Exists(path...), nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"explode", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Explodes an array or object at a [field path][field_paths].",
		NewExampleSpec(`##### On arrays

Exploding arrays results in an array containing elements matching the original document, where the target field of each element is an element of the exploded array:`,
			`root = this.explode("value")`,
			`{"id":1,"value":["foo","bar","baz"]}`,
			`[{"id":1,"value":"foo"},{"id":1,"value":"bar"},{"id":1,"value":"baz"}]`,
		),
		NewExampleSpec(`##### On objects

Exploding objects results in an object where the keys match the target object, and the values match the original document but with the target field replaced by the exploded value:`,
			`root = this.explode("value")`,
			`{"id":1,"value":{"foo":2,"bar":[3,4],"baz":{"bev":5}}}`,
			`{"bar":{"id":1,"value":[3,4]},"baz":{"id":1,"value":{"bev":5}},"foo":{"id":1,"value":2}}`,
		),
	).Param(ParamString("path", "A [dot path][field_paths] to a field to explode.")),
	func(args *ParsedParams) (simpleMethod, error) {
		pathRaw, err := args.FieldString("path")
		if err != nil {
			return nil, err
		}
		path := gabs.DotPathToSlice(pathRaw)
		return func(v any, ctx FunctionContext) (any, error) {
			rootMap, ok := v.(map[string]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TObject)
			}

			target := gabs.Wrap(v).Search(path...)
			copyFrom := mapWithout(rootMap, [][]string{path})

			switch t := target.Data().(type) {
			case []any:
				result := make([]any, len(t))
				for i, ele := range t {
					gExploded := gabs.Wrap(value.IClone(copyFrom))
					_, _ = gExploded.Set(ele, path...)
					result[i] = gExploded.Data()
				}
				return result, nil
			case map[string]any:
				result := make(map[string]any, len(t))
				for key, ele := range t {
					gExploded := gabs.Wrap(value.IClone(copyFrom))
					_, _ = gExploded.Set(ele, path...)
					result[key] = gExploded.Data()
				}
				return result, nil
			}

			return nil, fmt.Errorf("expected array or object value at path '%v', found: %v", pathRaw, value.ITypeOf(target.Data()))
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"filter", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Executes a mapping query argument for each element of an array or key/value pair of an object. If the query returns `false` the item is removed from the resulting array or object. The item will also be removed if the query returns any non-boolean value.",
		NewExampleSpec(``,
			`root.new_nums = this.nums.filter(num -> num > 10)`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[11,17]}`,
		),
		NewExampleSpec(`##### On objects

When filtering objects the mapping query argument is provided a context with a field `+"`key`"+` containing the value key, and a field `+"`value`"+` containing the value.`,
			`root.new_dict = this.dict.filter(item -> item.value.contains("foo"))`,
			`{"dict":{"first":"hello foo","second":"world","third":"this foo is great"}}`,
			`{"new_dict":{"first":"hello foo","third":"this foo is great"}}`,
		),
	).Param(ParamQuery("test", "A query to apply to each element, if this query resolves to any value other than a boolean `true` the element will be removed from the result.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		mapFn, err := args.FieldQuery("test")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			var resValue any
			switch t := res.(type) {
			case []any:
				newSlice := make([]any, 0, len(t))
				for _, v := range t {
					f, err := mapFn.Exec(ctx.WithValue(v))
					if err != nil {
						return nil, err
					}
					if b, _ := f.(bool); b {
						newSlice = append(newSlice, v)
					}
				}
				resValue = newSlice
			case map[string]any:
				newMap := make(map[string]any, len(t))
				for k, v := range t {
					var ctxMap any = map[string]any{
						"key":   k,
						"value": v,
					}
					f, err := mapFn.Exec(ctx.WithValue(ctxMap))
					if err != nil {
						return nil, err
					}
					if b, _ := f.(bool); b {
						newMap[k] = v
					}
				}
				resValue = newMap
			default:
				return nil, value.NewTypeError(res, value.TArray, value.TObject)
			}
			return resValue, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"find",
		"Returns the index of the first occurrence of a value in an array. `-1` is returned if there are no matches. Numerical comparisons are made irrespective of the representation type (float versus integer).",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.index = this.find("bar")`,
			`["foo", "bar", "baz"]`,
			`{"index":1}`,
		),
		NewExampleSpec("",
			`root.index = this.things.find(this.goal)`,
			`{"goal":"bar","things":["foo", "bar", "baz"]}`,
			`{"index":1}`,
		),
	).Beta().Param(ParamAny("value", "A value to find.")),
	func(args *ParsedParams) (simpleMethod, error) {
		val, err := args.Field("value")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			array, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}

			for i, elem := range array {
				if value.ICompare(val, elem) {
					return i, nil
				}
			}
			return -1, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"find_all",
		"Returns an array containing the indexes of all occurrences of a value in an array. An empty array is returned if there are no matches. Numerical comparisons are made irrespective of the representation type (float versus integer).",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.index = this.find_all("bar")`,
			`["foo", "bar", "baz", "bar"]`,
			`{"index":[1,3]}`,
		),
		NewExampleSpec("",
			`root.indexes = this.things.find_all(this.goal)`,
			`{"goal":"bar","things":["foo", "bar", "baz", "bar", "buz"]}`,
			`{"indexes":[1,3]}`,
		),
	).Beta().Param(ParamAny("value", "A value to find.")),
	func(args *ParsedParams) (simpleMethod, error) {
		val, err := args.Field("value")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			array, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}

			output := []any{}
			for i, elem := range array {
				if value.ICompare(val, elem) {
					output = append(output, i)
				}
			}

			return output, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"find_by",
		"Returns the index of the first occurrence of an array where the provided query resolves to a boolean `true`. `-1` is returned if there are no matches.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.index = this.find_by(v -> v != "bar")`,
			`["foo", "bar", "baz"]`,
			`{"index":0}`,
		),
	).Beta().Param(ParamQuery("query", "A query to execute for each element.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		queryFn, err := args.FieldQuery("query")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			array, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}

			for i, elem := range array {
				iIsMatch, err := queryFn.Exec(ctx.WithValue(elem))
				if err != nil {
					return nil, fmt.Errorf("query returned an error for index %v: %w", i, err)
				}
				isMatch, ok := iIsMatch.(bool)
				if !ok {
					return nil, fmt.Errorf("query returned a non-boolean value for index %v: %w", i, value.NewTypeError(iIsMatch, value.TBool))
				}
				if isMatch {
					return i, nil
				}
			}
			return -1, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"find_all_by",
		"Returns an array containing the indexes of all occurrences of an array where the provided query resolves to a boolean `true`. An empty array is returned if there are no matches. Numerical comparisons are made irrespective of the representation type (float versus integer).",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.index = this.find_all_by(v -> v != "bar")`,
			`["foo", "bar", "baz"]`,
			`{"index":[0,2]}`,
		),
	).Beta().Param(ParamQuery("query", "A query to execute for each element.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		queryFn, err := args.FieldQuery("query")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			array, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}

			output := []any{}
			for i, elem := range array {
				iIsMatch, err := queryFn.Exec(ctx.WithValue(elem))
				if err != nil {
					return nil, fmt.Errorf("query returned an error for index %v: %w", i, err)
				}
				isMatch, ok := iIsMatch.(bool)
				if !ok {
					return nil, fmt.Errorf("query returned a non-boolean value for index %v: %w", i, value.NewTypeError(iIsMatch, value.TBool))
				}
				if isMatch {
					output = append(output, i)
				}
			}

			return output, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"flatten",
		"Iterates an array and any element that is itself an array is removed and has its elements inserted directly in the resulting array.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root.result = this.flatten()`,
			`["foo",["bar","baz"],"buz"]`,
			`{"result":["foo","bar","baz","buz"]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			array, isArray := v.([]any)
			if !isArray {
				return nil, value.NewTypeError(v, value.TArray)
			}
			result := make([]any, 0, len(array))
			for _, child := range array {
				switch t := child.(type) {
				case []any:
					result = append(result, t...)
				default:
					result = append(result, t)
				}
			}
			return result, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"fold",
		"Takes two arguments: an initial value, and a mapping query. For each element of an array the mapping context is an object with two fields `tally` and `value`, where `tally` contains the current accumulated value and `value` is the value of the current element. The mapping must return the result of adding the value to the tally.\n\nThe first argument is the value that `tally` will have on the first call.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root.sum = this.foo.fold(0, item -> item.tally + item.value)`,
			`{"foo":[3,8,11]}`,
			`{"sum":22}`,
		),
		NewExampleSpec(``,
			`root.result = this.foo.fold("", item -> "%v%v".format(item.tally, item.value))`,
			`{"foo":["hello ", "world"]}`,
			`{"result":"hello world"}`,
		),
		NewExampleSpec(`You can use fold to merge an array of objects together:`,
			`root.smoothie = this.fruits.fold({}, item -> item.tally.merge(item.value))`,
			`{"fruits":[{"apple":5},{"banana":3},{"orange":8}]}`,
			`{"smoothie":{"apple":5,"banana":3,"orange":8}}`,
		),
	).
		Param(ParamAny("initial", "The initial value to start the fold with. For example, an empty object `{}`, a zero count `0`, or an empty string `\"\"`.")).
		Param(ParamQuery("query", "A query to apply for each element. The query is provided an object with two fields; `tally` containing the current tally, and `value` containing the value of the current element. The query should result in a new tally to be passed to the next element query.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		foldTallyStart, err := args.Field("initial")
		if err != nil {
			return nil, err
		}
		foldFn, err := args.FieldQuery("query")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			resArray, ok := res.([]any)
			if !ok {
				return nil, value.NewTypeError(res, value.TArray)
			}

			tally := value.IClone(foldTallyStart)
			for _, v := range resArray {
				newV, mapErr := foldFn.Exec(ctx.WithValue(map[string]any{
					"tally": tally,
					"value": v,
				}))
				if mapErr != nil {
					return nil, mapErr
				}
				tally = newV
			}
			return tally, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"index",
		"Extract an element from an array by an index. The index can be negative, and if so the element will be selected from the end counting backwards starting from -1. E.g. an index of -1 returns the last element, an index of -2 returns the element before the last, and so on.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.last_name = this.names.index(-1)`,
			`{"names":["rachel","stevens"]}`,
			`{"last_name":"stevens"}`,
		),
		NewExampleSpec("It is also possible to use this method on byte arrays, in which case the selected element will be returned as an integer.",
			`root.last_byte = this.name.bytes().index(-1)`,
			`{"name":"foobar bazson"}`,
			`{"last_byte":110}`,
		),
	).Param(ParamInt64("index", "The index to obtain from an array.")),
	func(args *ParsedParams) (simpleMethod, error) {
		index, err := args.FieldInt64("index")
		if err != nil {
			return nil, err
		}
		return func(v any, ctx FunctionContext) (any, error) {
			switch array := v.(type) {
			case []any:
				i := int(index)
				if i < 0 {
					i = len(array) + i
				}
				if i < 0 || i >= len(array) {
					return nil, fmt.Errorf("index '%v' was out of bounds for array size: %v", i, len(array))
				}
				return array[i], nil
			case []byte:
				i := int(index)
				if i < 0 {
					i = len(array) + i
				}
				if i < 0 || i >= len(array) {
					return nil, fmt.Errorf("index '%v' was out of bounds for array size: %v", i, len(array))
				}
				return int64(array[i]), nil
			default:
				return nil, value.NewTypeError(v, value.TArray)
			}
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"json_schema",
		"Checks a [JSON schema](https://json-schema.org/) against a value and returns the value if it matches or throws and error if it does not.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"",
		NewExampleSpec("",
			`root = this.json_schema("""{
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}""")`,
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			`{"foo":5}`,
			`Error("failed assignment (line 1): field `+"`this`"+`: foo invalid type. expected: string, given: integer")`,
		),
		NewExampleSpec(
			"In order to load a schema from a file use the `file` function.",
			`root = this.json_schema(file(env("BENTO_TEST_BLOBLANG_SCHEMA_FILE")))`,
		),
	).Beta().Param(ParamString("schema", "The schema to check values against.")),
	func(args *ParsedParams) (simpleMethod, error) {
		schemaStr, err := args.FieldString("schema")
		if err != nil {
			return nil, err
		}
		schema, err := jsonschema.NewSchema(jsonschema.NewStringLoader(schemaStr))
		if err != nil {
			return nil, fmt.Errorf("failed to parse json schema definition: %w", err)
		}
		return func(res any, ctx FunctionContext) (any, error) {
			result, err := schema.Validate(jsonschema.NewGoLoader(res))
			if err != nil {
				return nil, err
			}
			if !result.Valid() {
				var errStr string
				for i, desc := range result.Errors() {
					if i > 0 {
						errStr += "\n"
					}
					description := strings.ToLower(desc.Description())
					if property := desc.Details()["property"]; property != nil {
						description = property.(string) + strings.TrimPrefix(description, strings.ToLower(property.(string)))
					}
					errStr = errStr + desc.Field() + " " + description
				}
				return nil, errors.New(errStr)
			}
			return res, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"keys",
		"Returns the keys of an object as an array.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.foo_keys = this.foo.keys()`,
			`{"foo":{"bar":1,"baz":2}}`,
			`{"foo_keys":["bar","baz"]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			if m, ok := v.(map[string]any); ok {
				keys := make([]any, 0, len(m))
				for k := range m {
					keys = append(keys, k)
				}
				sort.Slice(keys, func(i, j int) bool {
					return keys[i].(string) < keys[j].(string)
				})
				return keys, nil
			}
			return nil, value.NewTypeError(v, value.TObject)
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"key_values",
		"Returns the key/value pairs of an object as an array, where each element is an object with a `key` field and a `value` field. The order of the resulting array will be random.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec("",
			`root.foo_key_values = this.foo.key_values().sort_by(pair -> pair.key)`,

			`{"foo":{"bar":1,"baz":2}}`,
			`{"foo_key_values":[{"key":"bar","value":1},{"key":"baz","value":2}]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			if m, ok := v.(map[string]any); ok {
				keyValues := make([]any, 0, len(m))
				for k, v := range m {
					keyValues = append(keyValues, map[string]any{
						"key":   k,
						"value": v,
					})
				}
				return keyValues, nil
			}
			return nil, value.NewTypeError(v, value.TObject)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"length", "",
	).InCategory(
		MethodCategoryStrings, "Returns the length of a string.",
		NewExampleSpec("",
			`root.foo_len = this.foo.length()`,
			`{"foo":"hello world"}`,
			`{"foo_len":11}`,
		),
	).InCategory(
		MethodCategoryObjectAndArray, "Returns the length of an array or object (number of keys).",
		NewExampleSpec("",
			`root.foo_len = this.foo.length()`,
			`{"foo":["first","second"]}`,
			`{"foo_len":2}`,
			`{"foo":{"first":"bar","second":"baz"}}`,
			`{"foo_len":2}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			var length int64
			switch t := v.(type) {
			case string:
				length = int64(len(t))
			case []byte:
				length = int64(len(t))
			case []any:
				length = int64(len(t))
			case map[string]any:
				length = int64(len(t))
			default:
				return nil, value.NewTypeError(v, value.TString, value.TArray, value.TObject)
			}
			return length, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"map_each", "",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(`##### On arrays

Apply a mapping to each element of an array and replace the element with the result. Within the argument mapping the context is the value of the element being mapped.`,
			`root.new_nums = this.nums.map_each(num -> if num < 10 {
  deleted()
} else {
  num - 10
})`,
			`{"nums":[3,11,4,17]}`,
			`{"new_nums":[1,7]}`,
		),
		NewExampleSpec(`##### On objects

Apply a mapping to each value of an object and replace the value with the result. Within the argument mapping the context is an object with a field `+"`key`"+` containing the value key, and a field `+"`value`"+`.`,
			`root.new_dict = this.dict.map_each(item -> item.value.uppercase())`,
			`{"dict":{"foo":"hello","bar":"world"}}`,
			`{"new_dict":{"bar":"WORLD","foo":"HELLO"}}`,
		),
	).Param(ParamQuery("query", "A query that will be used to map each element.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		mapFn, err := args.FieldQuery("query")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			var resValue any
			var err error
			switch t := res.(type) {
			case []any:
				newSlice := make([]any, 0, len(t))
				for i, v := range t {
					newV, mapErr := mapFn.Exec(ctx.WithValue(v))
					if mapErr != nil {
						return nil, fmt.Errorf("failed to process element %v: %w", i, ErrFrom(mapErr, mapFn))
					}
					switch newV.(type) {
					case value.Delete:
					case value.Nothing:
						newSlice = append(newSlice, v)
					default:
						newSlice = append(newSlice, newV)
					}
				}
				resValue = newSlice
			case map[string]any:
				newMap := make(map[string]any, len(t))
				for k, v := range t {
					var ctxMap any = map[string]any{
						"key":   k,
						"value": v,
					}
					newV, mapErr := mapFn.Exec(ctx.WithValue(ctxMap))
					if mapErr != nil {
						return nil, fmt.Errorf("failed to process element %v: %w", k, ErrFrom(mapErr, mapFn))
					}
					switch newV.(type) {
					case value.Delete:
					case value.Nothing:
						newMap[k] = v
					default:
						newMap[k] = newV
					}
				}
				resValue = newMap
			default:
				return nil, value.NewTypeError(res, value.TArray)
			}
			if err != nil {
				return nil, err
			}
			return resValue, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"map_each_key", "",
	).InCategory(
		MethodCategoryObjectAndArray, `Apply a mapping to each key of an object, and replace the key with the result, which must be a string.`,
		NewExampleSpec(``,
			`root.new_dict = this.dict.map_each_key(key -> key.uppercase())`,
			`{"dict":{"keya":"hello","keyb":"world"}}`,
			`{"new_dict":{"KEYA":"hello","KEYB":"world"}}`,
		),
		NewExampleSpec(``,
			`root = this.map_each_key(key -> if key.contains("kafka") { "_" + key })`,
			`{"amqp_key":"foo","kafka_key":"bar","kafka_topic":"baz"}`,
			`{"_kafka_key":"bar","_kafka_topic":"baz","amqp_key":"foo"}`,
		),
	).Param(ParamQuery("query", "A query that will be used to map each key.", false)),
	func(args *ParsedParams) (simpleMethod, error) {
		mapFn, err := args.FieldQuery("query")
		if err != nil {
			return nil, err
		}
		return func(res any, ctx FunctionContext) (any, error) {
			obj, ok := res.(map[string]any)
			if !ok {
				return nil, value.NewTypeError(res, value.TObject)
			}

			newMap := make(map[string]any, len(obj))
			for k, v := range obj {
				var ctxVal any = k
				newKey, mapErr := mapFn.Exec(ctx.WithValue(ctxVal))
				if mapErr != nil {
					return nil, mapErr
				}

				switch t := newKey.(type) {
				// TODO: Revise whether we want this.
				// case Delete:
				case value.Nothing:
					newMap[k] = v
				case string:
					newMap[t] = v
				default:
					return nil, fmt.Errorf("unexpected result from key mapping: %w", value.NewTypeError(newKey, value.TString))
				}
			}
			return newMap, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerMethod(
	NewMethodSpec(
		"merge", "Merge a source object into an existing destination object. When a collision is found within the merged structures (both a source and destination object contain the same non-object keys) the result will be an array containing both values, where values that are already arrays will be expanded into the resulting array. In order to simply override destination fields on collision use the [`assign`](#assign) method.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root = this.foo.merge(this.bar)`,
			`{"foo":{"first_name":"fooer","likes":"bars"},"bar":{"second_name":"barer","likes":"foos"}}`,
			`{"first_name":"fooer","likes":["bars","foos"],"second_name":"barer"}`,
		),
	).Param(ParamAny("with", "A value to merge the target value with.")),
	mergeMethod,
)

func mergeMethod(target Function, args *ParsedParams) (Function, error) {
	mergeFromSource, err := args.Field("with")
	if err != nil {
		return nil, err
	}
	return ClosureFunction("method merge", func(ctx FunctionContext) (any, error) {
		mergeInto, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		mergeFrom := value.IClone(mergeFromSource)
		if root, isArray := mergeInto.([]any); isArray {
			if rhs, isAlsoArray := mergeFrom.([]any); isAlsoArray {
				return append(root, rhs...), nil
			}
			return append(root, mergeFrom), nil
		}

		if _, isObject := mergeInto.(map[string]any); !isObject {
			return nil, value.NewTypeErrorFrom(target.Annotation(), mergeInto, value.TObject, value.TArray)
		}

		root := gabs.New()
		if err = root.Merge(gabs.Wrap(mergeInto)); err == nil {
			err = root.Merge(gabs.Wrap(mergeFrom))
		}
		if err != nil {
			return nil, err
		}
		return root.Data(), nil
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = registerMethod(
	NewMethodSpec(
		"assign", "Merge a source object into an existing destination object. When a collision is found within the merged structures (both a source and destination object contain the same non-object keys) the value in the destination object will be overwritten by that of source object. In order to preserve both values on collision use the [`merge`](#merge) method.",
	).InCategory(
		MethodCategoryObjectAndArray, "",
		NewExampleSpec(``,
			`root = this.foo.assign(this.bar)`,
			`{"foo":{"first_name":"fooer","likes":"bars"},"bar":{"second_name":"barer","likes":"foos"}}`,
			`{"first_name":"fooer","likes":"foos","second_name":"barer"}`,
		),
	).Param(ParamAny("with", "A value to merge the target value with.")),
	assignMethod,
)

func assignMethod(target Function, args *ParsedParams) (Function, error) {
	assignFromSource, err := args.Field("with")
	if err != nil {
		return nil, err
	}
	return ClosureFunction("method assign", func(ctx FunctionContext) (any, error) {
		assignInto, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}

		assignFrom := value.IClone(assignFromSource)
		if root, isArray := assignInto.([]any); isArray {
			if rhs, isAlsoArray := assignFrom.([]any); isAlsoArray {
				return append(root, rhs...), nil
			}
			return append(root, assignFrom), nil
		}

		if _, isObject := assignInto.(map[string]any); !isObject {
			return nil, value.NewTypeErrorFrom(target.Annotation(), assignInto, value.TObject, value.TArray)
		}

		root := gabs.New()
		if err = root.MergeFn(gabs.Wrap(assignInto), assigner); err == nil {
			err = root.MergeFn(gabs.Wrap(assignFrom), assigner)
		}
		if err != nil {
			return nil, err
		}
		return root.Data(), nil
	}, target.QueryTargets), nil
}

func assigner(destination, source any) any {
	return source
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"not_empty", "",
	).InCategory(
		MethodCategoryCoercion,
		"Ensures that the given string, array or object value is not empty, and if so returns it, otherwise an error is returned.",
		NewExampleSpec("",
			`root.a = this.a.not_empty()`,
			`{"a":"foo"}`,
			`{"a":"foo"}`,

			`{"a":""}`,
			`Error("failed assignment (line 1): field `+"`this.a`"+`: string value is empty")`,

			`{"a":["foo","bar"]}`,
			`{"a":["foo","bar"]}`,

			`{"a":[]}`,
			`Error("failed assignment (line 1): field `+"`this.a`"+`: array value is empty")`,

			`{"a":{"b":"foo","c":"bar"}}`,
			`{"a":{"b":"foo","c":"bar"}}`,

			`{"a":{}}`,
			`Error("failed assignment (line 1): field `+"`this.a`"+`: object value is empty")`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			switch t := v.(type) {
			case string:
				if t == "" {
					return nil, errors.New("string value is empty")
				}
			case []any:
				if len(t) == 0 {
					return nil, errors.New("array value is empty")
				}
			case map[string]any:
				if len(t) == 0 {
					return nil, errors.New("object value is empty")
				}
			default:
				return nil, value.NewTypeError(v, value.TString, value.TArray, value.TObject)
			}
			return v, nil
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerMethod(
	NewMethodSpec(
		"sort", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Attempts to sort the values of an array in increasing order. The type of all values must match in order for the ordering to succeed. Supports string and number values.",
		NewExampleSpec("",
			`root.sorted = this.foo.sort()`,
			`{"foo":["bbb","ccc","aaa"]}`,
			`{"sorted":["aaa","bbb","ccc"]}`,
		),
		NewExampleSpec("It's also possible to specify a mapping argument, which is provided an object context with fields `left` and `right`, the mapping must return a boolean indicating whether the `left` value is less than `right`. This allows you to sort arrays containing non-string or non-number values.",
			`root.sorted = this.foo.sort(item -> item.left.v < item.right.v)`,
			`{"foo":[{"id":"foo","v":"bbb"},{"id":"bar","v":"ccc"},{"id":"baz","v":"aaa"}]}`,
			`{"sorted":[{"id":"baz","v":"aaa"},{"id":"foo","v":"bbb"},{"id":"bar","v":"ccc"}]}`,
		),
	).
		Param(ParamQuery(
			"compare",
			"An optional query that should explicitly compare elements `left` and `right` and provide a boolean result.",
			false,
		).Optional()),
	sortMethod,
)

func sortMethod(target Function, args *ParsedParams) (Function, error) {
	compareFn := func(ctx FunctionContext, values []any, i, j int) (bool, error) {
		switch values[i].(type) {
		case float64, int, int64, uint64, json.Number:
			lhs, err := value.IGetNumber(values[i])
			if err != nil {
				return false, fmt.Errorf("sort element %v: %w", i, err)
			}
			rhs, err := value.IGetNumber(values[j])
			if err != nil {
				return false, fmt.Errorf("sort element %v: %w", j, err)
			}
			return lhs < rhs, nil
		case string, []byte:
			lhs, err := value.IGetString(values[i])
			if err != nil {
				return false, fmt.Errorf("sort element %v: %w", i, err)
			}
			rhs, err := value.IGetString(values[j])
			if err != nil {
				return false, fmt.Errorf("sort element %v: %w", j, err)
			}
			return lhs < rhs, nil
		}
		return false, fmt.Errorf("sort element %v: %w", i, value.NewTypeError(values[i], value.TNumber, value.TString))
	}

	mapFn, err := args.FieldOptionalQuery("compare")
	if err != nil {
		return nil, err
	}

	if mapFn != nil {
		compareFn = func(ctx FunctionContext, values []any, i, j int) (bool, error) {
			var ctxValue any = map[string]any{
				"left":  values[i],
				"right": values[j],
			}
			v, err := mapFn.Exec(ctx.WithValue(ctxValue))
			if err != nil {
				return false, err
			}
			b, ok := v.(bool)
			if !ok {
				return false, value.NewTypeErrorFrom("sort argument", v, value.TBool)
			}
			return b, nil
		}
	}

	targets := target.QueryTargets
	if mapFn != nil {
		targets = aggregateTargetPaths(target, mapFn)
	}

	return ClosureFunction("method sort", func(ctx FunctionContext) (any, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if m, ok := v.([]any); ok {
			values := make([]any, 0, len(m))
			values = append(values, m...)

			sort.Slice(values, func(i, j int) bool {
				if err == nil {
					var b bool
					b, err = compareFn(ctx, values, i, j)
					return b
				}
				return false
			})
			if err != nil {
				return nil, err
			}
			return values, nil
		}
		return nil, value.NewTypeErrorFrom(target.Annotation(), v, value.TArray)
	}, targets), nil
}

var _ = registerMethod(
	NewMethodSpec(
		"sort_by", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Attempts to sort the elements of an array, in increasing order, by a value emitted by an argument query applied to each element. The type of all values must match in order for the ordering to succeed. Supports string and number values.",
		NewExampleSpec("",
			`root.sorted = this.foo.sort_by(ele -> ele.id)`,
			`{"foo":[{"id":"bbb","message":"bar"},{"id":"aaa","message":"foo"},{"id":"ccc","message":"baz"}]}`,
			`{"sorted":[{"id":"aaa","message":"foo"},{"id":"bbb","message":"bar"},{"id":"ccc","message":"baz"}]}`,
		),
	).Param(ParamQuery("query", "A query to apply to each element that yields a value used for sorting.", false)),
	sortByMethod,
)

func sortByMethod(target Function, args *ParsedParams) (Function, error) {
	mapFn, err := args.FieldQuery("query")
	if err != nil {
		return nil, err
	}

	compareFn := func(ctx FunctionContext, values []any, i, j int) (bool, error) {
		var leftValue, rightValue any
		var err error

		if leftValue, err = mapFn.Exec(ctx.WithValue(values[i])); err != nil {
			return false, err
		}
		if rightValue, err = mapFn.Exec(ctx.WithValue(values[j])); err != nil {
			return false, err
		}

		switch leftValue.(type) {
		case float64, int, int64, uint64, json.Number:
			lhs, err := value.IGetNumber(leftValue)
			if err != nil {
				return false, fmt.Errorf("sort_by element %v: %w", i, ErrFrom(err, mapFn))
			}
			rhs, err := value.IGetNumber(rightValue)
			if err != nil {
				return false, fmt.Errorf("sort_by element %v: %w", j, ErrFrom(err, mapFn))
			}
			return lhs < rhs, nil
		case string, []byte:
			lhs, err := value.IGetString(leftValue)
			if err != nil {
				return false, fmt.Errorf("sort_by element %v: %w", i, ErrFrom(err, mapFn))
			}
			rhs, err := value.IGetString(rightValue)
			if err != nil {
				return false, fmt.Errorf("sort_by element %v: %w", j, ErrFrom(err, mapFn))
			}
			return lhs < rhs, nil
		}
		return false, fmt.Errorf("sort_by element %v: %w", i, ErrFrom(value.NewTypeError(leftValue, value.TNumber, value.TString), mapFn))
	}

	return ClosureFunction("method sort_by", func(ctx FunctionContext) (any, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if m, ok := v.([]any); ok {
			values := make([]any, 0, len(m))
			values = append(values, m...)

			sort.Slice(values, func(i, j int) bool {
				if err == nil {
					var b bool
					b, err = compareFn(ctx, values, i, j)
					return b
				}
				return false
			})
			if err != nil {
				return nil, err
			}
			return values, nil
		}
		return nil, value.NewTypeErrorFrom(target.Annotation(), v, value.TArray)
	}, aggregateTargetPaths(target, mapFn)), nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"slice", "consider bracket syntax for more functionality: this.value[0:2] instead of this.value.slice(0, 2)",
	).InCategory(
		MethodCategoryStrings,
		"Extract a slice from a string by specifying two indices, a low and high bound, which selects a half-open range that includes the first character, but excludes the last one. If the second index is omitted then it defaults to the length of the input sequence. **Consider using bracket syntax for more functionality: `this.value[0:2]` instead of `this.value.slice(0, 2)`.**",
		NewExampleSpec("",
			`# Method syntax
root.beginning = this.value.slice(0, 2)
root.end = this.value.slice(4)

# Bracket syntax (recommended)
root.beginning = this.value[0:2]  
root.end = this.value[4:]`,
			`{"value":"foo bar"}`,
			`{"beginning":"fo","end":"bar"}`,
		),
		NewExampleSpec("A negative low index can be used, indicating an offset from the end of the sequence. If the low index is greater than the length of the sequence then an empty result is returned. **Consider bracket syntax for consistency: `this.value[-4:]` instead of `this.value.slice(-4)`.**",
			`# Method syntax
root.last_chunk = this.value.slice(-4)
root.the_rest = this.value.slice(0, -4)

# Bracket syntax (recommended)
root.last_chunk = this.value[-4:]
root.the_rest = this.value[:-4]`,
			`{"value":"foo bar"}`,
			`{"last_chunk":" bar","the_rest":"foo"}`,
		),
	).InCategory(
		MethodCategoryObjectAndArray,
		"Extract a slice from an array by specifying two indices, a low and high bound, which selects a half-open range that includes the first element, but excludes the last one. If the second index is omitted then it defaults to the length of the input sequence. **This method is deprecated, use bracket syntax: `this.value[0:2]` instead of `this.value.slice(0, 2)`.**",
		NewExampleSpec("",
			`# Deprecated - use bracket syntax instead
root.beginning = this.value.slice(0, 2)
root.end = this.value.slice(4)

# Bracket syntax (recommended)
root.beginning = this.value[0:2]
root.end = this.value[4:]`,
			`{"value":["foo","bar","baz","buz","bev"]}`,
			`{"beginning":["foo","bar"],"end":["bev"]}`,
		),
		NewExampleSpec(
			"A negative low index can be used, indicating an offset from the end of the sequence. If the low index is greater than the length of the sequence then an empty result is returned. **Use bracket syntax: `this.value[-2:]` instead of `this.value.slice(-2)`.**",
			`# Deprecated - use bracket syntax instead
root.last_chunk = this.value.slice(-2)
root.the_rest = this.value.slice(0, -2)

# New bracket syntax (recommended)
root.last_chunk = this.value[-2:]
root.the_rest = this.value[:-2]`,
			`{"value":["foo","bar","baz","buz","bev"]}`,
			`{"last_chunk":["buz","bev"],"the_rest":["foo","bar","baz"]}`,
		),
	).
		Param(ParamInt64("low", "The low bound, which is the first element of the selection, or if negative selects from the end.")).
		Param(ParamInt64("high", "An optional high bound.").Optional()),
	sliceMethod,
)

func sliceMethod(args *ParsedParams) (simpleMethod, error) {
	low, err := args.FieldInt64("low")
	if err != nil {
		return nil, err
	}
	high, err := args.FieldOptionalInt64("high")
	if err != nil {
		return nil, err
	}
	if high != nil && *high > 0 && low >= *high {
		return nil, fmt.Errorf("lower slice bound %v must be lower than upper (%v)", low, *high)
	}
	getBounds := func(l int64) (lowV, highV int64, err error) {
		highV = l
		if high != nil {
			if *high < 0 {
				highV += *high
			} else {
				highV = *high
			}
		}
		if highV > l {
			highV = l
		}
		if highV < 0 {
			highV = 0
		}
		lowV = low
		if lowV < 0 {
			lowV = l + lowV
			if lowV < 0 {
				lowV = 0
			}
		}
		if lowV > highV {
			err = fmt.Errorf("lower slice bound %v must be lower than or equal to upper bound (%v) and target length (%v)", lowV, highV, l)
		}
		return
	}
	return func(v any, ctx FunctionContext) (any, error) {
		switch t := v.(type) {
		case string:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		case []byte:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		case []any:
			start, end, err := getBounds(int64(len(t)))
			if err != nil {
				return nil, err
			}
			return t[start:end], nil
		}
		return nil, value.NewTypeError(v, value.TArray, value.TString)
	}, nil
}

//------------------------------------------------------------------------------

var _ = registerMethod(
	NewMethodSpec(
		"sum", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Sum the numerical values of an array.",
		NewExampleSpec("",
			`root.sum = this.foo.sum()`,
			`{"foo":[3,8,4]}`,
			`{"sum":15}`,
		),
	),
	sumMethod,
)

func sumMethod(target Function, _ *ParsedParams) (Function, error) {
	return ClosureFunction("method sum", func(ctx FunctionContext) (any, error) {
		v, err := target.Exec(ctx)
		if err != nil {
			return nil, err
		}
		switch t := value.ISanitize(v).(type) {
		case float64, int64, uint64, json.Number:
			return v, nil
		case []any:
			var total float64
			for i, v := range t {
				n, nErr := value.IGetNumber(v)
				if nErr != nil {
					err = fmt.Errorf("index %v: %w", i, nErr)
				} else {
					total += n
				}
			}
			if err != nil {
				return nil, err
			}
			return total, nil
		}
		return nil, value.NewTypeErrorFrom(target.Annotation(), v, value.TArray)
	}, target.QueryTargets), nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"unique", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Attempts to remove duplicate values from an array. The array may contain a combination of different value types, but numbers and strings are checked separately (`\"5\"` is a different element to `5`).",
		NewExampleSpec("",
			`root.uniques = this.foo.unique()`,
			`{"foo":["a","b","a","c"]}`,
			`{"uniques":["a","b","c"]}`,
		),
	).
		Param(ParamQuery(
			"emit",
			"An optional query that can be used in order to yield a value for each element to determine uniqueness.",
			false,
		).Optional()),
	uniqueMethod,
)

func uniqueMethod(args *ParsedParams) (simpleMethod, error) {
	emitFn, err := args.FieldOptionalQuery("emit")
	if err != nil {
		return nil, err
	}
	return func(v any, ctx FunctionContext) (any, error) {
		slice, ok := v.([]any)
		if !ok {
			return nil, value.NewTypeError(v, value.TArray)
		}

		var strCompares map[string]struct{}
		var numCompares map[float64]struct{}

		checkStr := func(str string) bool {
			if strCompares == nil {
				strCompares = make(map[string]struct{}, len(slice))
			}
			_, exists := strCompares[str]
			if !exists {
				strCompares[str] = struct{}{}
			}
			return !exists
		}

		checkNum := func(num float64) bool {
			if numCompares == nil {
				numCompares = make(map[float64]struct{}, len(slice))
			}
			_, exists := numCompares[num]
			if !exists {
				numCompares[num] = struct{}{}
			}
			return !exists
		}

		uniqueSlice := make([]any, 0, len(slice))
		for i, v := range slice {
			check := v
			if emitFn != nil {
				var err error
				if check, err = emitFn.Exec(ctx.WithValue(v)); err != nil {
					return nil, fmt.Errorf("index %v: %w", i, err)
				}
			}
			var unique bool
			switch t := value.ISanitize(check).(type) {
			case string:
				unique = checkStr(t)
			case []byte:
				unique = checkStr(string(t))
			case json.Number:
				f, err := t.Float64()
				if err != nil {
					var i int64
					if i, err = t.Int64(); err == nil {
						f = float64(i)
					}
				}
				if err != nil {
					return nil, fmt.Errorf("index %v: failed to parse number: %w", i, err)
				}
				unique = checkNum(f)
			case int64:
				unique = checkNum(float64(t))
			case uint64:
				unique = checkNum(float64(t))
			case float64:
				unique = checkNum(t)
			default:
				return nil, fmt.Errorf("index %v: %w", i, value.NewTypeError(check, value.TString, value.TNumber))
			}
			if unique {
				uniqueSlice = append(uniqueSlice, v)
			}
		}
		return uniqueSlice, nil
	}, nil
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"values", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Returns the values of an object as an array. The order of the resulting array will be random.",
		NewExampleSpec("",
			`root.foo_vals = this.foo.values().sort()`,
			`{"foo":{"bar":1,"baz":2}}`,
			`{"foo_vals":[1,2]}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			if m, ok := v.(map[string]any); ok {
				values := make([]any, 0, len(m))
				for _, e := range m {
					values = append(values, e)
				}
				return values, nil
			}
			return nil, value.NewTypeError(v, value.TObject)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"without", "",
	).InCategory(
		MethodCategoryObjectAndArray,
		`Returns an object where one or more [field path][field_paths] arguments are removed. Each path specifies a specific field to be deleted from the input object, allowing for nested fields.

If a key within a nested path does not exist or is not an object then it is not removed.`,
		NewExampleSpec("",
			`root = this.without("inner.a","inner.c","d")`,
			`{"inner":{"a":"first","b":"second","c":"third"},"d":"fourth","e":"fifth"}`,
			`{"e":"fifth","inner":{"b":"second"}}`,
		),
	).VariadicParams(),
	func(args *ParsedParams) (simpleMethod, error) {
		excludeList := make([][]string, 0, len(args.Raw()))
		for i, argVal := range args.Raw() {
			argStr, err := value.IGetString(argVal)
			if err != nil {
				return nil, fmt.Errorf("argument %v: %w", i, err)
			}
			excludeList = append(excludeList, gabs.DotPathToSlice(argStr))
		}
		return func(v any, ctx FunctionContext) (any, error) {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TObject)
			}
			return mapWithout(m, excludeList), nil
		}, nil
	},
)

func mapWithout(m map[string]any, paths [][]string) map[string]any {
	newMap := make(map[string]any, len(m))
	for k, v := range m {
		excluded := false
		var nestedExclude [][]string
		for _, p := range paths {
			if p[0] == k {
				if len(p) > 1 {
					nestedExclude = append(nestedExclude, p[1:])
				} else {
					excluded = true
				}
			}
		}
		if !excluded {
			if len(nestedExclude) > 0 {
				vMap, ok := v.(map[string]any)
				if ok {
					newMap[k] = mapWithout(vMap, nestedExclude)
				} else {
					newMap[k] = v
				}
			} else {
				newMap[k] = v
			}
		}
	}
	return newMap
}

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"split",
		"Splits a string or array into segments by splitting on all occurrences of a delimiter value. Returns an array of segments with delimiters excluded.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Splits an array into segments by splitting on all occurrences of a delimiter value. Returns an array of segments with delimiters excluded.",
		NewExampleSpec("Split array on value",
			`root.segments = this.villains.split("Kraven The Hunter")`,
			`{"villains": ["Doctor Octopus", "Electro", "Kraven The Hunter", "Mysterio", "Sandman", "Vulture"]}`,
			`{"segments":[["Doctor Octopus","Electro"],["Mysterio","Sandman","Vulture"]]}`,
		),
		NewExampleSpec("Split an array of mixed types",
			`root.parts = this.mixed.split(0)`,
			`{"mixed": [1, "a", 2, "b", 3, "c", 4, "d", 0, "e", 5, "f", 6, "g", 7, "h", 0, "i", 8, "j", 9]}`,
			`{"parts":[[1,"a",2,"b",3,"c",4,"d"],["e",5,"f",6,"g",7,"h"],["i",8,"j",9]]}`,
		),
		NewExampleSpec("Split array of objects by object separator",
			`root.groups = this.objects.split({"type": "separator"})`,
			`{"objects": [{"id": 1, "name": "Spider-Man"}, {"type": "separator"}, {"id": 2, "name": "Daredevil"}]}`,
			`{"groups":[[{"id":1,"name":"Spider-Man"}],[{"id":2,"name":"Daredevil"}]]}`,
		),
	).InCategory(
		MethodCategoryStrings,
		"Splits a string into segments by splitting on all occurrences of a delimiter string. Returns an array of string segments with delimiters excluded.",
		NewExampleSpec("Split on space",
			`root.words = this.sentence.split(" ")`,
			`{"sentence":"Hello Bento!"}`,
			`{"words":["Hello","Bento!"]}`,
		),
		NewExampleSpec("Split string on delimiter",
			`root.words = this.value.split(",")`,
			`{"value":"foo,bar,baz"}`,
			`{"words":["foo","bar","baz"]}`,
		),
	).Param(ParamAny("delimiter", "The delimiter to split with.")), func(args *ParsedParams) (simpleMethod, error) {
		delim, err := args.Field("delimiter")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			switch t := v.(type) {
			case string:
				delimStr, ok := delim.(string)
				if !ok {
					return nil, fmt.Errorf("delimiter type: %T must match type of list elements: %T", delim, v)
				}

				parts := strings.Split(t, delimStr)
				vals := make([]any, 0, len(parts))
				for _, b := range parts {
					vals = append(vals, b)
				}
				return vals, nil

			case []byte:
				delimStr, ok := delim.(string)
				if !ok {
					return nil, fmt.Errorf("delimiter type: %T must match type of list elements: %T", delim, v)
				}

				parts := bytes.Split(t, []byte(delimStr))
				vals := make([]any, 0, len(parts))
				for _, b := range parts {
					vals = append(vals, b)
				}
				return vals, nil

			case []any:
				if len(t) == 0 {
					return []any{[]any{}}, nil
				}

				var segments []any
				var currentSegment []any

				for _, elem := range t {
					if !value.ICompare(delim, elem) {
						currentSegment = append(currentSegment, elem)
						continue
					}

					segments = append(segments, currentSegment)
					currentSegment = []any{}
				}

				segments = append(segments, currentSegment)
				return segments, nil
			}

			return nil, value.NewTypeError(v, value.TString, value.TArray, value.TBytes)
		}, nil
	},
)

//------------------------------------------------------------------------------

var _ = registerSimpleMethod(
	NewMethodSpec(
		"split_by",
		"Splits a string or array into segments where a query resolves to true. Returns an array of segments with matching elements excluded.",
	).InCategory(
		MethodCategoryObjectAndArray,
		"Splits an array into segments where a query applied to each element resolves to true. Returns an array of segments with matching elements excluded.",
		NewExampleSpec("Split array using element predicate",
			`root.authors = this.writers.split_by(x -> x.contains("Kafka"))`,
			`{"writers": ["George Orwell", "Franz Kafka", "Anton Chekhov"]}`,
			`{"authors":[["George Orwell"],["Anton Chekhov"]]}`,
		),
		NewExampleSpec("Split array using numeric predicate",
			`root.segments = this.numbers.split_by(x -> x > 50)`,
			`{"numbers": [1, 2, 100, 3, 4, 200, 5]}`,
			`{"segments":[[1,2],[3,4],[5]]}`,
		),
		NewExampleSpec("Split array of objects using predicate",
			`root.groups = this.items.split_by(item -> item.type == "separator")`,
			`{"items": [{"id": 1, "type": "data"}, {"id": 2, "type": "separator"}, {"id": 3, "type": "data"}]}`,
			`{"groups":[[{"id":1,"type":"data"}],[{"id":3,"type":"data"}]]}`,
		),
	).InCategory(
		MethodCategoryStrings,
		"Splits a string into segments where a query applied to each character resolves to true. Returns an array of string segments with matching characters excluded.",
		NewExampleSpec("Split string using character predicate",
			`root.words = this.sentence.split_by(c -> c == " ")`,
			`{"sentence": "Hello Bento!"}`,
			`{"words":["Hello","Bento!"]}`,
		),
		NewExampleSpec("Split on punctuation",
			`root.tokens = this.text.split_by(c -> c == "," || c == ".")`,
			`{"text": "foo,bar.baz"}`,
			`{"tokens":["foo","bar","baz"]}`,
		),
	).Param(ParamQuery("predicate", "A query that returns true where splits should occur.", false)).Experimental().AtVersion("1.11.0"),
	func(args *ParsedParams) (simpleMethod, error) {
		queryFn, err := args.FieldQuery("predicate")
		if err != nil {
			return nil, err
		}

		return func(v any, ctx FunctionContext) (any, error) {
			switch t := v.(type) {
			case string:
				if t == "" {
					return []any{""}, nil
				}

				var segments []any
				runes := []rune(t)
				start := 0

				for i, r := range runes {
					result, err := queryFn.Exec(ctx.WithValue(string(r)))
					if err != nil {
						return nil, fmt.Errorf("predicate returned an error for character at position %v: %w", i, err)
					}

					if found, ok := result.(bool); !ok || !found {
						continue
					}

					segment := ""
					if i > start {
						segment = string(runes[start:i])
					}
					segments = append(segments, segment)
					start = i + 1
				}

				finalSegment := ""
				if start < len(runes) {
					finalSegment = string(runes[start:])
				}
				return append(segments, finalSegment), nil

			case []byte:
				if len(t) == 0 {
					return []any{[]byte{}}, nil
				}

				var segments []any
				start := 0

				for i, b := range t {
					result, err := queryFn.Exec(ctx.WithValue(b))
					if err != nil {
						return nil, fmt.Errorf("predicate returned an error for byte at position %v: %w", i, err)
					}

					if found, ok := result.(bool); !ok || !found {
						continue
					}

					segment := []byte{}
					if i > start {
						segment = t[start:i]
					}
					segments = append(segments, segment)
					start = i + 1
				}

				finalSegment := []byte{}
				if start < len(t) {
					finalSegment = t[start:]
				}
				return append(segments, finalSegment), nil

			case []any:
				if len(t) == 0 {
					return []any{[]any{}}, nil
				}

				var segments []any
				var currentSegment []any

				for i, elem := range t {
					result, err := queryFn.Exec(ctx.WithValue(elem))
					if err != nil {
						return nil, fmt.Errorf("predicate returned an error for index %v: %w", i, err)
					}

					if found, ok := result.(bool); !ok || !found {
						currentSegment = append(currentSegment, elem)
						continue
					}

					segments = append(segments, currentSegment)
					currentSegment = []any{}
				}

				return append(segments, currentSegment), nil

			}

			return nil, value.NewTypeError(v, value.TString, value.TArray, value.TBytes)
		}, nil
	},
)
