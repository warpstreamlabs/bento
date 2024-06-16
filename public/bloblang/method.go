package bloblang

import (
	"time"

	"github.com/warpstreamlabs/bento/internal/value"
)

// Method defines a Bloblang function that executes on a value. Arguments are
// provided to the constructor, allowing the implementation of this method to
// resolve them statically when possible.
//
// In order to avoid type checking the value use one of the typed variants such
// as StringMethod.
type Method func(v any) (any, error)

// MethodConstructor defines a constructor for a Bloblang method, where a
// variadic list of arguments are provided.
//
// When a method is parsed from a mapping with static arguments the constructor
// will be called only once at parse time. When a method is parsed with dynamic
// arguments, such as a value derived from the mapping input, the constructor
// will be called on each invocation of the mapping with the derived arguments.
//
// For a convenient way to perform type checking and coercion on the arguments
// use an ArgSpec.
type MethodConstructor func(args ...any) (Method, error)

// MethodConstructorV2 defines a constructor for a Bloblang method where
// parameters are parsed using a ParamsSpec provided when registering the
// method.
//
// When a method is parsed from a mapping with static arguments the constructor
// will be called only once at parse time. When a method is parsed with dynamic
// arguments, such as a value derived from the mapping input, the constructor
// will be called on each invocation of the mapping with the derived arguments.
type MethodConstructorV2 func(args *ParsedParams) (Method, error)

//------------------------------------------------------------------------------

// AdvancedMethod defines a Bloblang method that accesses the execution context
// of the mapping during invocation. Advanced methods are responsible for
// calling Exec upon the ExecFunction they target, and have the capability to
// mutate or modify the execution context of that target.
type AdvancedMethod func(ctx *ExecContext, fn *ExecFunction) (any, error)

// AdvancedMethodConstructor defines a constructor for a Bloblang method
// where parameters are parsed using a ParamsSpec provided when registering the
// method, and the constructed method is provided an ExecContext.
type AdvancedMethodConstructor func(args *ParsedParams) (AdvancedMethod, error)

//------------------------------------------------------------------------------

// StringMethod creates a general method signature from a string method by
// performing type checking on the method target.
func StringMethod(methodFn func(string) (any, error)) Method {
	return func(v any) (any, error) {
		str, err := value.IGetString(v)
		if err != nil {
			return nil, err
		}
		return methodFn(str)
	}
}

// BytesMethod creates a general method signature from a byte slice method by
// performing type checking on the method target.
func BytesMethod(methodFn func([]byte) (any, error)) Method {
	return func(v any) (any, error) {
		b, err := value.IGetBytes(v)
		if err != nil {
			return nil, err
		}
		return methodFn(b)
	}
}

// TimestampMethod creates a general method signature from a timestamp method by
// performing type checking on the method target.
func TimestampMethod(methodFn func(time.Time) (any, error)) Method {
	return func(v any) (any, error) {
		t, err := value.IGetTimestamp(v)
		if err != nil {
			return nil, err
		}
		return methodFn(t)
	}
}

// ArrayMethod creates a general method signature from an array method by
// performing type checking on the method target.
func ArrayMethod(methodFn func([]any) (any, error)) Method {
	return func(v any) (any, error) {
		arr, ok := v.([]any)
		if !ok {
			return nil, value.NewTypeError(v, value.TArray)
		}
		return methodFn(arr)
	}
}

// BoolMethod creates a general method signature from a bool method by
// performing type checking on the method target.
func BoolMethod(methodFn func(bool) (any, error)) Method {
	return func(v any) (any, error) {
		b, err := value.IGetBool(v)
		if err != nil {
			return nil, err
		}
		return methodFn(b)
	}
}

// Int64Method creates a general method signature from an int method by
// performing type checking on the method target.
func Int64Method(methodFn func(int64) (any, error)) Method {
	return func(v any) (any, error) {
		i, err := value.IGetInt(v)
		if err != nil {
			return nil, err
		}
		return methodFn(i)
	}
}

// Float64Method creates a general method signature from a float method by
// performing type checking on the method target.
func Float64Method(methodFn func(float64) (any, error)) Method {
	return func(v any) (any, error) {
		f, err := value.IGetNumber(v)
		if err != nil {
			return nil, err
		}
		return methodFn(f)
	}
}

// ObjectMethod creates a general method signature from an object method by
// performing type checking on the method target.
func ObjectMethod(methodFn func(obj map[string]any) (any, error)) Method {
	return func(v any) (any, error) {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, value.NewTypeError(v, value.TObject)
		}
		return methodFn(obj)
	}
}
