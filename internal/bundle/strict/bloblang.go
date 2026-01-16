package strict

import (
	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/bundle"
)

func StrictBloblangEnvironment(nm bundle.NewManagement) *bloblang.Environment {
	env := nm.BloblEnvironment()
	newEnv := bloblang.NewEnvironment()

	env.WalkFunctions(func(name string, spec query.FunctionSpec) {
		_ = newEnv.RegisterFunction(spec, func(args *query.ParsedParams) (query.Function, error) {
			if isBloblangFunctionIncompatible(name) {
				nm.Logger().Warn("Disabling strict mode due to incompatible Bloblang function(s) of type '%s'", name)
				nm.SetGeneric(strictModeEnabledKey{}, false)
			} else {
				// For compatible processors, set true if not already set
				nm.GetOrSetGeneric(strictModeEnabledKey{}, true)
			}

			return query.AllFunctions.Init(name, args)
		})
	})

	env.WalkMethods(func(name string, spec query.MethodSpec) {
		_ = newEnv.RegisterMethod(spec, func(target query.Function, args *query.ParsedParams) (query.Function, error) {
			if isBloblangMethodIncompatible(name) {
				nm.Logger().Warn("Disabling strict mode due to incompatible Bloblang method(s) of type '%s'", name)
				nm.SetGeneric(strictModeEnabledKey{}, false)
			} else {
				// For compatible processors, set true if not already set
				nm.GetOrSetGeneric(strictModeEnabledKey{}, true)
			}
			return query.AllMethods.Init(name, target, args)
		})
	})

	return newEnv
}
