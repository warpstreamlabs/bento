package bloblang

import "github.com/warpstreamlabs/bento/internal/bloblang/mapping"

type executorUnwrapper struct {
	child *mapping.Executor
}

func (e executorUnwrapper) Unwrap() *mapping.Executor {
	return e.child
}

// XUnwrapper is for internal use only, do not use this.
func (e *Executor) XUnwrapper() any {
	return executorUnwrapper{child: e.exec}
}
