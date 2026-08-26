package bloblang

import (
	"errors"

	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

// Executor stores a parsed Bloblang mapping and provides APIs for executing it.
type Executor struct {
	exec              *mapping.Executor
	emptyQueryMessage message.Batch
	assignsVars       bool
}

func newExecutor(exec *mapping.Executor) *Executor {
	return &Executor{
		exec:              exec,
		emptyQueryMessage: message.QuickBatch(nil),
		assignsVars:       exec.AssignsVariables(),
	}
}

// sharedEmptyVars is handed to mappings that contain no variable assignments,
// so that the common case does not allocate a map per execution.
//
// It must never be written to. Only VarAssignment writes to a variables map,
// and it is reached only from a statement whose assignment target is a
// variable, which is precisely what AssignsVariables reports. Reads of an
// undefined variable are unaffected: an empty map yields the same "variable
// undefined" error as a freshly allocated one, whereas a nil map would report
// "variables were undefined" instead and change behaviour.
var sharedEmptyVars = map[string]any{}

func (e *Executor) newVars() map[string]any {
	if e.assignsVars {
		return map[string]any{}
	}
	return sharedEmptyVars
}

// ErrRootDeleted is returned by a Bloblang query when the mapping results in
// the root being deleted. It might be considered correct to do this in
// situations where filtering is allowed or expected.
var ErrRootDeleted = errors.New("root was deleted")

// Query executes a Bloblang mapping against a value and returns the result. The
// argument and return values can be structured using the same
// map[string]interface{} and []interface{} types as would be returned by the Go
// standard json package unmarshaler.
//
// If the mapping results in the root of the new document being deleted then
// ErrRootDeleted is returned, which can be used as a signal to filter rather
// than fail the mapping.
func (e *Executor) Query(val any) (any, error) {
	res, err := e.exec.Exec(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     e.newVars(),
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
	}.WithValue(val))
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case value.Delete:
		return nil, ErrRootDeleted
	case value.Nothing:
		return val, nil
	}
	return res, nil
}

// Overlay executes a Bloblang mapping against a value, where assignments are
// overlayed onto an existing structure.
//
// If the mapping results in the root of the new document being deleted then
// ErrRootDeleted is returned, which can be used as a signal to filter rather
// than fail the mapping.
func (e *Executor) Overlay(val any, onto *any) error {
	vars := e.newVars()

	if err := e.exec.ExecOnto(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     vars,
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
		NewValue: onto,
	}.WithValue(val), mapping.AssignmentContext{
		Vars:  vars,
		Value: onto,
	}); err != nil {
		return err
	}

	switch (*onto).(type) {
	case value.Delete:
		return ErrRootDeleted
	case value.Nothing:
		*onto = nil
	}
	return nil
}
