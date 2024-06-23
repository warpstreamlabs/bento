package service

import (
	"github.com/warpstreamlabs/bento/internal/bloblang/field"
	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

// MessageBatchBloblangExecutor is a mechanism for executing a given bloblang
// executor against a message batch, with each invocation from the perspective
// of a given index of the batch. This allows mappings to perform windowed
// aggregations across message batches.
type MessageBatchBloblangExecutor struct {
	oldBatch message.Batch
	exe      *mapping.Executor
}

// BloblangExecutor instantiates a mechanism for executing a given bloblang
// executor against a message batch, with each invocation from the perspective
// of a given index of the batch. This allows mappings to perform windowed
// aggregations across message batches.
func (b MessageBatch) BloblangExecutor(blobl *bloblang.Executor) *MessageBatchBloblangExecutor {
	uw := blobl.XUnwrapper().(interface {
		Unwrap() *mapping.Executor
	}).Unwrap()

	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}

	return &MessageBatchBloblangExecutor{
		oldBatch: msg,
		exe:      uw,
	}
}

// Query executes a parsed Bloblang mapping on a message batch, from the
// perspective of a particular message index, and returns a message back or an
// error if the mapping fails. If the mapping results in the root being deleted
// the returned message will be nil, which indicates it has been filtered.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
func (b MessageBatchBloblangExecutor) Query(index int) (*Message, error) {
	res, err := b.exe.MapPart(index, b.oldBatch)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return NewInternalMessage(res), nil
	}
	return nil, nil
}

// QueryValue executes a parsed Bloblang mapping on a message batch,
// from the perspective of a particular message index, and returns the raw value
// result or an error if the mapping fails. The error bloblang.ErrRootDeleted is
// returned if the root of the mapping value is deleted, this is in order to
// allow distinction between a real nil value and a deleted value.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
func (b MessageBatchBloblangExecutor) QueryValue(index int) (any, error) {
	res, err := b.exe.Exec(query.FunctionContext{
		Maps:     b.exe.Maps(),
		Vars:     map[string]any{},
		Index:    index,
		MsgBatch: b.oldBatch,
	})
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case value.Delete:
		return nil, bloblang.ErrRootDeleted
	case value.Nothing:
		return nil, nil
	}
	return res, nil
}

// Mutate executes a parsed Bloblang mapping onto a message within the
// batch, where the contents of the message are mutated directly rather than
// creating an entirely new object.
//
// Returns the same message back in a mutated form, or an error if the mapping
// fails. If the mapping results in the root being deleted the returned message
// will be nil, which indicates it has been filtered.
//
// This method allows mappings to perform windowed aggregations across message
// batches.
//
// Note that using overlay means certain functions within the Bloblang mapping
// will behave differently. In the root of the mapping the right-hand keywords
// `root` and `this` refer to the same mutable root of the output document.
func (b MessageBatchBloblangExecutor) Mutate(index int) (*Message, error) {
	res, err := b.exe.MapOnto(b.oldBatch[index], index, b.oldBatch)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return NewInternalMessage(res), nil
	}
	return nil, nil
}

//------------------------------------------------------------------------------

// MessageBatchInterpolationExecutor is a mechanism for executing a given
// bloblang interpolation string against a message batch, with each invocation
// from the perspective of a given index of the batch. This allows
// interpolations to perform windowed aggregations across message batches.
type MessageBatchInterpolationExecutor struct {
	oldBatch message.Batch
	i        *field.Expression
}

// InterpolationExecutor instantiates a mechanism for executing a given bloblang
// interpolation string against a message batch, with each invocation from the
// perspective of a given index of the batch. This allows interpolations to
// perform windowed aggregations across message batches.
func (b MessageBatch) InterpolationExecutor(i *InterpolatedString) *MessageBatchInterpolationExecutor {
	msg := make(message.Batch, len(b))
	for i, m := range b {
		msg[i] = m.part
	}

	return &MessageBatchInterpolationExecutor{
		oldBatch: msg,
		i:        i.expr,
	}
}

// TryString resolves an interpolated string expression on a message batch, from
// the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
func (b MessageBatchInterpolationExecutor) TryString(index int) (string, error) {
	return b.i.String(index, b.oldBatch)
}

// TryBytes resolves an interpolated string expression on a message batch, from
// the perspective of a particular message index.
//
// This method allows interpolation functions to perform windowed aggregations
// across message batches, and is a more powerful way to interpolate strings
// than the standard .String method.
func (b MessageBatchInterpolationExecutor) TryBytes(index int) ([]byte, error) {
	return b.i.Bytes(index, b.oldBatch)
}
