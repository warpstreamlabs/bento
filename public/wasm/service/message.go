package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aperturerobotics/protobuf-go-lite/types/known/timestamppb"
	messagepb "github.com/warpstreamlabs/bento/internal/message/messagepblite"
)

// MessageBatch is an ordered list of messages.
type MessageBatch []*Message

// Message is a single message, with its content, metadata and error. It wraps
// the protobuf part exchanged with Bento, which is decoded once per call and
// encoded back without any conversion.
type Message struct {
	part *messagepb.Part
}

// NewMessage returns a message with raw content and no metadata.
func NewMessage(content []byte) *Message {
	return &Message{
		part: &messagepb.Part{
			Content: &messagepb.Part_Raw{Raw: content},
		},
	}
}

// AsBytes returns the content of the message, where structured content is
// encoded as JSON.
func (m *Message) AsBytes() ([]byte, error) {
	switch c := m.part.GetContent().(type) {
	case *messagepb.Part_Raw:
		return c.Raw, nil
	case *messagepb.Part_Structured:
		return json.Marshal(fromProtoValue(c.Structured))
	}
	return nil, nil
}

// AsStructured returns the content of the message as a structured value, where
// raw content is parsed as JSON. Mutating the value does not change the message.
func (m *Message) AsStructured() (any, error) {
	switch c := m.part.GetContent().(type) {
	case *messagepb.Part_Raw:
		var v any
		if err := json.Unmarshal(c.Raw, &v); err != nil {
			return nil, err
		}
		return v, nil
	case *messagepb.Part_Structured:
		return fromProtoValue(c.Structured), nil
	}
	return nil, errors.New("message has no content")
}

// GetError returns the error that the message is marked with, or nil.
func (m *Message) GetError() error {
	if m.part.Error == nil {
		return nil
	}
	return errors.New(m.part.Error.Message)
}

// SetBytes replaces the content of the message with raw bytes.
func (m *Message) SetBytes(b []byte) {
	m.part.Content = &messagepb.Part_Raw{Raw: b}
}

// SetError marks the message with err, or clears its error when err is nil.
// A marked message continues through the pipeline, and can be handled with
// Bento's error handling.
func (m *Message) SetError(err error) {
	m.part.Error = nil
	if err != nil {
		m.part.Error = &messagepb.Error{Message: err.Error()}
	}
}

// SetStructured replaces the content of the message with a structured value.
// Values that aren't maps, slices, strings, numbers, bools, bytes, timestamps or
// nil, such as structs, are converted through JSON. If that fails, the message
// is marked with the error.
func (m *Message) SetStructured(i any) {
	v, err := toProtoValue(i)
	if err != nil {
		v, err = valueFromJSON(i)
	}
	if err != nil {
		m.SetError(err)
		return
	}
	m.part.Content = &messagepb.Part_Structured{Structured: v}
}

func valueFromJSON(i any) (*messagepb.Value, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return toProtoValue(v)
}

// MetaDelete removes the metadata value of key.
func (m *Message) MetaDelete(key string) {
	delete(m.part.Metadata, key)
}

// MetaGet returns the metadata value of key, and whether it exists. Every
// metadata value is a string.
func (m *Message) MetaGet(key string) (string, bool) {
	v, exists := m.part.Metadata[key]
	return v, exists
}

// MetaSet sets the metadata value of key.
func (m *Message) MetaSet(key, value string) {
	// Messages decoded without metadata have no map.
	if m.part.Metadata == nil {
		m.part.Metadata = make(map[string]string)
	}
	m.part.Metadata[key] = value
}

// MetaWalk calls fn with each metadata key and value, in no particular order,
// and returns the first error that fn returns.
func (m *Message) MetaWalk(fn func(string, string) error) error {
	for k, v := range m.part.Metadata {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// toProtoValue converts a structured value to a protobuf Value. Numbers keep
// their type, and a json.Number keeps its exact text as a decimal.
func toProtoValue(v any) (*messagepb.Value, error) {
	switch v := v.(type) {
	case nil:
		return &messagepb.Value{Kind: &messagepb.Value_Null{}}, nil
	case bool:
		return &messagepb.Value{Kind: &messagepb.Value_Bool{Bool: v}}, nil
	case string:
		return &messagepb.Value{Kind: &messagepb.Value_String_{String_: v}}, nil
	case []byte:
		return &messagepb.Value{Kind: &messagepb.Value_Bytes{Bytes: v}}, nil
	case json.Number:
		return &messagepb.Value{Kind: &messagepb.Value_Decimal{Decimal: v.String()}}, nil
	case time.Time:
		return &messagepb.Value{Kind: &messagepb.Value_Timestamp{Timestamp: timestamppb.New(v)}}, nil
	case []any:
		items := make([]*messagepb.Value, len(v))
		for i, item := range v {
			var err error
			if items[i], err = toProtoValue(item); err != nil {
				return nil, err
			}
		}
		return &messagepb.Value{Kind: &messagepb.Value_List{List: &messagepb.List{Items: items}}}, nil
	case map[string]any:
		fields := make(map[string]*messagepb.Value, len(v))
		for k, field := range v {
			var err error
			if fields[k], err = toProtoValue(field); err != nil {
				return nil, err
			}
		}
		return &messagepb.Value{Kind: &messagepb.Value_Struct{Struct: &messagepb.Struct{Fields: fields}}}, nil
	}

	switch rv := reflect.ValueOf(v); {
	case rv.CanInt():
		return &messagepb.Value{Kind: &messagepb.Value_Int64{Int64: rv.Int()}}, nil
	case rv.CanUint():
		return &messagepb.Value{Kind: &messagepb.Value_Uint64{Uint64: rv.Uint()}}, nil
	case rv.CanFloat():
		return &messagepb.Value{Kind: &messagepb.Value_Double{Double: rv.Float()}}, nil
	}
	return nil, fmt.Errorf("unsupported structured value of type %T", v)
}

// fromProtoValue converts a protobuf Value to a structured value.
func fromProtoValue(v *messagepb.Value) any {
	switch kind := v.GetKind().(type) {
	case *messagepb.Value_Bool:
		return kind.Bool
	case *messagepb.Value_Double:
		return kind.Double
	case *messagepb.Value_String_:
		return kind.String_
	case *messagepb.Value_Int64:
		return kind.Int64
	case *messagepb.Value_Uint64:
		return kind.Uint64
	case *messagepb.Value_Bytes:
		return kind.Bytes
	case *messagepb.Value_Decimal:
		return json.Number(kind.Decimal)
	case *messagepb.Value_Timestamp:
		return kind.Timestamp.AsTime()
	case *messagepb.Value_List:
		items := make([]any, len(kind.List.GetItems()))
		for i, item := range kind.List.GetItems() {
			items[i] = fromProtoValue(item)
		}
		return items
	case *messagepb.Value_Struct:
		fields := make(map[string]any, len(kind.Struct.GetFields()))
		for k, field := range kind.Struct.GetFields() {
			fields[k] = fromProtoValue(field)
		}
		return fields
	}
	return nil
}

//------------------------------------------------------------------------------

func toProtoBatch(batch MessageBatch) *messagepb.Batch {
	parts := make([]*messagepb.Part, len(batch))
	for i, msg := range batch {
		parts[i] = msg.part
	}
	return &messagepb.Batch{Parts: parts}
}

func fromProtoBatch(batch *messagepb.Batch) MessageBatch {
	result := make(MessageBatch, len(batch.GetParts()))
	for i, part := range batch.GetParts() {
		result[i] = &Message{part: part}
	}
	return result
}

// UnmarshalBatchFromProto decodes a protobuf Batch. It is used by the PDK to
// exchange messages with Bento, and plugins don't usually need it.
func UnmarshalBatchFromProto(b []byte) (MessageBatch, error) {
	batch := &messagepb.Batch{}
	if err := batch.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return fromProtoBatch(batch), nil
}

// MarshalBatchToProto encodes batch as a protobuf Batch. It is used by the PDK
// to exchange messages with Bento, and plugins don't usually need it.
func MarshalBatchToProto(batch MessageBatch) ([]byte, error) {
	return toProtoBatch(batch).MarshalVT()
}

// MarshalBatchesToProto encodes batches as protobuf Batches. It is used by the
// PDK to exchange messages with Bento, and plugins don't usually need it.
func MarshalBatchesToProto(batches []MessageBatch) ([]byte, error) {
	out := &messagepb.Batches{Batches: make([]*messagepb.Batch, len(batches))}
	for i, batch := range batches {
		out.Batches[i] = toProtoBatch(batch)
	}
	return out.MarshalVT()
}

// MarshalProcessResponseToProto encodes batch, or err if it isn't nil, as a
// protobuf ProcessResponse. It is used by the PDK to exchange messages with
// Bento, and plugins don't usually need it.
func MarshalProcessResponseToProto(batch MessageBatch, err error) ([]byte, error) {
	if err != nil {
		res := &messagepb.ProcessResponse{Result: &messagepb.ProcessResponse_Error{Error: &messagepb.Error{Message: err.Error()}}}
		return res.MarshalVT()
	}
	res := &messagepb.ProcessResponse{Result: &messagepb.ProcessResponse_Batch{Batch: toProtoBatch(batch)}}
	return res.MarshalVT()
}

// MarshalProcessBatchResponseToProto encodes batches, or err if it isn't nil,
// as a protobuf ProcessBatchResponse. It is used by the PDK to exchange
// messages with Bento, and plugins don't usually need it.
func MarshalProcessBatchResponseToProto(batches []MessageBatch, err error) ([]byte, error) {
	if err != nil {
		res := &messagepb.ProcessBatchResponse{Result: &messagepb.ProcessBatchResponse_Error{Error: &messagepb.Error{Message: err.Error()}}}
		return res.MarshalVT()
	}
	out := &messagepb.Batches{Batches: make([]*messagepb.Batch, len(batches))}
	for i, batch := range batches {
		out.Batches[i] = toProtoBatch(batch)
	}
	res := &messagepb.ProcessBatchResponse{Result: &messagepb.ProcessBatchResponse_Batches{Batches: out}}
	return res.MarshalVT()
}

// UnmarshalMessageFromProto decodes a protobuf Part. It is used by the PDK to
// exchange messages with Bento, and plugins don't usually need it.
func UnmarshalMessageFromProto(b []byte) (*Message, error) {
	part := &messagepb.Part{}
	if err := part.UnmarshalVT(b); err != nil {
		return nil, err
	}
	return &Message{part: part}, nil
}

// MarshalMessageToProto encodes msg as a protobuf Part. It is used by the PDK
// to exchange messages with Bento, and plugins don't usually need it.
func MarshalMessageToProto(msg *Message) ([]byte, error) {
	return msg.part.MarshalVT()
}
