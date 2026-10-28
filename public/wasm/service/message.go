package service

import (
	"encoding/json"
	"errors"

	"github.com/aperturerobotics/protobuf-go-lite/types/known/structpb"
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
		return c.Structured.MarshalJSON()
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
		return c.Structured.AsInterface(), nil
	}
	return nil, errors.New("message has no content")
}

// GetError returns the error that the message is marked with, or nil.
func (m *Message) GetError() error {
	if m.part.Error == "" {
		return nil
	}
	return errors.New(m.part.Error)
}

// SetBytes replaces the content of the message with raw bytes.
func (m *Message) SetBytes(b []byte) {
	m.part.Content = &messagepb.Part_Raw{Raw: b}
}

// SetError marks the message with err, or clears its error when err is nil.
// A marked message continues through the pipeline, and can be handled with
// Bento's error handling.
func (m *Message) SetError(err error) {
	m.part.Error = ""
	if err != nil {
		m.part.Error = err.Error()
	}
}

// SetStructured replaces the content of the message with a structured value.
// Values that structpb.NewValue doesn't support, such as structs, are converted
// through JSON. If that fails, the message is marked with the error.
func (m *Message) SetStructured(i any) {
	v, err := structpb.NewValue(i)
	if err != nil {
		v, err = valueFromJSON(i)
	}
	if err != nil {
		m.SetError(err)
		return
	}
	m.part.Content = &messagepb.Part_Structured{Structured: v}
}

func valueFromJSON(i any) (*structpb.Value, error) {
	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	v := &structpb.Value{}
	if err := v.UnmarshalJSON(b); err != nil {
		return nil, err
	}
	return v, nil
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
