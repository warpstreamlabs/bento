package service

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/message"
)

type MessageBatch []*Message

type Message struct {
	part *message.Part
}

func NewMessage(content []byte) *Message {
	return &Message{
		part: message.NewPart(content),
	}
}

func (m *Message) Context() context.Context {
	return m.part.GetContext()
}

func (m *Message) AsBytes() ([]byte, error) {
	return m.part.AsBytes(), nil
}

func (m *Message) AsStructured() (any, error) {
	return m.part.AsStructured()
}

func (m *Message) AsStructuredMut() (any, error) {
	return m.part.AsStructuredMut()
}

func (m *Message) GetError() error {
	return m.part.ErrorGet()
}

func (m *Message) SetBytes(b []byte) {
	_ = m.part.SetBytes(b)
}

func (m *Message) SetError(err error) {
	m.part.ErrorSet(err)
}

func (m *Message) SetStructured(i any) {
	m.part.SetStructured(i)
}

func (m *Message) SetStructuredMut(i any) {
	m.part.SetStructuredMut(i)
}

func (m *Message) MetaDelete(key string) {
	m.part.MetaDelete(key)
}

func (m *Message) MetaGet(key string) (string, bool) {
	val := m.part.MetaGetStr(key)
	return val, val != ""
}

func (m *Message) MetaSet(key, value string) {
	m.part.MetaSetMut(key, value)
}

func (m *Message) MetaWalk(fn func(string, string) error) error {
	return m.part.MetaIterStr(fn)
}

func toInternalBatch(batch MessageBatch) message.Batch {
	internal := make(message.Batch, len(batch))
	for i, msg := range batch {
		internal[i] = msg.part
	}
	return internal
}

func fromInternalBatch(batch message.Batch) MessageBatch {
	result := make(MessageBatch, len(batch))
	for i, part := range batch {
		result[i] = &Message{part: part}
	}
	return result
}

func UnmarshalBatchFromProto(b []byte) (MessageBatch, error) {
	internalBatch, err := message.UnmarshalBatchFromProto(b)
	if err != nil {
		return nil, err
	}
	return fromInternalBatch(internalBatch), nil
}

func MarshalBatchToProto(batch MessageBatch) ([]byte, error) {
	return message.MarshalBatchToProto(toInternalBatch(batch))
}

func MarshalBatchesToProto(batches []MessageBatch) ([]byte, error) {
	internalBatches := make([]message.Batch, len(batches))
	for i, batch := range batches {
		internalBatches[i] = toInternalBatch(batch)
	}
	return message.MarshalBatchesToProto(internalBatches)
}

func UnmarshalMessageFromProto(b []byte) (*Message, error) {
	internalPart, err := message.UnmarshalFromProto(b)
	if err != nil {
		return nil, err
	}
	return &Message{part: internalPart}, nil
}

func MarshalMessageToProto(msg *Message) ([]byte, error) {
	return message.MarshalToProto(msg.part)
}
