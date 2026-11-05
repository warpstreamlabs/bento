package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
)

// PartToProto converts part to a protobuf Part.
func PartToProto(part *Part) (*pb.Part, error) {
	if part == nil || part.data == nil {
		return nil, ErrMessagePartNotExist
	}

	proto := &pb.Part{}
	if err := part.data.err; err != nil {
		proto.Error = &pb.Error{Message: err.Error()}
	}

	if structured := part.data.structured; structured != nil {
		structured, err := toProtoValue(structured)
		if err != nil {
			return nil, err
		}
		proto.Content = &pb.Part_Structured{Structured: structured}
	} else {
		proto.Content = &pb.Part_Raw{Raw: part.data.rawBytes}
	}

	if len(part.data.metadata) > 0 {
		proto.Metadata = make(map[string]string, len(part.data.metadata))
		for k, v := range part.data.metadata {
			proto.Metadata[k] = metaToString(v)
		}
	}

	return proto, nil
}

func toPart(proto *pb.Part) (*Part, error) {
	data := messageData{}
	switch c := proto.GetContent(); c.(type) {
	case *pb.Part_Raw:
		data.rawBytes = proto.GetRaw()
	case *pb.Part_Structured:
		data.structured = fromProtoValue(proto.GetStructured())
	}

	if len(proto.Metadata) > 0 {
		data.metadata = make(map[string]any, len(proto.Metadata))
		for k, v := range proto.Metadata {
			data.metadata[k] = v
		}
	}
	if protoErr := proto.GetError(); protoErr != nil {
		data.err = errors.New(protoErr.GetMessage())
	}

	return &Part{
		data: &data,
		ctx:  context.Background(),
	}, nil
}

// BatchToProto converts batch to a protobuf Batch.
func BatchToProto(batch Batch) (*pb.Batch, error) {
	parts := make([]*pb.Part, len(batch))
	for i, msg := range batch {
		part, err := PartToProto(msg)
		if err != nil {
			return nil, err
		}
		parts[i] = part
	}

	return &pb.Batch{Parts: parts}, nil
}

// BatchFromProto converts a protobuf Batch to a batch.
func BatchFromProto(proto *pb.Batch) (Batch, error) {
	batch := make([]*Part, len(proto.GetParts()))
	for i, msg := range proto.GetParts() {
		part, err := toPart(msg)
		if err != nil {
			return nil, err
		}
		batch[i] = part
	}

	return batch, nil
}

// BatchesFromProto converts protobuf Batches to batches.
func BatchesFromProto(proto *pb.Batches) ([]Batch, error) {
	batches := make([]Batch, len(proto.GetBatches()))
	for i, protoBatch := range proto.GetBatches() {
		batch, err := BatchFromProto(protoBatch)
		if err != nil {
			return nil, err
		}

		batches[i] = batch
	}

	return batches, nil
}

//------------------------------------------------------------------------------

func toProtoValue(v any) (*pb.Value, error) {
	switch v := v.(type) {
	case nil:
		return &pb.Value{Kind: &pb.Value_Null{}}, nil
	case bool:
		return &pb.Value{Kind: &pb.Value_Bool{Bool: v}}, nil
	case string:
		return &pb.Value{Kind: &pb.Value_String_{String_: v}}, nil
	case []byte:
		return &pb.Value{Kind: &pb.Value_Bytes{Bytes: v}}, nil
	case json.Number:
		return &pb.Value{Kind: &pb.Value_Decimal{Decimal: v.String()}}, nil
	case time.Time:
		return &pb.Value{Kind: &pb.Value_Timestamp{Timestamp: timestamppb.New(v)}}, nil
	case []any:
		items := make([]*pb.Value, len(v))
		for i, item := range v {
			var err error
			if items[i], err = toProtoValue(item); err != nil {
				return nil, err
			}
		}
		return &pb.Value{Kind: &pb.Value_List{List: &pb.List{Items: items}}}, nil
	case map[string]any:
		fields := make(map[string]*pb.Value, len(v))
		for k, field := range v {
			var err error
			if fields[k], err = toProtoValue(field); err != nil {
				return nil, err
			}
		}
		return &pb.Value{Kind: &pb.Value_Struct{Struct: &pb.Struct{Fields: fields}}}, nil
	}

	switch rv := reflect.ValueOf(v); {
	case rv.CanInt():
		return &pb.Value{Kind: &pb.Value_Int64{Int64: rv.Int()}}, nil
	case rv.CanUint():
		return &pb.Value{Kind: &pb.Value_Uint64{Uint64: rv.Uint()}}, nil
	case rv.CanFloat():
		return &pb.Value{Kind: &pb.Value_Double{Double: rv.Float()}}, nil
	}
	return nil, fmt.Errorf("unsupported structured value of type %T", v)
}

func fromProtoValue(v *pb.Value) any {
	switch kind := v.GetKind().(type) {
	case *pb.Value_Bool:
		return kind.Bool
	case *pb.Value_Double:
		return kind.Double
	case *pb.Value_String_:
		return kind.String_
	case *pb.Value_Int64:
		return kind.Int64
	case *pb.Value_Uint64:
		return kind.Uint64
	case *pb.Value_Bytes:
		return kind.Bytes
	case *pb.Value_Decimal:
		return json.Number(kind.Decimal)
	case *pb.Value_Timestamp:
		return kind.Timestamp.AsTime()
	case *pb.Value_List:
		items := make([]any, len(kind.List.GetItems()))
		for i, item := range kind.List.GetItems() {
			items[i] = fromProtoValue(item)
		}
		return items
	case *pb.Value_Struct:
		fields := make(map[string]any, len(kind.Struct.GetFields()))
		for k, field := range kind.Struct.GetFields() {
			fields[k] = fromProtoValue(field)
		}
		return fields
	}
	return nil
}
