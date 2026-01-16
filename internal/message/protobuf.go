package message

import (
	"context"
	"errors"

	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func UnmarshalFromProto(b []byte) (*Part, error) {
	protoPart := &pb.Part{}
	if err := proto.Unmarshal(b, protoPart); err != nil {
		return nil, err
	}

	return toPart(protoPart)
}

func MarshalToProto(part *Part) ([]byte, error) {
	protoPart, err := fromPart(part)
	if err != nil {
		return nil, err
	}

	opts := proto.MarshalOptions{
		Deterministic: true,
	}

	return opts.Marshal(protoPart)
}

func MarshalBatchToProto(batch Batch) ([]byte, error) {
	protoBatch, err := fromBatch(batch)
	if err != nil {
		return nil, err
	}
	opts := proto.MarshalOptions{
		Deterministic: true,
	}

	return opts.Marshal(protoBatch)
}

func UnmarshalBatchFromProto(b []byte) (Batch, error) {
	protoBatch := &pb.Batch{}
	if err := proto.Unmarshal(b, protoBatch); err != nil {
		return nil, err
	}

	return toBatch(protoBatch)
}

func UnmarshalBatchesFromProto(b []byte) ([]Batch, error) {
	protoBatch := &pb.Batches{}
	if err := proto.Unmarshal(b, protoBatch); err != nil {
		return nil, err
	}

	batches := make([]Batch, len(protoBatch.GetBatches()))
	for i, protoBatch := range protoBatch.GetBatches() {
		batch, err := toBatch(protoBatch)
		if err != nil {
			return nil, err
		}

		batches[i] = batch
	}

	return batches, nil
}

func MarshalBatchesToProto(batches []Batch) ([]byte, error) {
	protoBatches := &pb.Batches{
		Batches: make([]*pb.Batch, len(batches)),
	}

	for i, batch := range batches {
		protoBatch, err := fromBatch(batch)
		if err != nil {
			return nil, err
		}
		protoBatches.Batches[i] = protoBatch
	}

	opts := proto.MarshalOptions{
		Deterministic: true,
	}

	return opts.Marshal(protoBatches)
}

func fromPart(part *Part) (*pb.Part, error) {
	if part == nil || part.data == nil {
		return nil, ErrMessagePartNotExist
	}

	proto := &pb.Part{}
	if err := part.data.err; err != nil {
		proto.Error = part.data.err.Error()
	}

	if structured := part.data.structured; structured != nil {
		structured, err := structpb.NewValue(structured)
		if err != nil {
			return nil, err
		}
		proto.Content = &pb.Part_Structured{Structured: structured}
	} else {
		proto.Content = &pb.Part_Raw{Raw: part.data.rawBytes}
	}

	if part.data.metadata != nil {
		meta, err := structpb.NewStruct(part.data.metadata)
		if err != nil {
			return nil, err
		}
		proto.Metadata = meta
	}

	return proto, nil
}

func toPart(proto *pb.Part) (*Part, error) {
	data := messageData{}
	switch c := proto.GetContent(); c.(type) {
	case *pb.Part_Raw:
		data.rawBytes = proto.GetRaw()
	case *pb.Part_Structured:
		data.structured = proto.GetStructured().AsInterface()
	}

	if proto.Metadata != nil {
		data.metadata = proto.Metadata.AsMap()
	}
	// TODO(gregfurman): This is unideal but we do it
	// in lieu of error type enums/constants i.e
	// ERR_NOT_CONNECTED
	if errStr := proto.GetError(); errStr != "" {
		data.err = errors.New(errStr)
	}

	return &Part{
		data: &data,
		ctx:  context.Background(),
	}, nil
}

func fromBatch(batch Batch) (*pb.Batch, error) {
	parts := make([]*pb.Part, len(batch))
	for i, msg := range batch {
		part, err := fromPart(msg)
		if err != nil {
			return nil, err
		}
		parts[i] = part
	}

	return &pb.Batch{Parts: parts}, nil
}

func toBatch(proto *pb.Batch) (Batch, error) {
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
